from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import requests


STRATEGY_DIR = Path(__file__).resolve().parent
ROOT = STRATEGY_DIR.parents[1]
EVENT_FEATURES_PATH = STRATEGY_DIR / "event_features.parquet"
LIQUIDITY_PATH = STRATEGY_DIR / "liquidity_monthly.parquet"
FUNDING_PATH = STRATEGY_DIR / "ALL-Funding-20250101.parquet"
BAD_SYMBOLS = {"DODOX", "STORJ", "SIREN", "RIVER", "BARD", "PIPPIN", "ORCA"}
RATE_BUCKET_ORD = {"30-50": 0, "50-75": 1, "75-100": 2, "100-150": 3, "150+": 4}
LIQUIDITY_BUCKET_ORD = {"unknown": 0, "micro": 1, "small": 2, "mid": 3, "large": 4}


@dataclass(frozen=True)
class XgbModelSpec:
    name: str
    path: Path
    kind: str
    score: str


class MaxFundingXgbScorer:
    def __init__(
        self,
        specs: list[dict[str, Any]],
        primary: str = "",
        api_url: str = "",
        api_timeout: float = 0.8,
        proxies: dict[str, str] | None = None,
    ) -> None:
        self.specs = [self._parse_spec(row) for row in specs]
        names = [spec.name for spec in self.specs]
        if len(names) != len(set(names)):
            raise RuntimeError("xgb_models contains duplicate names")
        if primary and primary not in names:
            raise RuntimeError(f"xgb_primary not found in xgb_models: {primary}")
        self.primary = primary
        self.api_url = api_url.rstrip("/")
        self.api_timeout = float(api_timeout)
        self.proxies = proxies
        self.models = {spec.name: joblib.load(spec.path) for spec in self.specs}
        self.history = self._load_history()
        self.funding_history = self._load_funding_history()
        self.liquidity = self._load_liquidity()

    @property
    def metric_columns(self) -> list[str]:
        columns: list[str] = []
        for spec in self.specs:
            prefix = f"xgb_{spec.name}_"
            if spec.kind == "node_select":
                columns.extend(
                    [
                        f"{prefix}pred_net_bps",
                        f"{prefix}pred_pos_prob",
                        f"{prefix}score_bps",
                        f"{prefix}threshold_bps",
                        f"{prefix}pass",
                        f"{prefix}missing",
                    ],
                )
            elif spec.kind == "ensemble_node_select":
                columns.extend(
                    [
                        f"{prefix}score_bps",
                        f"{prefix}threshold_bps",
                        f"{prefix}pass",
                        f"{prefix}missing",
                    ],
                )
            elif spec.kind == "top1_select":
                columns.extend([f"{prefix}top1_prob", f"{prefix}missing"])
        return columns

    def score(
        self,
        candidates: list[dict[str, Any]],
        observed: list[dict[str, Any]],
        funding_ns: int,
    ) -> dict[str, dict[str, Any]]:
        frame = self.prepare_frame(candidates, observed, funding_ns)
        return self.score_frame(frame)

    def prepare_frame(
        self,
        candidates: list[dict[str, Any]],
        observed: list[dict[str, Any]],
        funding_ns: int,
    ) -> pd.DataFrame:
        if not candidates:
            funding_utc = pd.Timestamp(funding_ns, unit="ns", tz="UTC")
            for base in ("BTC", "ETH", "SOL"):
                self._fetch_market_values(base, funding_utc)
            return pd.DataFrame()
        frame = self._base_frame(candidates, observed, funding_ns)
        return self._add_market_features(frame, funding_ns)

    def score_frame(self, frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
        if frame.empty:
            return {}
        out = {symbol: {} for symbol in frame["symbol"].astype(str)}
        primary_scores: dict[str, float] = {}
        primary_pass: dict[str, bool] = {}

        for spec in self.specs:
            model = self.models[spec.name]
            numeric = list(model.get("numeric", []))
            categorical = list(model.get("categorical", []))
            x = self._model_frame(frame, numeric, categorical, int(model.get("horizon_ms", 0)))
            missing = self._missing_count(x, numeric, categorical)
            prefix = f"xgb_{spec.name}_"

            if spec.kind == "node_select":
                pred_net, pred_pos, score, missing = self._score_node_model(model, frame, spec.score)
                threshold = float(model.get("threshold", np.nan))
                passed = score >= threshold
                for idx, symbol in enumerate(frame["symbol"].astype(str)):
                    item = out[symbol]
                    item[f"{prefix}pred_net_bps"] = float(pred_net[idx])
                    item[f"{prefix}pred_pos_prob"] = float(pred_pos[idx])
                    item[f"{prefix}score_bps"] = float(score[idx])
                    item[f"{prefix}threshold_bps"] = threshold
                    item[f"{prefix}pass"] = bool(passed[idx])
                    item[f"{prefix}missing"] = int(missing[idx])
                    if spec.name == self.primary:
                        primary_scores[symbol] = float(score[idx])
                        primary_pass[symbol] = bool(passed[idx])
            elif spec.kind == "ensemble_node_select":
                scores = []
                missings = []
                for sub in model["models"]:
                    _, _, sub_score, sub_missing = self._score_node_model(sub, frame, str(sub.get("score", "score_mul")))
                    scores.append(sub_score)
                    missings.append(sub_missing)
                stacked = np.vstack(scores)
                if str(model.get("score", "")).startswith("ens_avg"):
                    score = np.nanmean(stacked, axis=0)
                else:
                    score = np.nanmin(stacked, axis=0)
                missing = np.vstack(missings).sum(axis=0)
                threshold = float(model.get("threshold", np.nan))
                passed = score >= threshold
                for idx, symbol in enumerate(frame["symbol"].astype(str)):
                    item = out[symbol]
                    item[f"{prefix}score_bps"] = float(score[idx])
                    item[f"{prefix}threshold_bps"] = threshold
                    item[f"{prefix}pass"] = bool(passed[idx])
                    item[f"{prefix}missing"] = int(missing[idx])
                    if spec.name == self.primary:
                        primary_scores[symbol] = float(score[idx])
                        primary_pass[symbol] = bool(passed[idx])
            elif spec.kind == "top1_select":
                prob = model["model"].predict_proba(x)[:, 1]
                for idx, symbol in enumerate(frame["symbol"].astype(str)):
                    item = out[symbol]
                    item[f"{prefix}top1_prob"] = float(prob[idx])
                    item[f"{prefix}missing"] = int(missing[idx])
                    if spec.name == self.primary:
                        primary_scores[symbol] = float(prob[idx])
                        primary_pass[symbol] = True
            else:
                raise RuntimeError(f"unsupported xgb model kind: {spec.kind}")

        for symbol, item in out.items():
            if self.primary:
                item["xgb_primary_model"] = self.primary
                item["xgb_primary_score"] = primary_scores.get(symbol, np.nan)
                item["xgb_primary_pass"] = primary_pass.get(symbol, False)
        return out

    def _parse_spec(self, row: dict[str, Any]) -> XgbModelSpec:
        name = str(row["name"])
        path = Path(str(row["path"]))
        if not path.is_absolute():
            path = ROOT / path
        if not path.exists():
            raise RuntimeError(f"xgb model not found: {path}")
        kind = str(row["kind"])
        if kind not in {"node_select", "ensemble_node_select", "top1_select"}:
            raise RuntimeError(f"unsupported xgb model kind: {kind}")
        score = str(row.get("score") or ("top1_prob" if kind == "top1_select" else "pred_net_bps"))
        return XgbModelSpec(name=name, path=path, kind=kind, score=score)

    def _load_history(self) -> pd.DataFrame:
        if not EVENT_FEATURES_PATH.exists():
            return pd.DataFrame()
        df = pd.read_parquet(EVENT_FEATURES_PATH)
        df["funding_utc"] = pd.to_datetime(df["funding_utc"], utc=True)
        return df.sort_values("funding_utc").groupby("symbol", as_index=False).tail(1).set_index("symbol")

    def _load_liquidity(self) -> pd.DataFrame:
        if not LIQUIDITY_PATH.exists():
            return pd.DataFrame()
        df = pd.read_parquet(LIQUIDITY_PATH)
        df = df.sort_values(["base", "liq_month"])
        return df.groupby("base", as_index=False).tail(1).set_index("base")

    def _load_funding_history(self) -> pd.DataFrame:
        if not FUNDING_PATH.exists():
            return pd.DataFrame()
        df = pd.read_parquet(FUNDING_PATH).sort_values(["symbol", "funding_utc"])
        rows = []
        for symbol, group in df.groupby("symbol", sort=False):
            abs_rate = group["abs_rate_bps"].astype(float)
            rate = group["rate_bps"].astype(float)
            rows.append(
                {
                    "symbol": symbol,
                    "funding_hist_count": int(len(group)),
                    "funding_prev_rate_bps": float(rate.iloc[-1]) if len(rate) else np.nan,
                    "funding_prev_abs_bps": float(abs_rate.iloc[-1]) if len(abs_rate) else np.nan,
                    "funding_hist_abs_mean": float(abs_rate.mean()) if len(abs_rate) else np.nan,
                    "funding_hist_abs_p90": float(abs_rate.quantile(0.90)) if len(abs_rate) else np.nan,
                    "funding_roll3_abs_mean": float(abs_rate.tail(3).mean()) if len(abs_rate) else np.nan,
                    "funding_roll10_abs_mean": float(abs_rate.tail(10).mean()) if len(abs_rate) else np.nan,
                    "funding_roll10_abs_max": float(abs_rate.tail(10).max()) if len(abs_rate) else np.nan,
                },
            )
        return pd.DataFrame(rows).set_index("symbol") if rows else pd.DataFrame()

    def _base_frame(
        self,
        candidates: list[dict[str, Any]],
        observed: list[dict[str, Any]],
        funding_ns: int,
    ) -> pd.DataFrame:
        funding_utc = pd.Timestamp(funding_ns, unit="ns", tz="UTC")
        node_type = self._node_type(funding_utc)
        obs = [self._funding_row(row, funding_utc, node_type) for row in observed]
        rows = [self._funding_row(row, funding_utc, node_type) for row in candidates]
        obs_abs = pd.Series([row["abs_rate_bps"] for row in obs], dtype=float)
        cand_abs = pd.Series([row["abs_rate_bps"] for row in rows], dtype=float)
        rank = (
            pd.DataFrame(obs)
            .sort_values("abs_rate_bps", ascending=False)
            .assign(rank_abs=lambda x: np.arange(1, len(x) + 1))
            .set_index("symbol")["rank_abs"]
            if obs
            else pd.Series(dtype=float)
        )
        node_30 = int((obs_abs >= 30.0).sum())
        node_50 = int((obs_abs >= 50.0).sum())
        node_top = float(obs_abs.max()) if len(obs_abs) else np.nan
        node_mean = float(obs_abs.mean()) if len(obs_abs) else np.nan
        node_1h = int(sum(row["interval_h"] == 1 for row in obs))
        candidate_n = len(rows)
        candidate_mean = float(cand_abs.mean()) if len(cand_abs) else np.nan
        candidate_max = float(cand_abs.max()) if len(cand_abs) else np.nan
        candidate_std = float(cand_abs.std()) if len(cand_abs) > 1 else 0.0

        for row in rows:
            row["rank_abs"] = float(rank.get(row["symbol"], np.nan))
            row["node_1h_count"] = node_1h
            row["node_30_count"] = node_30
            row["node_50_count"] = node_50
            row["node_top_abs"] = node_top
            row["node_mean_abs"] = node_mean
            row["candidate_n"] = candidate_n
            row["candidate_abs_mean"] = candidate_mean
            row["candidate_abs_max"] = candidate_max
            row["candidate_abs_std"] = candidate_std
        frame = pd.DataFrame(rows)
        return self._add_node_ranks(frame)

    def _add_market_features(self, frame: pd.DataFrame, funding_ns: int) -> pd.DataFrame:
        out = frame.copy()
        funding_utc = pd.Timestamp(funding_ns, unit="ns", tz="UTC")
        for base in ("BTC", "ETH", "SOL"):
            values = self._fetch_market_values(base, funding_utc)
            for key, value in values.items():
                out[key] = value
        return out

    def _fetch_market_values(self, base: str, funding_utc: pd.Timestamp) -> dict[str, float]:
        if not self.api_url:
            raise RuntimeError("api_url is required for market features")
        target = funding_utc - pd.Timedelta(minutes=1)
        end_ms = int(target.timestamp() * 1000) + 59_999
        path = "/fapi/v1/klines" if "fapi" in self.api_url else "/api/v3/klines"
        resp = requests.get(
            f"{self.api_url}{path}",
            params={"symbol": f"{base}USDT", "interval": "1m", "limit": 90, "endTime": end_ms},
            timeout=self.api_timeout,
            proxies=self.proxies,
        )
        if resp.status_code == 404:
            resp = requests.get(
                f"{self.api_url}/fapi/v1/klines",
                params={"symbol": f"{base}USDT", "interval": "1m", "limit": 90, "endTime": end_ms},
                timeout=self.api_timeout,
                proxies=self.proxies,
            )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            raise RuntimeError(f"empty market kline response: {base}USDT")
        cols = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "trade_count", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]
        df = pd.DataFrame(rows, columns=cols[: len(rows[0])])
        return self._market_values(base, df)

    def _market_values(self, base: str, df: pd.DataFrame) -> dict[str, float]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        quote = pd.to_numeric(df["quote_volume"], errors="coerce").astype(float)
        buy_quote = pd.to_numeric(df["taker_buy_quote_volume"], errors="coerce").astype(float)
        ret1 = np.log(close).diff() * 10000.0
        values: dict[str, float] = {}
        prefix = f"mkt_{base.lower()}"
        for win in (5, 15, 60):
            values[f"{prefix}_ret_{win}m"] = float(np.log(close.iloc[-1] / close.iloc[-win - 1]) * 10000.0) if len(close) > win else np.nan
            values[f"{prefix}_vol_{win}m"] = float(ret1.tail(win).std()) if len(ret1.dropna()) >= 3 else np.nan
            total_quote = float(quote.tail(win).sum())
            total_buy = float(buy_quote.tail(win).sum())
            values[f"{prefix}_quote_{win}m"] = float(np.log1p(total_quote)) if np.isfinite(total_quote) else np.nan
            values[f"{prefix}_imb_{win}m"] = float(2.0 * total_buy / total_quote - 1.0) if total_quote else np.nan
        return values

    def _funding_row(self, row: dict[str, Any], funding_utc: pd.Timestamp, node_type: str) -> dict[str, Any]:
        symbol = str(row["symbol"])
        base = self._base(symbol)
        rate_bps = float(row["rate"]) * 10000.0
        hist = self.history.loc[symbol] if symbol in self.history.index else None
        funding_hist = self.funding_history.loc[symbol] if symbol in self.funding_history.index else None
        liq = self.liquidity.loc[base] if base in self.liquidity.index else None
        bucket = self._rate_bucket(abs(rate_bps))
        liquidity_bucket = str(liq["liquidity_bucket"]) if liq is not None else "unknown"
        liquidity_usdt = float(liq["liquidity_usdt"]) if liq is not None and pd.notna(liq["liquidity_usdt"]) else np.nan
        data: dict[str, Any] = {
            "symbol": symbol,
            "base": base,
            "funding_utc": funding_utc,
            "funding_time": int(funding_utc.timestamp() * 1000),
            "rate_bps": rate_bps,
            "abs_rate_bps": abs(rate_bps),
            "side": "SELL" if rate_bps > 0 else "BUY",
            "interval_h": self._hist_value(hist, "interval_h", self._interval_from_node(funding_utc)),
            "rate_bucket": bucket,
            "rate_bucket_ord": RATE_BUCKET_ORD[bucket],
            "node_type": node_type,
            "is_bad_symbol": int(base in BAD_SYMBOLS),
            "is_negative_funding": int(rate_bps < 0),
            "liquidity_bucket": liquidity_bucket,
            "liquidity_bucket_ord": LIQUIDITY_BUCKET_ORD.get(liquidity_bucket, 0),
            "log_liquidity_usdt": np.log1p(liquidity_usdt) if np.isfinite(liquidity_usdt) else np.nan,
            "symbol_hist_count": self._hist_value(hist, "symbol_hist_count", np.nan),
            "funding_hist_count": self._hist_value(funding_hist, "funding_hist_count", 0),
            "funding_prev_rate_bps": self._hist_value(funding_hist, "funding_prev_rate_bps", np.nan),
            "funding_prev_abs_bps": self._hist_value(funding_hist, "funding_prev_abs_bps", np.nan),
            "funding_hist_abs_mean": self._hist_value(funding_hist, "funding_hist_abs_mean", np.nan),
            "funding_hist_abs_p90": self._hist_value(funding_hist, "funding_hist_abs_p90", np.nan),
            "funding_roll3_abs_mean": self._hist_value(funding_hist, "funding_roll3_abs_mean", np.nan),
            "funding_roll10_abs_mean": self._hist_value(funding_hist, "funding_roll10_abs_mean", np.nan),
            "funding_roll10_abs_max": self._hist_value(funding_hist, "funding_roll10_abs_max", np.nan),
            "usdt_pre": np.nan,
            "log_usdt_pre": np.nan,
            "log_tick_pre": np.nan,
            "pre_cost_bps": self._row_float(row, "pre_cost_bps", np.nan),
            "pre_vol_bps": np.nan,
            "entry_err_ms": np.nan,
        }
        if hist is not None:
            for col in hist.index:
                if str(col).startswith(("symbol_hist_mean_cost_", "symbol_hist_p75_cost_")):
                    data[str(col)] = hist[col]
        return data

    def _add_node_ranks(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = frame.copy()
        denom = max(len(out) - 1, 1)
        out["node_abs_rank"] = out["abs_rate_bps"].rank(method="first", ascending=False)
        out["node_abs_pct"] = (len(out) - out["node_abs_rank"]) / denom
        out["abs_gap_to_max"] = out["candidate_abs_max"] - out["abs_rate_bps"]
        out["abs_ratio_to_max"] = out["abs_rate_bps"] / out["candidate_abs_max"].replace(0, np.nan)
        out["node_usdt_rank"] = out["usdt_pre"].rank(method="first", ascending=False)
        out["node_usdt_pct"] = (len(out) - out["node_usdt_rank"]) / denom
        return out

    def _model_frame(
        self,
        frame: pd.DataFrame,
        numeric: list[str],
        categorical: list[str],
        horizon_ms: int,
    ) -> pd.DataFrame:
        out = frame.copy()
        p75_col = f"symbol_hist_p75_cost_{horizon_ms}ms"
        if p75_col in out.columns:
            denom = max(len(out) - 1, 1)
            out["node_hist_p75_rank"] = out[p75_col].rank(method="first", ascending=True)
            out["node_hist_p75_pct"] = (len(out) - out["node_hist_p75_rank"]) / denom
        for col in numeric:
            if col not in out.columns:
                out[col] = np.nan
        for col in categorical:
            if col not in out.columns:
                out[col] = "missing"
        data = out[numeric + categorical].copy()
        data[numeric] = data[numeric].replace([np.inf, -np.inf], np.nan)
        for col in categorical:
            data[col] = data[col].astype(str).fillna("missing")
        return data

    def _score_node_model(
        self,
        model: dict[str, Any],
        frame: pd.DataFrame,
        score_name: str,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        numeric = list(model.get("numeric", []))
        categorical = list(model.get("categorical", []))
        x = self._model_frame(frame, numeric, categorical, int(model.get("horizon_ms", 0)))
        missing = self._missing_count(x, numeric, categorical)
        pred_net = model["reg"].predict(x)
        pred_pos = model["clf"].predict_proba(x)[:, 1]
        score = self._node_score(score_name, pred_net, pred_pos)
        return pred_net, pred_pos, score, missing

    def _missing_count(self, frame: pd.DataFrame, numeric: list[str], categorical: list[str]) -> np.ndarray:
        numeric_missing = frame[numeric].isna().sum(axis=1).to_numpy() if numeric else 0
        if categorical:
            cat = frame[categorical].replace({"nan": np.nan, "None": np.nan, "missing": np.nan})
            cat_missing = cat.isna().sum(axis=1).to_numpy()
        else:
            cat_missing = 0
        return numeric_missing + cat_missing

    def _node_score(self, name: str, pred_net: np.ndarray, pred_pos: np.ndarray) -> np.ndarray:
        if name in {"pred_net", "pred_net_bps", "score_bps"}:
            return pred_net
        if name == "pred_pos_prob":
            return pred_pos
        if name == "score_adj10":
            return pred_net + 10.0 * (pred_pos - 0.5)
        if name == "score_adj20":
            return pred_net + 20.0 * (pred_pos - 0.5)
        if name == "score_mul":
            return pred_net * (0.5 + pred_pos)
        if name == "adjusted_500ms":
            return pred_net + 10.0 * (pred_pos - 0.5)
        if name == "adjusted_3000ms":
            return pred_net * (0.5 + pred_pos)
        raise RuntimeError(f"unsupported node_select score: {name}")

    def _hist_value(self, hist: pd.Series | None, column: str, default: Any) -> Any:
        if hist is None or column not in hist.index or pd.isna(hist[column]):
            return default
        return hist[column]

    def _row_float(self, row: dict[str, Any], column: str, default: float) -> float:
        value = row.get(column)
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _base(self, symbol: str) -> str:
        return symbol.removesuffix("USDT")

    def _rate_bucket(self, value: float) -> str:
        if value < 50:
            return "30-50"
        if value < 75:
            return "50-75"
        if value < 100:
            return "75-100"
        if value < 150:
            return "100-150"
        return "150+"

    def _node_type(self, ts: pd.Timestamp) -> str:
        hour = int(ts.hour)
        if hour % 8 == 0:
            return "8h"
        if hour % 4 == 0:
            return "4h"
        return "1h"

    def _interval_from_node(self, ts: pd.Timestamp) -> int:
        hour = int(ts.hour)
        if hour % 8 == 0:
            return 8
        if hour % 4 == 0:
            return 4
        return 1
