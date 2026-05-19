from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import load
from nautilus_trader.core.nautilus_pyo3 import InstrumentId
from nautilus_trader.core.nautilus_pyo3 import LogColor
from nautilus_trader.core.nautilus_pyo3 import OrderSide

from strategies.maxfunding import Maxfunding
from strategies.maxfunding import MaxfundingConfig


ROOT = Path(__file__).resolve().parents[1]
RATE_BUCKET_ORDER = {"30-50": 1, "50-75": 2, "75-100": 3, "100-150": 4, "150+": 5}


class XgbfundingConfig(MaxfundingConfig):
    model_horizon_ms: int = 500
    model_path: str = "models/node_select_outputs/node_select_500ms.joblib"
    feature_path: str = "models/funding_return_outputs/event_features.parquet"

    def __new__(
        cls,
        model_horizon_ms: int = 500,
        model_path: str = "models/node_select_outputs/node_select_500ms.joblib",
        feature_path: str = "models/funding_return_outputs/event_features.parquet",
        **kwargs,
    ):
        config = super().__new__(cls, **kwargs)
        config.model_horizon_ms = model_horizon_ms
        config.model_path = model_path
        config.feature_path = feature_path
        return config


class Xgbfunding(Maxfunding):
    def __init__(self, config: XgbfundingConfig) -> None:
        super().__init__(config)
        self.model = None
        self.hist: dict[str, dict[str, object]] = {}
        self.numeric: list[str] = []
        self.categorical: list[str] = []
        self.threshold = 0.0

    # 启动前加载 XGB 模型和历史画像，其他定时逻辑沿用 maxfunding。
    def on_start(self) -> None:
        self._load_model()
        self._load_hist()
        super().on_start()
        self.log.info(
            f"XGB资金费率策略启动，模型{self.config.model_horizon_ms}ms，"
            f"阈值{self.threshold:.2f}bps，历史画像{len(self.hist)}个",
            LogColor.NORMAL,
        )

    # t-1 用 XGB 在候选里排序，并要求预测收益超过模型阈值才下单。
    def _freeze_funding(self) -> None:
        if self.sent_done:
            return
        if not self.entry_done:
            self.sent_done = True
            self.log.info("跳过本轮，候选未准备好", LogColor.NORMAL)
            return

        rows = [
            (ins_id, row)
            for ins_id, row in self.ins_map.items()
            if ins_id in self.ins and "rate" in row and "pre" in row
        ]
        if not rows:
            self.log.info(f"XGB交易模式，候选{len(self.ins_map)}个，无可下单交易对", LogColor.NORMAL)
            self.sent_done = True
            return

        scored = self._score(rows)
        if scored.empty:
            self.log.info(f"XGB交易模式，候选{len(rows)}个，模型无有效评分", LogColor.NORMAL)
            self.sent_done = True
            return

        best = scored.sort_values(["pred_net", "pred_pos_prob"], ascending=[False, False]).iloc[0]
        ins_id = best["instrument_id"]
        row = self.ins_map[ins_id]
        row["pred_net"] = Decimal(str(round(float(best["pred_net"]), 6)))
        row["pred_pos_prob"] = Decimal(str(round(float(best["pred_pos_prob"]), 6)))
        row["model_threshold"] = Decimal(str(round(float(self.threshold), 6)))

        if float(best["pred_net"]) < self.threshold:
            self.log.info(
                f"XGB交易模式，候选{len(rows)}个，最高{self._base(ins_id)} "
                f"预测{float(best['pred_net']):.2f}bps，低于阈值{self.threshold:.2f}bps，跳过",
                LogColor.NORMAL,
            )
            self.sent_done = True
            return

        ins = self.ins[ins_id]
        side = self._side(row["rate"])
        row["side"] = side
        qty = self._qty(ins, row["pre"])
        order = self._market_order(ins_id, side, qty)
        self.order_map[order.client_order_id] = ins_id
        self.chosen_id = ins_id
        self.submit_order(order)
        self.had_order = True
        self.sent_done = True
        self.log.info(
            f"XGB交易模式，候选{len(rows)}个，选择{self._base(ins_id)}，"
            f"费率{self._bps(row['rate'])}bps，预测{float(best['pred_net']):.2f}bps，"
            f"胜率{float(best['pred_pos_prob']) * 100:.2f}%，名义{self.notional:.2f}USDT",
            LogColor.NORMAL,
        )

    def _load_model(self) -> None:
        path = self._path(self.config.model_path)
        self.model = load(path)
        self.numeric = list(self.model["numeric"])
        self.categorical = list(self.model["categorical"])
        self.threshold = float(self.model["threshold"])
        horizon = int(self.model["horizon_ms"])
        if horizon != int(self.config.model_horizon_ms):
            raise RuntimeError(f"model horizon mismatch: config={self.config.model_horizon_ms} file={horizon}")

    def _load_hist(self) -> None:
        path = self._path(self.config.feature_path)
        df = pd.read_parquet(path)
        df["funding_utc"] = pd.to_datetime(df["funding_utc"], utc=True)
        ms = int(self.config.model_horizon_ms)
        self.hist.clear()
        for symbol, rows in df.sort_values("funding_utc").groupby("symbol", sort=False):
            last = rows.iloc[-1]
            cost = rows[f"price_cost_{ms}ms"].astype(float)
            self.hist[str(symbol)] = {
                "interval_h": self._num(last.get("interval_h")),
                "symbol_hist_count": int(len(rows)),
                f"symbol_hist_mean_cost_{ms}ms": float(cost.mean()),
                f"symbol_hist_p75_cost_{ms}ms": float(cost.quantile(0.75)),
                "log_liquidity_usdt": self._num(last.get("log_liquidity_usdt")),
                "liquidity_bucket_ord": self._num(last.get("liquidity_bucket_ord")),
                "liquidity_bucket": str(last.get("liquidity_bucket", "unknown")),
            }

    def _score(self, rows: list[tuple[InstrumentId, dict]]) -> pd.DataFrame:
        frame = self._features(rows)
        x = frame[self.numeric + self.categorical].copy()
        x[self.numeric] = x[self.numeric].replace([np.inf, -np.inf], np.nan)
        for col in self.categorical:
            x[col] = x[col].astype(str).fillna("missing")
        frame["pred_net"] = self.model["reg"].predict(x)
        frame["pred_pos_prob"] = self.model["clf"].predict_proba(x)[:, 1]
        return frame

    def _features(self, rows: list[tuple[InstrumentId, dict]]) -> pd.DataFrame:
        obs_rates = [
            float(abs(row["rate"]) * Decimal("10000"))
            for row in self.obs_map.values()
            if "rate" in row
        ]
        node_30 = sum(1 for value in obs_rates if value >= 30.0)
        node_50 = sum(1 for value in obs_rates if value >= 50.0)
        node_top = max(obs_rates) if obs_rates else np.nan
        node_mean = float(np.mean(obs_rates)) if obs_rates else np.nan
        one_h = sum(1 for ins_id, row in self.obs_map.items() if "rate" in row and self._hist(ins_id).get("interval_h") == 1)

        rank = {
            ins_id: i + 1
            for i, (ins_id, _) in enumerate(
                sorted(
                    [(ins_id, row) for ins_id, row in self.obs_map.items() if "rate" in row],
                    key=lambda item: abs(item[1]["rate"]),
                    reverse=True,
                )
            )
        }
        cand_abs = [float(abs(row["rate"]) * Decimal("10000")) for _, row in rows]
        candidate_n = len(rows)
        candidate_abs_mean = float(np.mean(cand_abs)) if cand_abs else np.nan
        candidate_abs_max = max(cand_abs) if cand_abs else np.nan

        out = []
        for ins_id, row in rows:
            rate_bps = float(row["rate"] * Decimal("10000"))
            abs_bps = abs(rate_bps)
            hist = self._hist(ins_id)
            item = {col: np.nan for col in self.numeric}
            item.update({col: "missing" for col in self.categorical})
            item.update(hist)
            item.update(
                {
                    "instrument_id": ins_id,
                    "rate_bps": rate_bps,
                    "abs_rate_bps": abs_bps,
                    "rank_abs": rank.get(ins_id, 999),
                    "node_1h_count": one_h,
                    "node_30_count": node_30,
                    "node_50_count": node_50,
                    "node_top_abs": node_top,
                    "node_mean_abs": node_mean,
                    "candidate_n": candidate_n,
                    "candidate_abs_mean": candidate_abs_mean,
                    "candidate_abs_max": candidate_abs_max,
                    "rate_bucket_ord": self._bucket_ord(abs_bps),
                    "is_negative_funding": int(rate_bps < 0),
                    "node_type": self._node_type(),
                    "side": "SELL" if rate_bps > 0 else "BUY",
                    "spot_status": "not_fetched",
                    "has_spot_t1": 0,
                }
            )
            out.append(item)
        return pd.DataFrame(out)

    def _hist(self, ins_id: InstrumentId) -> dict[str, object]:
        return self.hist.get(self.symbols.get(ins_id, ""), {})

    def _node_type(self) -> str:
        hour = pd.Timestamp(self._iso(self.fund_ns)).hour
        if hour % 8 == 0:
            return "8h"
        if hour % 4 == 0:
            return "4h"
        return "1h"

    def _bucket_ord(self, value: float) -> int:
        if value < 50:
            return RATE_BUCKET_ORDER["30-50"]
        if value < 75:
            return RATE_BUCKET_ORDER["50-75"]
        if value < 100:
            return RATE_BUCKET_ORDER["75-100"]
        if value < 150:
            return RATE_BUCKET_ORDER["100-150"]
        return RATE_BUCKET_ORDER["150+"]

    def _path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else ROOT / path

    def _num(self, value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return np.nan

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "funding_time",
                    "instrument",
                    "entry_rate",
                    "settle_rate",
                    "side",
                    "pre_px",
                    "notional",
                    "entry_funding_gain",
                    "settle_funding_gain",
                    "close_count",
                    "pred_net",
                    "pred_pos_prob",
                    "model_threshold",
                ],
            )

    # 写本轮选中交易的资金费率和模型判断记录。
    def _write_trade(self, close_cnt: int) -> None:
        ins_id = self.chosen_id
        if ins_id is None:
            return
        row = self.ins_map.get(ins_id)
        if row is None:
            return
        rate = row.get("rate")
        settle_rate = row.get("settle_rate", "")
        pre = row.get("pre", "")
        side = row.get("side") or (self._side(rate) if isinstance(rate, Decimal) else "")
        fund_gain = abs(rate) * self.notional if isinstance(rate, Decimal) else ""
        settle_gain = abs(settle_rate) * self.notional if isinstance(settle_rate, Decimal) else ""
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    self._iso(self.fund_ns),
                    ins_id,
                    rate,
                    settle_rate,
                    ("BUY" if side == OrderSide.BUY else "SELL") if isinstance(side, OrderSide) else "",
                    pre,
                    self.notional,
                    fund_gain,
                    settle_gain,
                    close_cnt,
                    row.get("pred_net", ""),
                    row.get("pred_pos_prob", ""),
                    row.get("model_threshold", ""),
                ],
            )
