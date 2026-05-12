from __future__ import annotations

import csv
from time import perf_counter
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import requests
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class FundingCollectorConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_notional: Decimal = Decimal("5.5")
    mode: str = "watch"
    min_rate: Decimal = Decimal("0.0050")
    trade_scope: str = "all"
    entry_sec: float = 2.0
    entry_before: float = 1.0
    exit_sec: float = 2.0
    stop_close: bool = True
    event_log_path: str = "auto"
    api_url: str = "https://fapi.binance.com"
    api_timeout: float = 0.8
    proxy_url: str = ""


class FundingCollector(Strategy):
    def __init__(self, config: FundingCollectorConfig) -> None:
        super().__init__(config)
        self.mode = config.mode.lower()
        self.notional = Decimal(str(config.trade_notional))
        self.min_rate = Decimal(str(config.min_rate))
        self.fund_ns = 0
        self.entry_done = False
        self.exit_done = False
        self.sent_done = False
        self.close_done = False
        self.log_done = False
        self.ins: dict[InstrumentId, Instrument] = {}
        self.trade_ids: set[InstrumentId] | None = None
        self.ins_map: dict[InstrumentId, dict[str, Decimal]] = {}
        self.open_ids: set[InstrumentId] = set()
        self.log_path = Path(config.event_log_path)
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if config.proxy_url else None

    # 启动时只订阅 heartbeat tick，资金费和价格用 REST 全量快照。
    def on_start(self) -> None:
        if self.mode not in {"watch", "trade"}:
            raise RuntimeError(f"Invalid mode: {self.mode}")
        if self.config.entry_sec <= self.config.entry_before:
            raise RuntimeError("entry_sec must be greater than entry_before")
        if self.config.entry_before <= 0:
            raise RuntimeError("entry_before must be positive")
        if self.config.exit_sec <= 0:
            raise RuntimeError("exit_sec must be positive")
        if self.config.api_timeout <= 0:
            raise RuntimeError("api_timeout must be positive")

        self._load_ins()
        self._load_scope()
        if self.mode == "watch":
            self._init_log()

        self._next_time()
        self.subscribe_trade_ticks(self.config.instrument_id)

        self.log.info(
            "FundingCollector started: "
            f"mode={self.mode}, heartbeat={self.config.instrument_id}, "
            f"instruments={len(self.ins)}, notional={self.notional}, "
            f"min_rate={self.min_rate}, trade_scope={self.config.trade_scope}, "
            f"entry_sec={self.config.entry_sec}s, exit_sec={self.config.exit_sec}s"
        )

    # heartbeat tick 负责 t-2/t+2 快照、最后一秒下单和平仓/记账。
    def on_trade_tick(self, tick: TradeTick) -> None:
        now_ns = self.clock.timestamp_ns()
        entry_ns = self.fund_ns - int(self.config.entry_sec * 1_000_000_000)
        order_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        exit_ns = self.fund_ns + int(self.config.exit_sec * 1_000_000_000)

        if now_ns >= exit_ns:
            if not self.exit_done:
                self._load_snap("post")
                self.exit_done = True
            if self.mode == "trade" and not self.close_done:
                close_cnt = 0
                for ins_id in list(self.open_ids):
                    self.close_all_positions(ins_id)
                    close_cnt += 1
                self.close_done = True
                self.log.info(
                    "FundingCollector close summary: "
                    f"funding={self._iso(self.fund_ns)}, close_count={close_cnt}, "
                    f"candidates={len(self.ins_map)}"
                )
                self._reset_state()
            elif self.mode == "watch" and not self.log_done:
                row_cnt, fund_sum, pnl_sum, net_sum = self._write_watch()
                self.log.info(
                    "FundingCollector watch summary: "
                    f"funding={self._iso(self.fund_ns)}, rows={row_cnt}, "
                    f"candidates={len(self.ins_map)}, funding_gain={fund_sum}, "
                    f"px_pnl={pnl_sum}, net_est={net_sum}"
                )
                self._reset_state()
            return

        if now_ns >= self.fund_ns:
            if not self.sent_done:
                self.sent_done = True
                self.log.info(
                    "FundingCollector entry skipped: "
                    f"funding={self._iso(self.fund_ns)}, reason=entry_window_missed"
                )
            return

        if entry_ns <= now_ns < order_ns and not self.entry_done and not self.sent_done:
            snap_ok = self._load_snap("pre")
            if not snap_ok:
                return
            if self.clock.timestamp_ns() < order_ns:
                self.entry_done = True
            else:
                self.sent_done = True
                self.log.info(
                    "FundingCollector entry skipped: "
                    f"funding={self._iso(self.fund_ns)}, reason=snapshot_late"
                )
            return

        if self.sent_done:
            return
        if now_ns < order_ns:
            return
        if not self.entry_done:
            self.sent_done = True
            self.log.info(
                "FundingCollector entry skipped: "
                f"funding={self._iso(self.fund_ns)}, reason=snapshot_not_ready"
            )
            return

        if self.mode == "trade":
            px_cnt = 0
            sub_cnt = 0
            for ins_id, row in list(self.ins_map.items()):
                if self.trade_ids is not None and ins_id not in self.trade_ids:
                    continue
                if "rate" not in row or "pre" not in row:
                    continue
                px_cnt += 1
                ins = self.ins.get(ins_id)
                if ins is None:
                    continue
                side = self._side(row["rate"])
                qty = self._qty(ins, row["pre"])
                order = self.order_factory.market(
                    instrument_id=ins_id,
                    order_side=side,
                    quantity=qty,
                    time_in_force=TimeInForce.GTC,
                )
                self.submit_order(order)
                self.open_ids.add(ins_id)
                sub_cnt += 1
            self.log.info(
                "FundingCollector entry summary: "
                f"funding={self._iso(self.fund_ns)}, candidates={len(self.ins_map)}, "
                f"priced={px_cnt}, submitted={sub_cnt}"
            )
        else:
            px_cnt = sum(1 for row in self.ins_map.values() if "pre" in row)
            self.log.info(
                "FundingCollector watch frozen: "
                f"funding={self._iso(self.fund_ns)}, candidates={len(self.ins_map)}, "
                f"priced={px_cnt}"
            )

        self.sent_done = True

    def on_stop(self) -> None:
        self.unsubscribe_trade_ticks(self.config.instrument_id)
        for ins_id in self.ins:
            self.cancel_all_orders(ins_id)

        if self.config.stop_close and self.mode == "trade":
            for ins_id in self.open_ids:
                self.close_all_positions(ins_id)

        self.log.info("FundingCollector stopped.")

    def on_reset(self) -> None:
        self.ins_map.clear()
        self.open_ids.clear()
        self.entry_done = False
        self.exit_done = False
        self.sent_done = False
        self.close_done = False
        self.log_done = False

    # 从 cache 读取 node 已加载的 USDT 本位永续。
    def _load_ins(self) -> None:
        self.ins = {
            ins.id: ins
            for ins in self.cache.instruments()
            if ins is not None and str(ins.id).split(".")[0].endswith("USDT-PERP")
        }
        if not self.ins:
            raise RuntimeError("No USDT perpetual instruments found in cache")

    # 拉全量 premiumIndex，并按阶段写 pre/post 价格。
    def _load_snap(self, phase: str) -> bool:
        start = perf_counter()
        try:
            response = requests.get(
                f"{self.config.api_url}/fapi/v1/premiumIndex",
                proxies=self.proxies,
                timeout=float(self.config.api_timeout),
            )
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list):
                raise TypeError("premiumIndex response is not a list")
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            elapsed_ms = (perf_counter() - start) * 1000
            self.log.error(
                "FundingCollector snapshot failed: "
                f"phase={phase}, funding={self._iso(self.fund_ns)}, "
                f"elapsed_ms={elapsed_ms:.2f}, error={exc}"
            )
            return False
        passed = 0
        priced = 0
        skipped = 0

        for item in rows:
            try:
                ins_id = InstrumentId.from_str(f"{item['symbol']}-PERP.BINANCE")
                if ins_id not in self.ins:
                    continue
                price = Decimal(str(item["markPrice"]))
                if phase == "pre":
                    if int(item["nextFundingTime"]) * 1_000_000 != self.fund_ns:
                        continue
                    rate = Decimal(str(item["lastFundingRate"]))
                    if abs(rate) < self.min_rate:
                        self.ins_map.pop(ins_id, None)
                        continue
                    row = self.ins_map.setdefault(ins_id, {})
                    row["rate"] = rate
                    row["pre"] = price
                    passed += 1
                elif ins_id in self.ins_map and "post" not in self.ins_map[ins_id]:
                    self.ins_map[ins_id]["post"] = price
                    priced += 1
            except (KeyError, ValueError, TypeError):
                skipped += 1

        elapsed_ms = (perf_counter() - start) * 1000
        self.log.info(
            "FundingCollector snapshot: "
            f"phase={phase}, funding={self._iso(self.fund_ns)}, "
            f"elapsed_ms={elapsed_ms:.2f}, rows={len(rows)}, "
            f"candidates={len(self.ins_map)}, passed={passed}, "
            f"priced={priced}, skipped={skipped}"
        )
        return True

    # 解析 trade 模式允许下单的 base symbol 范围。
    def _load_scope(self) -> None:
        raw = self.config.trade_scope.strip()
        if raw.lower() == "all":
            self.trade_ids = None
            return

        by_base = {
            str(ins_id).split(".")[0].replace("USDT-PERP", "").upper(): ins_id
            for ins_id in self.ins
        }
        ids = set()
        for item in raw.split(","):
            key = item.strip().upper()
            if not key:
                continue
            if key not in by_base:
                raise RuntimeError(f"Unknown trade_scope instrument: {item}")
            ids.add(by_base[key])
        if not ids:
            raise RuntimeError("trade_scope must be all or a non-empty base symbol list")
        self.trade_ids = ids

    # 推进到下一个 UTC 4h 准点。
    def _next_time(self) -> None:
        now_ns = self.clock.timestamp_ns()
        four_ns = 4 * 60 * 60 * 1_000_000_000
        self.fund_ns = ((now_ns // four_ns) + 1) * four_ns
        self.log.info(
            "FundingCollector next time: "
            f"now={self._iso(now_ns)}, funding={self._iso(self.fund_ns)}"
        )

    def _side(self, rate: Decimal) -> OrderSide:
        return OrderSide.SELL if rate > 0 else OrderSide.BUY

    def _side_name(self, side: OrderSide) -> str:
        return "BUY" if side == OrderSide.BUY else "SELL"

    def _qty(self, ins: Instrument, price: Decimal):
        raw_qty = self.notional / price
        step = Decimal(str(ins.size_increment))
        if step > 0:
            steps = (raw_qty / step).to_integral_value(rounding=ROUND_CEILING)
            raw_qty = steps * step
        return ins.make_qty(raw_qty)

    # watch 模式只写粗略收益估算。
    def _write_watch(self) -> tuple[int, Decimal, Decimal, Decimal]:
        row_cnt = 0
        fund_sum = Decimal("0")
        pnl_sum = Decimal("0")
        net_sum = Decimal("0")
        with self.log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for ins_id, row in sorted(self.ins_map.items(), key=lambda item: str(item[0])):
                if "rate" not in row or "pre" not in row or "post" not in row:
                    continue
                rate = row["rate"]
                pre = row["pre"]
                post = row["post"]
                side = self._side(rate)
                fund_gain = abs(rate) * self.notional
                if side == OrderSide.SELL:
                    px_pnl = (pre - post) / pre * self.notional
                else:
                    px_pnl = (post - pre) / pre * self.notional
                net_est = fund_gain + px_pnl
                row_cnt += 1
                fund_sum += fund_gain
                pnl_sum += px_pnl
                net_sum += net_est
                writer.writerow(
                    [
                        self._iso(self.fund_ns),
                        ins_id,
                        rate,
                        self._side_name(side),
                        pre,
                        post,
                        self.notional,
                        fund_gain,
                        px_pnl,
                        net_est,
                    ],
                )
        self.log_done = True
        return row_cnt, fund_sum, pnl_sum, net_sum

    def _init_log(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                [
                    "funding_time",
                    "instrument",
                    "rate",
                    "side",
                    "pre_px",
                    "post_px",
                    "notional",
                    "funding_gain",
                    "px_pnl",
                    "net_est",
                ],
            )

    def _reset_state(self) -> None:
        self.ins_map.clear()
        self.open_ids.clear()
        self.entry_done = False
        self.exit_done = False
        self.sent_done = False
        self.close_done = False
        self.log_done = False
        self._next_time()

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()
