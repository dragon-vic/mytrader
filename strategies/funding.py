from __future__ import annotations

import csv
from time import perf_counter
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_CEILING
from pathlib import Path

import requests
from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


class FundingConfig(StrategyConfig, frozen=True):
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
    taker_fee: Decimal = Decimal("0.0005")


class Funding(Strategy):
    PRE_TIMER = "funding_pre"
    FREEZE_TIMER = "funding_freeze"
    RATE_TIMER = "funding_rate"
    POST_TIMER = "funding_post"

    def __init__(self, config: FundingConfig) -> None:
        super().__init__(config)
        self.mode = config.mode.lower()
        self.notional = Decimal(str(config.trade_notional))
        self.min_rate = Decimal(str(config.min_rate))
        self.taker_fee = Decimal(str(config.taker_fee))
        self.fund_ns = 0
        self.entry_done = False
        self.exit_done = False
        self.sent_done = False
        self.close_done = False
        self.log_done = False
        self.ins: dict[InstrumentId, Instrument] = {}
        self.trade_ids: set[InstrumentId] | None = None
        self.ins_map: dict[InstrumentId, dict[str, Decimal | OrderSide]] = {}
        self.open_ids: set[InstrumentId] = set()
        self.log_path = Path(config.event_log_path)
        self.proxies = {"http": config.proxy_url, "https": config.proxy_url} if config.proxy_url else None

    # 启动时注册 NT 定时器，资金费和价格用 REST 全量快照。
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

        self._schedule_next()

        mode_name = "观察" if self.mode == "watch" else "交易"
        self.log.info(
            f"资金费率监控启动，模式{mode_name}，交易对{len(self.ins)}个，"
            f"阈值{self._bps(self.min_rate)}bps，单币名义{self._money(self.notional)}USDT"
        )

    # clock alert 负责 t-2/t-1/t+2 三个确定动作。
    def _on_time(self, event: TimeEvent) -> None:
        name = event.name.split(":", 1)[0]
        if name == self.PRE_TIMER:
            self._pre_funding()
        elif name == self.FREEZE_TIMER:
            self._freeze_funding()
        elif name == self.RATE_TIMER:
            self._rate_funding()
        elif name == self.POST_TIMER:
            self._post_funding()

    # t-2 拉 rate 和 pre mark。
    def _pre_funding(self) -> None:
        if self.sent_done or self.entry_done:
            return
        stats = self._load_snap("pre")
        if stats is None:
            return
        self.log.info(
            "拉取资金费率成功，"
            f"价格{stats['rows']}条，阈值{self._bps(self.min_rate)}bps，"
            f"候选{len(self.ins_map)}个，耗时{self._ms(stats['elapsed_ms'])}ms，"
            f"候选 {self._rate_list(limit=3)}"
        )
        order_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        if self.clock.timestamp_ns() < order_ns:
            self.entry_done = True
        else:
            self.sent_done = True
            self.log.info("跳过本轮，资金费率拉取太晚")

    # t-1 冻结观察列表或提交订单。
    def _freeze_funding(self) -> None:
        if self.sent_done:
            return
        if not self.entry_done:
            self.sent_done = True
            self.log.info("跳过本轮，候选未准备好")
            return
        if self.mode == "trade":
            rows = []
            for ins_id, row in self.ins_map.items():
                if self.trade_ids is not None and ins_id not in self.trade_ids:
                    continue
                if ins_id not in self.ins or "rate" not in row or "pre" not in row:
                    continue
                rows.append((ins_id, row))
            if not rows:
                self.log.info(f"交易模式，候选{len(self.ins_map)}个，无可下单交易对")
            else:
                ins_id, row = max(rows, key=lambda item: abs(item[1]["rate"]))
                ins = self.ins[ins_id]
                side = self._side(row["rate"])
                row["side"] = side
                qty = self._qty(ins, row["pre"])
                order = self.order_factory.market(
                    instrument_id=ins_id,
                    order_side=side,
                    quantity=qty,
                    time_in_force=TimeInForce.GTC,
                )
                self.submit_order(order)
                self.open_ids.add(ins_id)
                self.log.info(
                    f"交易模式，候选{len(self.ins_map)}个，选择{self._base(ins_id)}，"
                    f"费率{self._bps(row['rate'])}bps，方向{self._side_cn(side)}，"
                    f"名义{self._money(self.notional)}USDT"
                )
        else:
            for row in self.ins_map.values():
                if "rate" in row:
                    row["side"] = self._side(row["rate"])
            px_cnt = sum(1 for row in self.ins_map.values() if "pre" in row)
            self.log.info(f"观察模式，冻结{px_cnt}个候选，不下单")

        self.sent_done = True

    # 准点复核候选的实际资金费率。
    def _rate_funding(self) -> None:
        stats = self._load_snap("rate")
        if stats is None:
            return
        self.log.info(
            f"准点复核成功，命中{stats['priced']}个，"
            f"耗时{self._ms(stats['elapsed_ms'])}ms，前三 {self._rate_list(limit=3)}"
        )

    # t+2 拉 post mark，并完成平仓或 watch 记账。
    def _post_funding(self) -> None:
        stats = None
        if not self.exit_done:
            stats = self._load_snap("post")
            self.exit_done = True
        if self.mode == "trade" and not self.close_done:
            close_cnt = 0
            for ins_id in list(self.open_ids):
                self.close_all_positions(ins_id)
                close_cnt += 1
            self.close_done = True
            self.log.info(
                f"交易模式，平仓{close_cnt}个，候选{len(self.ins_map)}个"
            )
            self._reset_state()
            self._schedule_next()
        elif self.mode == "watch" and not self.log_done:
            row_cnt, fund_sum, pnl_sum, net_sum = self._write_watch()
            if stats is not None:
                self._log_result(stats, row_cnt, fund_sum, pnl_sum, net_sum)
            self._reset_state()
            self._schedule_next()

    def on_stop(self) -> None:
        names = set(self.clock.timer_names)
        for kind in (self.PRE_TIMER, self.FREEZE_TIMER, self.RATE_TIMER, self.POST_TIMER):
            name = self._timer_name(kind)
            if name in names:
                self.clock.cancel_timer(name)
        for ins_id in self.ins:
            self.cancel_all_orders(ins_id)

        if self.config.stop_close and self.mode == "trade":
            for ins_id in self.open_ids:
                self.close_all_positions(ins_id)

        self.log.info("资金费率监控停止")

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
    def _load_snap(self, phase: str) -> dict[str, Decimal | float | int] | None:
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
            name = {"pre": "拉取资金费率", "rate": "准点复核"}.get(phase, "结算价格")
            self.log.error(f"{name}失败，错误 {exc}，耗时{self._ms(elapsed_ms)}ms")
            return None
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
                elif phase == "rate" and ins_id in self.ins_map:
                    self.ins_map[ins_id]["rate"] = Decimal(str(item["lastFundingRate"]))
                    self.ins_map[ins_id]["rate_px"] = price
                    priced += 1
                elif ins_id in self.ins_map and "post" not in self.ins_map[ins_id]:
                    self.ins_map[ins_id]["post"] = price
                    priced += 1
            except (KeyError, ValueError, TypeError):
                skipped += 1

        elapsed_ms = (perf_counter() - start) * 1000
        return {
            "rows": len(rows),
            "elapsed_ms": elapsed_ms,
            "candidates": len(self.ins_map),
            "passed": passed,
            "priced": priced,
            "skipped": skipped,
        }

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

    # 为下一次 funding 注册一次性 clock alert。
    def _schedule_next(self) -> None:
        self._next_time()
        now_ns = self.clock.timestamp_ns()
        pre_ns = self.fund_ns - int(self.config.entry_sec * 1_000_000_000)
        freeze_ns = self.fund_ns - int(self.config.entry_before * 1_000_000_000)
        rate_ns = self.fund_ns
        post_ns = self.fund_ns + int(self.config.exit_sec * 1_000_000_000)
        if now_ns >= pre_ns:
            four_ns = 4 * 60 * 60 * 1_000_000_000
            self.fund_ns += four_ns
            pre_ns += four_ns
            freeze_ns += four_ns
            rate_ns += four_ns
            post_ns += four_ns
        self.clock.set_time_alert_ns(
            self._timer_name(self.PRE_TIMER),
            pre_ns,
            callback=self._on_time,
            allow_past=False,
        )
        self.clock.set_time_alert_ns(
            self._timer_name(self.FREEZE_TIMER),
            freeze_ns,
            callback=self._on_time,
            allow_past=False,
        )
        self.clock.set_time_alert_ns(
            self._timer_name(self.RATE_TIMER),
            rate_ns,
            callback=self._on_time,
            allow_past=False,
        )
        self.clock.set_time_alert_ns(
            self._timer_name(self.POST_TIMER),
            post_ns,
            callback=self._on_time,
            allow_past=False,
        )

    def _timer_name(self, kind: str) -> str:
        return f"{kind}:{self.fund_ns}"

    def _rate_list(self, limit: int) -> str:
        rows = [
            (ins_id, row)
            for ins_id, row in self.ins_map.items()
            if "rate" in row
        ]
        rows.sort(key=lambda item: abs(item[1]["rate"]), reverse=True)
        parts = []
        for ins_id, row in rows[:limit]:
            side = row.get("side") or self._side(row["rate"])
            parts.append(f"{self._base(ins_id)} {self._bps(row['rate'])}bps {self._side_cn(side)}")
        if not parts:
            return "无"
        return "，".join(parts)

    def _top_moves(self, limit: int) -> str:
        rows = [
            (ins_id, row)
            for ins_id, row in self.ins_map.items()
            if "rate" in row and "pre" in row and "post" in row
        ]
        rows.sort(key=lambda item: abs(item[1]["rate"]), reverse=True)
        parts = []
        for ins_id, row in rows[:limit]:
            side = row.get("side") or self._side(row["rate"])
            move = self._px_pnl(row["pre"], row["post"], side) / self.notional
            parts.append(
                f"{self._base(ins_id)} {self._bps(row['rate'])}bps 价差{self._bps(move)}bps"
            )
        if not parts:
            return "无"
        return "，".join(parts)

    def _log_result(
        self,
        stats: dict[str, Decimal | float | int],
        row_cnt: int,
        fund_sum: Decimal,
        pnl_sum: Decimal,
        net_sum: Decimal,
    ) -> None:
        total_notional = self.notional * Decimal(row_cnt)
        fee = total_notional * self.taker_fee * Decimal("2")
        net_after_fee = net_sum - fee
        if total_notional > 0:
            gross_bps = fund_sum / total_notional
            pnl_bps = pnl_sum / total_notional
            fee_bps = fee / total_notional
            net_bps = net_after_fee / total_notional
        else:
            gross_bps = pnl_bps = fee_bps = net_bps = Decimal("0")
        self.log.info(
            f"结算价格成功，命中{stats['priced']}个，耗时{self._ms(stats['elapsed_ms'])}ms，"
            f"总收益{self._bps(net_bps)}bps，净额{self._money(net_after_fee)}USDT，"
            f"毛收益{self._bps(gross_bps)}bps，价差{self._bps(pnl_bps)}bps，"
            f"手续费{self._bps(fee_bps)}bps，前三 {self._top_moves(limit=3)}"
        )

    def _base(self, ins_id: InstrumentId) -> str:
        return str(ins_id).split(".")[0].replace("USDT-PERP", "")

    def _side_cn(self, side: OrderSide) -> str:
        return "买入" if side == OrderSide.BUY else "卖出"

    def _bps(self, value: Decimal) -> str:
        return f"{value * Decimal('10000'):.2f}"

    def _ms(self, value: Decimal | float | int) -> str:
        return f"{float(value):.2f}"

    def _money(self, value: Decimal) -> str:
        return f"{value:.2f}"

    def _px_pnl(self, pre: Decimal, post: Decimal, side: OrderSide) -> Decimal:
        if side == OrderSide.SELL:
            return (pre - post) / pre * self.notional
        return (post - pre) / pre * self.notional

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
                side = row.get("side") or self._side(rate)
                fund_gain = abs(rate) * self.notional
                px_pnl = self._px_pnl(pre, post, side)
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

    def _iso(self, ts_ns: int) -> str:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC).isoformat()
