from decimal import Decimal

from nautilus_trader.config import NautilusConfig, StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId


class PairConfig(NautilusConfig, frozen=True):
    name: str
    etf: InstrumentId
    underlying: InstrumentId
    multiple: int


class EtfArbConfig(StrategyConfig, frozen=True):
    pairs: list[PairConfig]
    capital: Decimal
    gross_leverage: Decimal
    entry_gap: float
    confirmation_ms: int
    batches: int
    exit_gap: float
    exit_hour_step: float
    exit_gap_cap: float
    open_block_minutes: int
    close_block_minutes: int
    close_flatten_minutes: int
    funding_block_minutes: int
    calibration_minutes: int
    calibration_cap: float
    calibration_iqr: float
    calibration_history_range: float
    calibration_history_distance: float
    calibration_history_weight: float
    history_days: int
    anchor_window_minutes: int
    anchor_basis_days: int
    anchor_basis_cap: float
    anchor_reference_cap: float
    reference_path: str
    max_quote_age_ms: int
    max_depth_age_ms: int
    max_funding_age_ms: int
    depth_levels: int
    depth_update_ms: int
    order_timeout_ms: int
    hedge_timeout_ms: int
    retry_interval_ms: int
    watchdog_ms: int
    taker_fee: float
    minimum_net_edge: float
    execution_depth_fraction: float
    max_execution_gap_slippage: float
    state_path: str
    audit_path: str
    data_client: str
    exec_client: str
    account_id: str
    bootstrap: bool
    capture_pre_seconds: int
    capture_post_seconds: int
    capture_ring_records: int
    capture_queue_batches: int
    capture_file_mb: int
    capture_max_disk_mb: int
    capture_min_free_mb: int

    def __post_init__(self):
        if not self.pairs or len({p.name for p in self.pairs}) != len(self.pairs):
            raise ValueError('pairs must contain unique names')
        if any(p.etf == p.underlying or p.multiple == 0 for p in self.pairs):
            raise ValueError('invalid ETF pair')
        if self.capital <= 0 or self.gross_leverage <= 0 or self.batches <= 0:
            raise ValueError('capital, leverage and batches must be positive')
        if not 0 < self.exit_gap <= self.exit_gap_cap < self.entry_gap:
            raise ValueError('exit thresholds must be below entry_gap')
        if not 0 < self.close_flatten_minutes < self.close_block_minutes:
            raise ValueError('flatten window must be inside entry block')
        if self.calibration_minutes != 5 or self.history_days < 28:
            raise ValueError('baseline requires first five minutes and 28-day history')
        if not 0 <= self.calibration_history_weight <= 1:
            raise ValueError('invalid history weight')
        if self.anchor_window_minutes < 5 or self.anchor_basis_days < 3 or not 0 < self.anchor_basis_cap < self.entry_gap:
            raise ValueError('invalid reference anchor controls')
        if not 0 < self.calibration_cap < self.anchor_reference_cap < self.entry_gap:
            raise ValueError('reference adjustment cap must be between calibration_cap and entry_gap')
        if min(self.confirmation_ms, self.max_quote_age_ms, self.max_depth_age_ms,
               self.max_funding_age_ms, self.order_timeout_ms, self.hedge_timeout_ms,
               self.retry_interval_ms, self.watchdog_ms) <= 0:
            raise ValueError('execution time limits must be positive')
        if self.depth_levels != 5 or self.depth_update_ms != 100:
            raise ValueError('Binance L5 requires depth=5, update_speed=100')
        if self.taker_fee < 0 or self.minimum_net_edge < 0:
            raise ValueError('cost limits must be nonnegative')
        if not 0 < self.execution_depth_fraction <= 1 or not 0 <= self.max_execution_gap_slippage < self.entry_gap - self.exit_gap_cap:
            raise ValueError('invalid execution depth or slippage limit')
        if min(self.capture_pre_seconds, self.capture_post_seconds, self.capture_ring_records,
               self.capture_queue_batches, self.capture_file_mb, self.capture_max_disk_mb,
               self.capture_min_free_mb) <= 0:
            raise ValueError('capture limits must be positive')
