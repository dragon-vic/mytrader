"""每阶段一次账户读取，阶段内使用成交与在途预留记账。"""
from dataclasses import dataclass


@dataclass
class CapitalState:
    cash: float
    pnl: float
    realized: float
    fees: float
    checked_ns: int


def portfolio_risk(stages, markets, pairs):
    pnl, margin = 0., 0.
    for stage in stages.values():
        pair = pairs[stage.pair]
        for quantity, average, iid, leverage in zip(stage.quantities, stage.average_prices,
                                                    (pair.etf, pair.underlying), stage.native_leverages):
            if not quantity:
                continue
            market = markets[iid]
            if min(market.bid, market.ask) <= 0:
                return float('-inf'), float('inf')
            price = market.bid if quantity > 0 else market.ask
            pnl += quantity * (price - average)
            margin += abs(quantity) * max(average, market.ask, market.mark) / leverage
    return pnl, margin


def capital_headroom(capital, stages, orders, markets, pairs, realized, fees, fraction):
    if capital is None:
        return 0.
    pnl, margin = portfolio_risk(stages, markets, pairs)
    equity = capital.cash + realized - capital.realized - (fees - capital.fees) + pnl - capital.pnl
    reserved = sum(p.reserved_margin * max(0., 1 - p.filled / abs(p.quantity))
                   for p in orders.values() if not p.terminal)
    return max(0., equity * fraction - margin - reserved)


def account_fill(stage, leg, signed, price, fee):
    held, average = stage.quantities[leg], stage.average_prices[leg]
    if held * signed < 0:
        stage.realized_pnl += min(abs(signed), abs(held)) * (price - average) * (1 if held > 0 else -1)
    if held * signed >= 0:
        stage.average_prices[leg] = (abs(held) * average + abs(signed) * price) / (abs(held) + abs(signed))
    elif abs(signed) > abs(held):
        stage.average_prices[leg] = price
    elif abs(signed) == abs(held):
        stage.average_prices[leg] = 0.
    stage.fees += fee
