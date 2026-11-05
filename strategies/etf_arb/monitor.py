"""独立查询ETF策略状态；读取日志与检查点，仅写监控摘要，不连接交易接口。"""
import json
import os
import tempfile
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[1]))
from strategies.etf_arb.state import read_state
ZONE = timezone(timedelta(hours=8))


def timestamp(ns):
    return datetime.fromtimestamp(ns / 1e9, ZONE).isoformat(timespec='seconds') if ns else None


def collect(root=ROOT):
    now = time.time_ns()
    midnight = datetime.fromtimestamp(now / 1e9, ZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    day_ns = int(midnight.timestamp() * 1e9)
    folders = sorted(p for p in (root / 'report').glob('live-*') if (p / 'etf_events.jsonl').exists())
    if not folders:
        raise FileNotFoundError('ETF strategy has no live event log')
    # 上一日启动的最后一个进程可能持续运行至今日。
    split = 'live-' + midnight.strftime('%Y%m%d')
    earlier = [p for p in folders if p.name < split]
    selected = earlier[-1:] + [p for p in folders if p.name >= split]
    counts, peaks, fees = Counter(), {}, defaultdict(Decimal)
    health, latest_events, coverage, issues = None, [], [], []
    for folder in selected:
        interval = {'run': folder.name}
        for line in (folder / 'etf_events.jsonl').open(encoding='utf-8'):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                issues.append({'type': 'incomplete_event_line', 'run': folder.name})
                continue
            t, kind = row['time_ns'], row['kind']
            if kind in ('started', 'baseline_ready', 'stopped'):
                interval[kind] = timestamp(t)
            if folder == folders[-1]:
                if kind == 'health':
                    health = row
                if kind in ('started', 'stopped', 'startup_failed'):
                    latest_events.append(row)
            if t < day_ns or t > now:
                continue
            counts[kind] += 1
            if kind == 'fill' and row.get('commission'):
                amount, currency = row['commission'].split()
                fees[currency] += Decimal(amount)
            if kind.endswith('_error') or kind in ('exit_requested', 'order_terminal', 'strategy_fault'):
                if kind != 'order_terminal' or row.get('event') in ('OrderRejected', 'OrderDenied'):
                    issues.append({'time': timestamp(t), 'type': kind, 'pair': row.get('pair'), 'reason': row.get('reason', row.get('error'))})
            if kind == 'reference_updated' and row.get('errors'):
                issues.append({'time': timestamp(t), 'type': kind, 'reason': row['errors']})
            if kind != 'health':
                continue
            interval.setdefault('first_health_today', timestamp(t))
            interval['last_health_today'] = timestamp(t)
            for name, p in row['pairs'].items():
                if p['anchor_ready'] and p['fresh'] and p['gap'] is not None:
                    if name not in peaks or p['gap'] * 100 > peaks[name]['gap_pct']:
                        peaks[name] = {'gap_pct': p['gap'] * 100, 'time': timestamp(t)}
        coverage.append(interval)
    if health is None:
        failures = [row for row in latest_events if row['kind'] == 'startup_failed']
        if failures:
            failure = failures[-1]
            return {'schema_version': 1, 'generated_at': timestamp(now), 'data_asof': timestamp(failure['time_ns']),
                    'status': 'startup_failed', 'run': folders[-1].name, 'fault': failure['reason']}
        raise RuntimeError(f'{folders[-1].name}: no health event yet; strategy may still be starting')
    checkpoint = root / 'runtime/state.json'
    state = read_state(checkpoint)
    if state is None:
        raise FileNotFoundError(checkpoint)
    age = (now - health['time_ns']) / 1e9
    stopped = state.get('stopped', False) or bool(latest_events and latest_events[-1]['kind'] == 'stopped')
    fault = state.get('fault') or health.get('fault')
    status = ('stopped' if stopped else 'heartbeat_stale' if age > 90 else
              'fault' if fault else 'running' if health['ready'] else 'initializing')
    pairs = {name: {'current_gap_pct': p['gap'] * 100 if p['gap'] is not None and age <= 90 and not stopped else None,
                    'quote_fresh': p['fresh'] and age <= 90 and not stopped,
                    'entry_blocked': p['entry_blocked'] or age > 90 or stopped or bool(fault),
                    'baseline_ready': p['anchor_ready'], 'baseline_correction_pct': p['correction'] * 100 if p['correction'] is not None else None,
                    'baseline': p.get('baseline'),
                    'today_sampled_peak': peaks.get(name)} for name, p in health['pairs'].items()}
    node_log = folders[-1] / 'node.log'
    error_count = None
    if node_log.exists():
        with node_log.open('rb') as stream:
            stream.seek(max(0, node_log.stat().st_size - 2_000_000))
            error_count = sum(b'[ERROR]' in line or b'[CRITICAL]' in line for line in stream)
    return {'schema_version': 1, 'generated_at': timestamp(now), 'data_asof': timestamp(health['time_ns']),
            'heartbeat_age_seconds': round(age, 2), 'status': status, 'run': folders[-1].name,
            'sources': {'report_dir': str(folders[-1]), 'checkpoint': str(checkpoint),
                        'current_gap': 'latest_30_second_health', 'daily_peak': '30_second_health_samples_fresh_quotes_only',
                        'positions': 'strategy_checkpoint_not_exchange_query', 'timezone': 'Asia/Shanghai'},
            'pairs': pairs, 'stage': state['stage'], 'pending_orders': {k: v for k, v in state['orders'].items() if not v['terminal']},
            'paused_pairs': state['paused'], 'available_usdt': health['available_usdt'],
            'today': {'date': midnight.date().isoformat(), 'stages_opened': counts['stage_open'], 'stages_closed': counts['stage_closed'],
                      'order_batches_sent': counts['orders_sent'], 'fill_events': counts['fill'],
                      'fees_by_currency': {k: str(v) for k, v in fees.items()}, 'coverage': coverage},
            'latency_ms': health['quote_age_ms'], 'market_freshness': health['markets'], 'capture': health.get('capture'),
            'references': health.get('references'),
            'fault': fault,
            'recent_events': issues[-20:], 'latest_log_error_count_in_last_2mb': error_count,
            'pnl': {'value': None, 'reason': 'query exchange reports for verified realized and unrealized PnL'}}


def markdown(data):
    if data['status'] == 'startup_failed':
        return f"ETF策略启动失败。查询时间 {data['generated_at']}，实例 {data['run']}。\n原因 {data['fault']}\n"
    lines = ['# ETF策略监控', '', f"查询时间：{data['generated_at']}；行情心跳：{data['data_asof']}（{data['heartbeat_age_seconds']}秒前）。",
             f"运行状态：{data['status']}；实例：{data['run']}。", '',
             '| 组合 | 当前偏离 | 报价新鲜 | 禁开仓 | 今日采样峰值 | 峰值时间 |',
             '|---|---:|---|---|---:|---|']
    for name, pair in data['pairs'].items():
        gap, peak = pair['current_gap_pct'], pair['today_sampled_peak']
        lines.append(f"| {name} | {f'{gap:.3f}%' if gap is not None else '无实时数据'} | {'是' if pair['quote_fresh'] else '否'} | {'是' if pair['entry_blocked'] else '否'} | {str(round(peak['gap_pct'], 3))+'%' if peak else '无样本'} | {peak['time'] if peak else '—'} |")
    stage, capture = data['stage'], data['capture']
    lines += ['', f"持仓阶段：{json.dumps(stage, ensure_ascii=False) if stage else '空阶段'}。",
              f"未完成订单：{len(data['pending_orders'])}；暂停组合：{data['paused_pairs']}。",
              f"今日开/闭阶段：{data['today']['stages_opened']}/{data['today']['stages_closed']}；成交回报：{data['today']['fill_events']}；手续费：{data['today']['fees_by_currency']}。",
              f"可用USDT（心跳缓存）：{data['available_usdt']}；行情延迟ms：{data['latency_ms']}。",
              f"采集状态：{json.dumps(capture, ensure_ascii=False)}。",
              f"最近2MB节点日志ERROR/CRITICAL行数：{data['latest_log_error_count_in_last_2mb']}。", '',
              '峰值口径：北京时间当日、30秒心跳、双腿报价新鲜样本；统计范围以JSON中的coverage为准。',
              '文件按查询命令更新。持仓来自策略检查点；交易所持仓、挂单和盈亏需另行账户核对。']
    return '\n'.join(lines) + '\n'


def write_atomic(path, content):
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(content)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    snapshot = collect()
    destination = ROOT / 'monitor'
    destination.mkdir(exist_ok=True)
    write_atomic(destination / 'status.json', json.dumps(snapshot, ensure_ascii=False, indent=2))
    write_atomic(destination / 'status.md', markdown(snapshot))
    print(markdown(snapshot))
