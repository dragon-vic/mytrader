from __future__ import annotations

import gzip
import queue
import shutil
import struct
import threading
import time
from collections import deque
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

import msgspec


@dataclass(frozen=True, slots=True)
class HistoryBlock:
    rows: tuple
    cutoff_ns: int
    seeds: tuple


class EventRecorder:
    """行情用C编码器转成不可变字节，后台批量压缩写盘。"""

    def __init__(self, path, instruments, config, metadata):
        self.path = Path(path)
        self.pre_ns = config.capture_pre_seconds * 1_000_000_000
        self.post_ns = config.capture_post_seconds * 1_000_000_000
        self.capacity = config.capture_ring_records
        self.rings = {iid: deque(maxlen=self.capacity) for iid in instruments}
        self.evicted_ns = {iid: 0 for iid in instruments}
        self.latest = {iid: {} for iid in instruments}
        self.active = {}
        self.instrument_users = {iid: 0 for iid in instruments}
        self.sequence = 0
        self.batch = []
        self.queue = queue.Queue(maxsize=config.capture_queue_batches)
        self.stopping = threading.Event()
        self.error = None
        self.dropped = 0
        self.accepted = 0
        self.written = 0
        self.processed = 0
        self.history_queued = 0
        self.history_processed = 0
        self.bytes_written = 0
        self.max_queue = 0
        self.pending_gap = None
        self.started_ns = time.time_ns()
        self.max_bytes = config.capture_max_disk_mb * 1024 * 1024
        self.reserve_bytes = config.capture_min_free_mb * 1024 * 1024
        self.rotate_bytes = config.capture_file_mb * 1024 * 1024
        self.metadata = metadata
        self.encoder = msgspec.json.Encoder()
        self.worker = threading.Thread(target=self._writer, name='etf-capture-writer', daemon=True)
        self.worker.start()

    # 字节不进入Python循环GC，避免大环形缓存触发全代扫描。
    def market(self, kind, iid, now, event_ns, init_ns, values):
        self.sequence += 1
        row = struct.pack('!Q', now) + self.encoder.encode((kind, self.sequence, now, event_ns, init_ns, str(iid), *values)) + b'\n'
        ring = self.rings[iid]
        if len(ring) == self.capacity:
            self.evicted_ns[iid] = struct.unpack_from('!Q', ring[0])[0]
        ring.append(row)
        self.latest[iid][kind] = row
        if self.instrument_users[iid]:
            self._append(row)

    def begin(self, name, ids, now, state):
        case = f'{name}-{now}'
        self.active[name] = {'id': case, 'ids': ids, 'until': now + self.post_ns}
        coverage = {}
        for iid in ids:
            rows = self.rings[iid]
            coverage[str(iid)] = {'oldest_available_ns': struct.unpack_from('!Q', rows[0])[0] if rows else None,
                                  'truncated_by_capacity': self.evicted_ns[iid] >= now - self.pre_ns,
                                  'recording_started_ns': self.started_ns}
        self.event('capture_begin', now, case=case, pair=name, requested_pre_ns=self.pre_ns,
                   coverage=coverage, state=state)
        for iid in ids:
            # 相交事件复用已经记录的行情；新激活合约补存触发前窗口。
            if not self.instrument_users[iid]:
                block = HistoryBlock(tuple(self.rings[iid]), now - self.pre_ns, tuple(self.latest[iid].values()))
                if not self._append(block):
                    self.event('prebuffer_dropped', now, case=case, instrument=str(iid), reason='history_backlog_or_writer_failure')
            self.instrument_users[iid] += 1
        return case

    def touch(self, name, now):
        if name in self.active:
            self.active[name]['until'] = now + self.post_ns

    def end_due(self, now, held_pair):
        for name, case in list(self.active.items()):
            if name == held_pair:
                self.touch(name, now)
            elif now >= case['until']:
                self.event('capture_end', now, case=case['id'], pair=name, reason='quiet_tail_completed')
                for iid in case['ids']:
                    self.instrument_users[iid] -= 1
                del self.active[name]

    def event(self, kind, now, **values):
        self._append({'kind': kind, 'time_ns': now, **values})

    def _append(self, row):
        if self.error:
            self.dropped += 1
            return False
        if isinstance(row, HistoryBlock):
            if self.history_queued - self.history_processed >= 2 * len(self.rings):
                self.dropped += len(row.rows)
                return False
            self.history_queued += 1
        self.batch.append(row)
        if len(self.batch) >= 256:
            self.flush()
        return True

    def flush(self):
        if not self.batch and self.pending_gap is None:
            return
        batch, self.batch = self.batch, []
        gap = self.pending_gap
        output = [gap, *batch] if gap else batch
        try:
            self.queue.put_nowait(output)
            self.pending_gap = None
            self.accepted += len(output)
            self.max_queue = max(self.max_queue, self.queue.qsize())
        except queue.Full:
            self.history_queued -= sum(isinstance(row, HistoryBlock) for row in batch)
            lost = sum(len(row.rows) + len(row.seeds) if isinstance(row, HistoryBlock) else 1 for row in batch)
            self.dropped += lost
            if batch:
                times = [row.cutoff_ns if isinstance(row, HistoryBlock) else
                         struct.unpack_from('!Q', row)[0] if isinstance(row, bytes) else row['time_ns'] for row in batch]
                self.pending_gap = {'kind': 'recording_gap', 'time_ns': max(times),
                                    'dropped_records': lost + (gap['dropped_records'] if gap else 0),
                                    'first_callback_ns': min(min(times), gap['first_callback_ns'] if gap else min(times)),
                                    'last_callback_ns': max(max(times), gap['last_callback_ns'] if gap else max(times)),
                                    'reason': 'writer_queue_full'}

    def status(self):
        return {'alive': self.worker.is_alive(), 'error': self.error, 'dropped_records': self.dropped,
                'accepted_items': self.accepted, 'processed_items': self.processed, 'written_records': self.written,
                'queued_batches': self.queue.qsize(), 'max_queued_batches': self.max_queue,
                'pending_history_blocks': self.history_queued - self.history_processed,
                'compressed_bytes': self.bytes_written, 'unwritten_gap': self.pending_gap}

    def close(self, now):
        for name, case in list(self.active.items()):
            self.event('capture_end', now, case=case['id'], pair=name, reason='strategy_stop')
        self.flush()
        self.stopping.set()
        self.worker.join(timeout=5)
        return self.status()

    # 前窗口只在触发线程复制字节引用，过滤与展开在后台执行。
    def _expand(self, batch):
        expanded = []
        for item in batch:
            if isinstance(item, HistoryBlock):
                rows = (row for row in item.seeds if struct.unpack_from('!Q', row)[0] < item.cutoff_ns)
                history = (row for row in item.rows if struct.unpack_from('!Q', row)[0] >= item.cutoff_ns)
                rows = chain(rows, history)
            else:
                rows = (item,)
            for row in rows:
                expanded.append(row)
                if len(expanded) == 256:
                    yield expanded
                    expanded = []
            if isinstance(item, HistoryBlock):
                self.history_processed += 1
        if expanded:
            yield expanded

    def _writer(self):
        stream = None
        raw = None
        part = 0
        uncompressed = 0
        completed_bytes = 0
        next_flush = 0.
        encoder = msgspec.json.Encoder()
        try:
            self.path.mkdir(parents=True, exist_ok=True)
            metadata = {'version': 1, 'started_ns': self.started_ns, 'metadata': self.metadata,
                        'rows': {'q': ['kind', 'seq', 'callback_ns', 'event_ns', 'init_ns', 'instrument', 'bid', 'ask', 'bid_size', 'ask_size'],
                                 'd': ['kind', 'seq', 'callback_ns', 'event_ns', 'init_ns', 'instrument', 'bids', 'asks', 'exchange_sequence'],
                                 'm': ['kind', 'seq', 'callback_ns', 'event_ns', 'init_ns', 'instrument', 'mark', 'index', 'estimated_settle', 'funding_rate', 'next_funding_ns']}}
            (self.path / 'metadata.json').write_bytes(encoder.encode(metadata))
            while not self.stopping.is_set() or not self.queue.empty():
                try:
                    batch = self.queue.get(timeout=.1)
                except queue.Empty:
                    batch = []
                now = time.monotonic()
                if now >= next_flush:
                    if stream:
                        stream.flush()
                        raw.flush()
                        self.bytes_written = completed_bytes + raw.tell()
                    if shutil.disk_usage(self.path).free < self.reserve_bytes:
                        raise OSError('capture disk reserve reached')
                    if self.bytes_written >= self.max_bytes:
                        raise OSError('capture run disk budget reached')
                    (self.path / 'writer_status.json').write_bytes(encoder.encode(self.status()))
                    next_flush = now + 1.
                if not batch:
                    continue
                for expanded in self._expand(batch):
                    if stream is None or uncompressed >= self.rotate_bytes:
                        if stream:
                            stream.close()
                            completed_bytes += raw.tell()
                            raw.close()
                        part += 1
                        raw = (self.path / f'events-{part:06d}.jsonl.gz').open('xb')
                        stream = gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=1)
                        uncompressed = 0
                    data = b''.join(row[8:] if isinstance(row, bytes) else encoder.encode(row) + b'\n' for row in expanded)
                    stream.write(data)
                    uncompressed += len(data)
                    self.written += len(expanded)
                self.processed += len(batch)
        except Exception as exc:
            self.error = f'{type(exc).__name__}: {exc}'
        finally:
            try:
                if stream:
                    stream.close()
                if raw:
                    self.bytes_written = completed_bytes + raw.tell()
                    raw.close()
                status = self.status()
                status['alive'] = False
                status['complete'] = not self.error and not self.dropped and self.accepted == self.processed
                (self.path / 'writer_status.json').write_bytes(encoder.encode(status))
            except Exception as exc:
                self.error = self.error or f'{type(exc).__name__}: {exc}'
