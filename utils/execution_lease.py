import hashlib

from utils.constants import PROJECT_ROOT
from utils.runtime_lock import RuntimeLock


class ExecutionLease:
    def __init__(self, settings):
        self.locks = []
        folder = PROJECT_ROOT / '.runtime' / 'execution-locks'
        folder.mkdir(parents=True, exist_ok=True)
        resources = set()
        for client in settings['node']['exec']['clients'].values():
            if not client['enabled']:
                continue
            if client['markets_all']:
                raise ValueError('live execution requires explicit instrument ownership')
            scope = f"{settings['mode']}:{client['venue']}:{client.get('account_type', '')}"
            for market in client['markets']:
                resources.add(scope + ':' + market['instrument_id'])
        try:
            for resource in sorted(resources):
                digest = hashlib.sha256(resource.encode()).hexdigest()
                self.locks.append(RuntimeLock(folder / (digest + '.lock')))
        except BaseException:
            self.close()
            raise

    def close(self):
        for lock in self.locks:
            lock.close()
        self.locks.clear()
