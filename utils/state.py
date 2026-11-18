import json
import os
import tempfile
import time
from pathlib import Path


def read_state(path):
    file = Path(path)
    if os.name == 'nt':
        import ctypes
        import msvcrt
        from ctypes import wintypes

        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        create_file = kernel.CreateFileW
        create_file.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                                wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create_file.restype = wintypes.HANDLE
        # 监控读取期间允许原子替换，读取者继续持有旧的完整快照。
        for attempt in range(10):
            handle = create_file(str(file), 0x80000000, 0x7, None, 3, 0x80, None)
            if handle != ctypes.c_void_p(-1).value:
                break
            error = ctypes.get_last_error()
            if error not in (2, 3, 5, 32) or attempt == 9:
                if error in (2, 3):
                    return None
                raise ctypes.WinError(error)
            time.sleep(.005)
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY)
        with os.fdopen(descriptor, 'r', encoding='utf-8') as stream:
            return json.load(stream)
    try:
        return json.loads(file.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return None


# 只有完整且已刷盘的快照才能替换检查点。
def _replace_file(temporary, file):
    if os.name == 'nt' and file.exists():
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        replace_file = kernel.ReplaceFileW
        replace_file.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR,
                                 wintypes.DWORD, wintypes.LPVOID, wintypes.LPVOID]
        replace_file.restype = wintypes.BOOL
        # Windows的MoveFileEx在目标被读取时可能拒绝替换，ReplaceFile支持共享删除句柄。
        for attempt in range(10):
            if replace_file(str(file), str(temporary), None, 0, None, None):
                return
            error = ctypes.get_last_error()
            if error not in (5, 32, 1175) or attempt == 9:
                raise ctypes.WinError(error)
            time.sleep(.005)
    else:
        os.replace(temporary, file)


def write_state(path, payload):
    file = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=file.parent,
                                         prefix=file.name + '.', suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(payload, stream, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
            stream.flush()
            os.fsync(stream.fileno())
        _replace_file(temporary, file)
        temporary = None
        if os.name != 'nt':
            descriptor = os.open(file.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
