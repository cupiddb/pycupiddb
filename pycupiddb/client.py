from typing import Any, List, Optional, Literal, Union
import pandas as pd

from .commands import SyncCommand, RowFilter


class CupidClient(SyncCommand):

    def __init__(self, host: str, port: Union[int, str]):
        port_number = int(port) if isinstance(port, str) else port
        super().__init__(host=host, port=port_number)

    def set(self, key: str, value: Any, timeout: float = 0.0):
        if isinstance(value, pd.DataFrame):
            self._set_record_batch(key=key, value=value, timeout=timeout, add_only=False)
        elif isinstance(value, int):
            self._set_int(key=key, value=value, timeout=timeout, add_only=False)
        elif isinstance(value, float):
            self._set_float(key=key, value=value, timeout=timeout, add_only=False)
        else:
            self._set_pickle(key=key, value=value, timeout=timeout, add_only=False)

    def add(self, key: str, value: Any, timeout: float = 0.0) -> bool:
        if isinstance(value, pd.DataFrame):
            return self._set_record_batch(key=key, value=value, timeout=timeout, add_only=True)
        elif isinstance(value, int):
            return self._set_int(key=key, value=value, timeout=timeout, add_only=True)
        elif isinstance(value, float):
            return self._set_float(key=key, value=value, timeout=timeout, add_only=True)
        else:
            return self._set_pickle(key=key, value=value, timeout=timeout, add_only=True)

    def incr(self, key: str, delta: int = 1) -> int:
        return self._incr(key=key, delta=delta)

    def incr_float(self, key: str, delta: float = 1.0) -> float:
        return self._incr_float(key=key, delta=delta)

    def get_dataframe(
        self,
        key: str,
        columns: List[str] = [],
        filter_operation: Literal['AND', 'OR'] = 'AND',
        filters: List[RowFilter] = [],
        result_cache_timeout: float = 0.0,
        compression_type: Literal['', 'lz4', 'zstd'] = ''
    ) -> Optional[pd.DataFrame]:
        return self._get_dataframe(key=key, columns=columns, filter_operation=filter_operation,
                                   filters=filters, result_cache_timeout=result_cache_timeout,
                                   compression_type=compression_type)

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._get(key=key, default=default)

    def delete(self, key: str) -> bool:
        return self._delete(key=key)

    def delete_many(self, keys: List[str]) -> int:
        return self._delete_many(keys=keys)

    def touch(self, key: str, timeout: float) -> bool:
        return self._touch(key=key, timeout=timeout)

    def ttl(self, key: str) -> Optional[float]:
        return self._ttl(key=key)

    def has_key(self, key: str) -> bool:
        return self._has_key(key=key)

    def keys(self, pattern: Optional[str] = None) -> list:
        return self._keys(pattern)

    def flush(self):
        return self._flush()
