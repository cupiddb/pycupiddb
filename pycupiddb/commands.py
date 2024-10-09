import json
import struct
import pickle
from datetime import date, datetime
from typing import Any, List, Literal, Optional
import pyarrow as pa
import pandas as pd

from .connection import SyncConnection, Serializer


class RowFilter():

    def __init__(self, column: str, logic: Literal['gte', 'gt', 'lte', 'lt', 'eq', 'ne'],
                 value: Any, data_type: Literal['int', 'float', 'date',
                                                'datetime', 'string', 'bool']):
        assert isinstance(column, str)
        assert logic in ['gte', 'gt', 'lte', 'lt', 'eq', 'ne']
        assert data_type in ['int', 'float', 'date', 'datetime', 'string', 'bool']
        self.query_dict = {
            'col': column,
            'filter_type': logic,
        }

        if data_type == 'int':
            assert isinstance(value, int)
            self.query_dict['data_type'] = 'IN'
            self.query_dict['value_int'] = value
        elif data_type == 'float':
            assert isinstance(value, float)
            self.query_dict['data_type'] = 'FL'
            self.query_dict['value_flt'] = value
        elif data_type == 'date':
            assert isinstance(value, date)
            self.query_dict['data_type'] = 'DA'
            self.query_dict['value_int'] = (value - date(1970, 1, 1)).days
        elif data_type == 'datetime':
            assert isinstance(value, datetime)
            self.query_dict['data_type'] = 'DT'
            self.query_dict['value_int'] = int(value.timestamp() * (10**9))
        elif data_type == 'string':
            assert isinstance(value, str)
            self.query_dict['data_type'] = 'ST'
            self.query_dict['value_str'] = value
        elif data_type == 'bool':
            assert isinstance(value, bool)
            assert logic == 'eq'
            self.query_dict['data_type'] = 'BL'
            self.query_dict['value_bol'] = value


class SyncCommand(SyncConnection, Serializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _set_record_batch(self, key: str, value: pd.DataFrame, timeout: float) -> bool:
        record_barch = pa.record_batch(value)

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, record_barch.schema) as writer:
            writer.write_batch(record_barch)
        record_batch_buffer = sink.getvalue()
        rb_payload = record_batch_buffer.to_pybytes()

        return self._set_data(data_type='A', key=key, byte_data=rb_payload, timeout=timeout)

    def _set_int(self, key: str, value: int, timeout: float):
        payload = struct.pack('>q', value)
        return self._set_data(data_type='I', key=key, byte_data=payload, timeout=timeout)

    def _set_float(self, key: str, value: float, timeout: float):
        payload = struct.pack('>d', value)
        return self._set_data(data_type='F', key=key, byte_data=payload, timeout=timeout)

    def _set_pickle(self, key: str, value: Any, timeout: float):
        return self._set_data(data_type='B', key=key, byte_data=pickle.dumps(value), timeout=timeout)

    def _set_data(self, data_type: str, key: str, byte_data: bytes, timeout: float) -> bool:
        cache_time_bytes = struct.pack('>Q', int(timeout * 1000))

        key_bytes = key.encode()
        key_len = len(key_bytes)
        assert key_len < 65536
        key_length_bytes = struct.pack('>H', key_len)

        assert data_type in ['A', 'B', 'I', 'F']
        data_type_byte = data_type.encode()

        payload = cache_time_bytes + key_length_bytes + key_bytes + data_type_byte + byte_data
        response_type, payload = self.send_command(message_type='SD', payload=payload)
        return self._process_set_data(response_type, payload)

    def _incr(self, key: str, delta: int) -> int:
        payload = struct.pack('>q', delta) + key.encode()
        response_type, payload = self.send_command(message_type='II', payload=payload)
        return self._process_incr(response_type, payload)

    def _incr_float(self, key: str, delta: float) -> float:
        payload = struct.pack('>d', delta) + key.encode()
        response_type, payload = self.send_command(message_type='IF', payload=payload)
        return self._process_incr_float(response_type, payload)

    def _get_dataframe(self, key: str, columns: List[str] = [], filter_operation: str = 'AND',
                       filters: List[RowFilter] = [], result_cache_timeout: float = 0.0,
                       compression_type: Literal['', 'lz4', 'zstd'] = '') -> Optional[pd.DataFrame]:
        query_dict = {
            'key': key,
            'columns': columns,
            'filterlogic': filter_operation,
            'filter': [rf.query_dict for rf in filters],
            'cachetime': int(result_cache_timeout * 1000),
            'compression_type': compression_type,
        }
        payload = json.dumps(query_dict, separators=(',', ':')).encode()

        response_type, payload = self.send_command(message_type='GA', payload=payload)
        return self._process_get_dataframe_response(response_type=response_type, payload=payload)

    def _get(self, key: str, default: Optional[Any]) -> Optional[Any]:
        key_bytes = key.encode()
        key_len = len(key)
        assert key_len < 65536

        response_type, payload = self.send_command(message_type='GD', payload=key_bytes)
        return self._process_get_response(response_type=response_type, payload=payload, default=default)

    def _delete(self, key: str) -> bool:
        key_bytes = key.encode()
        key_len = len(key)
        assert key_len < 65536

        response_type, payload = self.send_command(message_type='DL', payload=key_bytes)
        return self._process_delete(response_type, payload)

    def _delete_many(self, keys: List[str]) -> int:
        encoded_list = [k.encode() for k in keys if len(k) < 65536]
        assert len(encoded_list) < 65536
        payload = b'\x00'.join(encoded_list)

        response_type, payload = self.send_command(message_type='DM', payload=payload)
        return self._process_delete_many(response_type, payload)

    def _touch(self, key: str, timeout: float) -> bool:
        cache_time_bytes = struct.pack('>Q', int(timeout * 1000))

        key_bytes = key.encode()
        key_len = len(key)
        assert key_len < 65536

        response_type, payload = self.send_command(message_type='TH',
                                                   payload=cache_time_bytes + key_bytes)
        return self._process_touch_response(response_type, payload)

    def _ttl(self, key: str) -> Optional[float]:
        key_bytes = key.encode()
        key_len = len(key)
        assert key_len < 65536

        response_type, payload = self.send_command(message_type='TL', payload=key_bytes)
        return self._process_ttl_response(response_type, payload)

    def _keys(self) -> list:
        response_type, payload = self.send_command(message_type='LS', payload=bytearray())
        return self._process_keys_response(response_type, payload)
