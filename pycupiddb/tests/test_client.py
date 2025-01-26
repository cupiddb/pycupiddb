import os
import time

from pycupiddb import CupidClient
from pycupiddb.tests.utils import create_df


class TestClient:

    @classmethod
    def setup_class(cls):
        cupiddb_host = os.getenv('CUPIDDB_TEST_HOST', 'localhost')
        cupiddb_port = int(os.getenv('CUPIDDB_TEST_PORT', '5995'))
        cls.client = CupidClient(host=cupiddb_host, port=cupiddb_port)
        cls.test_df = create_df()

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_set_get_delete(self):
        df_key = 'test_df_key'
        data_key = 'test_data_key'
        self.client.delete(key=df_key)
        df = self.client.get_dataframe(key=df_key)
        assert df is None

        self.client.set(key=df_key, value=self.test_df)
        df = self.client.get_dataframe(key=df_key)
        assert self.test_df.equals(df)

        delete_success = self.client.delete(key=df_key)
        df = self.client.get_dataframe(key=df_key)
        assert df is None
        assert delete_success

        value = self.client.get(key=data_key)
        assert value is None

        data = {
            'message': 'test',
        }
        self.client.set(key=data_key, value=data)
        get_data = self.client.get(key=data_key)
        assert get_data == data

        delete_success = self.client.delete(key=data_key)
        get_data = self.client.get(key=data_key)
        assert get_data is None
        assert delete_success

        get_data = self.client.get(key=data_key, default='default')
        assert get_data == 'default'

        get_data = self.client.get(key=data_key, default=dict())
        assert get_data == dict()

    def test_haskey_add_get(self):
        key = 'test_add_get_key'
        self.client.delete(key=key)
        has_key = self.client.has_key(key=key)
        assert not has_key

        success_add = self.client.add(key=key, value='1')
        assert success_add
        has_key = self.client.has_key(key=key)
        assert has_key

        result = self.client.get(key=key)
        assert result == '1'

        success_add = self.client.add(key=key, value='2')
        assert not success_add
        result = self.client.get(key=key)
        assert result == '1'
        self.client.set(key=key, value='2')
        result = self.client.get(key=key)
        assert result == '2'

        delete_success = self.client.delete(key=key)
        assert delete_success
        delete_success = self.client.delete(key=key)
        assert not delete_success

    def test_set_with_timeout(self):
        key = 'test_timeout_key'
        self.client.set(key=key, value=self.test_df, timeout=0.5)
        df = self.client.get_dataframe(key=key)
        assert self.test_df.equals(df)

        time.sleep(1.0)
        df = self.client.get_dataframe(key=key)
        ttl_seconds = self.client.ttl(key=key)
        assert df is None
        assert ttl_seconds is None

    def test_touch(self):
        key = 'test_touch_key'
        self.client.set(key=key, value=self.test_df, timeout=0.5)
        df = self.client.get_dataframe(key=key)
        assert self.test_df.equals(df)

        self.client.touch(key=key, timeout=10.0)

        time.sleep(1.0)
        df = self.client.get_dataframe(key=key)
        assert self.test_df.equals(df)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds <= 9 and ttl_seconds >= 8

        self.client.touch(key=key, timeout=0)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds == 0
        df = self.client.get_dataframe(key=key)
        assert self.test_df.equals(df)

    def test_ttl(self):
        key = 'test_ttl_key'
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds is None

        self.client.set(key=key, value=self.test_df, timeout=10.0)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds <= 10.0 and ttl_seconds > 9.0

        self.client.set(key=key, value=self.test_df)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds == 0.0

        self.client.set(key=key, value=self.test_df, timeout=10.0)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds <= 10.0 and ttl_seconds > 9.0
        self.client.delete(key=key)
        ttl_seconds = self.client.ttl(key=key)
        assert ttl_seconds is None

    def test_keys(self):
        key = 'additional_key'
        self.client.delete(key=key)
        keys_list = self.client.keys()
        assert isinstance(keys_list, list)
        assert '' not in keys_list

        self.client.set(key=key, value=self.test_df)
        new_keys_list = self.client.keys()

        assert (set(new_keys_list) - set(keys_list)) == set([key])

        glob_keys_list = self.client.keys(pattern='additional_*')
        assert set(glob_keys_list) == {'additional_key'}
        glob_keys_list = self.client.keys(pattern='additi*key')
        assert set(glob_keys_list) == {'additional_key'}
        glob_keys_list = self.client.keys(pattern='additiona{k,l}_key')
        assert set(glob_keys_list) == {'additional_key'}
        glob_keys_list = self.client.keys(pattern='additional__*')
        assert len(glob_keys_list) == 0

    def test_delete_many(self):
        key_prefix = 'many_keys'
        count = self.client.delete_many([f'{key_prefix}_{i}' for i in range(5)])
        assert count == 0

        for i in range(5):
            self.client.set(key=f'{key_prefix}_{i}', value=i)

        for i in range(5):
            val = self.client.get(key=f'{key_prefix}_{i}')
            assert val == i

        count = self.client.delete_many([f'{key_prefix}_{i}' for i in range(2)])
        assert count == 2

        count = self.client.delete_many([f'{key_prefix}_{i}' for i in range(5)])
        assert count == 3

        count = self.client.delete_many([f'{key_prefix}_{i}' for i in range(5)])
        assert count == 0

        for i in range(5):
            val = self.client.get(key=f'{key_prefix}_{i}')
            assert val is None

    def test_flush(self):
        for i in range(5):
            self.client.set(key=f'flush_{i}', value=i)

        key_list = self.client.keys()
        assert len(key_list) > 0

        self.client.flush()
        key_list = self.client.keys()
        assert len(key_list) == 0
