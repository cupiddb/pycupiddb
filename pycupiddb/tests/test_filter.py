import random
import string

from pycupiddb import CupidClient, RowFilter
from pycupiddb.tests.utils import create_df


class TestClient:

    @classmethod
    def setup_class(cls):
        cls.client = CupidClient(host='localhost', port=5995)
        cls.test_df_1 = create_df()
        cls.test_df_2 = create_df()

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_cache_query(self):
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        key = 'test filters' + random_string
        keys_list = self.client.keys()
        self.client.set(key=key, value=self.test_df_1, timeout=60)
        df = self.client.get_dataframe(key=key)
        assert self.test_df_1.equals(df)

        df = self.client.get_dataframe(key=key, result_cache_timeout=1)
        assert self.test_df_1.equals(df)
        new_keys_list = self.client.keys()
        assert len(new_keys_list) == len(keys_list) + 1

        self.client.set(key=key, value=self.test_df_2, timeout=60)
        df = self.client.get_dataframe(key=key, result_cache_timeout=1)
        # Still get the same result since the filter result is cached
        assert self.test_df_1.equals(df)

        df = self.client.get_dataframe(key=key)
        assert self.test_df_2.equals(df)
        self.client.delete(key=key)
        new_keys_list = self.client.keys()
        assert len(new_keys_list) == len(keys_list)
