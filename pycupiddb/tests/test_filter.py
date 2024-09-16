import os
import random
import string
from datetime import date, datetime

from pycupiddb import CupidClient, RowFilter
from pycupiddb.tests.utils import create_df


class TestClient:

    @classmethod
    def setup_class(cls):
        cupiddb_host = os.getenv('CUPIDDB_TEST_HOST', 'localhost')
        cupiddb_port = int(os.getenv('CUPIDDB_TEST_PORT', '5995'))
        cls.client = CupidClient(host=cupiddb_host, port=cupiddb_port)
        cls.test_df_1 = create_df(rows=100, index_type='date')
        cls.test_df_1.index.name = 'date'
        cls.test_df_2 = create_df(rows=100, index_type='datetime')
        cls.test_df_2.index.name = 'datetime'

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

    def test_filter(self):
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        key_1 = 'test filters' + random_string
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        key_2 = 'test filters' + random_string
        self.client.set(key=key_1, value=self.test_df_1, timeout=60)
        self.client.set(key=key_2, value=self.test_df_2, timeout=60)

        # Test int filter
        filters = [
            RowFilter(column='c3', logic='lte', value=5, data_type='int'),
        ]
        filtered_df = self.client.get_dataframe(key=key_1, filters=filters)
        python_filtered = self.test_df_1[self.test_df_1['c3'] <= 5]
        assert filtered_df.equals(python_filtered)

        # Test float filter
        filters = [
            RowFilter(column='c0', logic='gte', value=0.5, data_type='float'),
        ]
        filtered_df = self.client.get_dataframe(key=key_1, filters=filters)
        python_filtered = self.test_df_1[self.test_df_1['c0'] >= 0.5]
        assert filtered_df.equals(python_filtered)

        # Test date filter
        filters = [
            RowFilter(column='date', logic='lt', value=date(2000, 1, 5), data_type='date'),
        ]
        filtered_df = self.client.get_dataframe(key=key_1, filters=filters)
        python_filtered = self.test_df_1[self.test_df_1.index < date(2000, 1, 5)]
        assert filtered_df.equals(python_filtered)

        # Test datetime filter
        filters = [
            RowFilter(column='datetime', logic='gt', value=datetime(2000, 1, 5), data_type='datetime'),
        ]
        filtered_df = self.client.get_dataframe(key=key_2, filters=filters)
        python_filtered = self.test_df_2[self.test_df_2.index > datetime(2000, 1, 5)]
        assert filtered_df.equals(python_filtered)

        # Test multiple filter
        filters = [
            RowFilter(column='date', logic='lt', value=date(2000, 1, 5), data_type='date'),
            RowFilter(column='c0', logic='gte', value=0.5, data_type='float'),
        ]
        filtered_df = self.client.get_dataframe(key=key_1, filters=filters, filter_operation='OR')
        python_filtered = self.test_df_1[(self.test_df_1.index < date(2000, 1, 5)) |
                                         (self.test_df_1['c0'] >= 0.5)]
        assert filtered_df.equals(python_filtered)

        # Test multiple filter
        filters = [
            RowFilter(column='c0', logic='gte', value=0.25, data_type='float'),
            RowFilter(column='c0', logic='lt', value=0.75, data_type='float'),
        ]
        filtered_df = self.client.get_dataframe(key=key_1, filters=filters, filter_operation='AND')
        python_filtered = self.test_df_1[(self.test_df_1['c0'] >= 0.25) &
                                         (self.test_df_1['c0'] < 0.75)]
        assert filtered_df.equals(python_filtered)

        self.client.delete(key=key_1)
        self.client.delete(key=key_2)

    def test_filter_columns(self):
        random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        key = 'test filters' + random_string
        self.client.set(key=key, value=self.test_df_1, timeout=60)

        # Test column filter
        filters = [
            RowFilter(column='c3', logic='lte', value=5, data_type='int'),
        ]
        filtered_df = self.client.get_dataframe(key=key, filters=filters, columns=[
            'date', 'c0', 'c2', 'c3', 'c4'
        ])
        assert set(filtered_df.columns) == set(['c0', 'c2', 'c3', 'c4'])
        python_filtered = self.test_df_1[self.test_df_1['c3'] <= 5]
        python_filtered = python_filtered[['c0', 'c2', 'c3', 'c4']]
        assert filtered_df.equals(python_filtered)

        # Test invalid column filter
        filters = [
            RowFilter(column='c0', logic='gt', value=0.5, data_type='float'),
            RowFilter(column='notexist', logic='eq', value=0.5, data_type='float'),
        ]
        filtered_df = self.client.get_dataframe(key=key, filters=filters, columns=[
            'date', 'c0', 'c2', 'c3', 'c4', 'dummy'
        ])
        assert set(filtered_df.columns) == set(['c0', 'c2', 'c3', 'c4'])
        python_filtered = self.test_df_1[self.test_df_1['c0'] > 0.5]
        python_filtered = python_filtered[['c0', 'c2', 'c3', 'c4']]
        assert filtered_df.equals(python_filtered)

        self.client.delete(key=key)
