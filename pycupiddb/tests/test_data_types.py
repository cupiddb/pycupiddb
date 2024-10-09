import os
import random
import string
import pandas as pd
from pycupiddb import CupidClient, RowFilter


class TestClient:

    @classmethod
    def setup_class(cls):
        cupiddb_host = os.getenv('CUPIDDB_TEST_HOST', 'localhost')
        cupiddb_port = int(os.getenv('CUPIDDB_TEST_PORT', '5995'))
        cls.client = CupidClient(host=cupiddb_host, port=cupiddb_port)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_string_dataframe(self):
        key = 'test string'
        self.client.set(key=key, value='hello', timeout=60)
        result = self.client.get(key=key)
        assert result == 'hello'
        assert isinstance(result, str)

        key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        test_df = pd.DataFrame({
            '0': [str(i) for i in range(100)],
            '1': [i for i in range(100)],
            '2': [str(i).zfill(3) for i in range(100)],
        }, index=[i for i in range(100)])
        self.client.set(key=key, value=test_df, timeout=60)
        result_df = self.client.get_dataframe(key=key, result_cache_timeout=10,
                                              compression_type='zstd')
        assert set(result_df.columns) == set(test_df.columns)
        assert test_df.equals(result_df)

        result_df = self.client.get_dataframe(key=key, filters=[
            RowFilter(column='2', logic='lt', value='042', data_type='string'),
            RowFilter(column='2', logic='gte', value='005', data_type='string'),
        ])
        python_filtered = test_df[test_df['2'] < '042']
        python_filtered = python_filtered[python_filtered['2'] >= '005']
        assert python_filtered.equals(result_df)

    def test_number_col(self):
        key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        test_df = pd.DataFrame({
            1: [bool(i % 2 == 0) for i in range(10)],
            2: [bool(i % 2 == 1) for i in range(10)],
        })
        self.client.set(key=key, value=test_df, timeout=60)
        result_df = self.client.get(key=key)
        assert set(result_df.columns) == set(test_df.columns)
        assert test_df.equals(result_df)

    def test_bool_type(self):
        key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        test_df = pd.DataFrame({
            '1': [bool(i % 2 == 0) for i in range(10)],
            '2': [bool(i % 2 == 1) for i in range(10)],
        }, index=list(range(10)))
        self.client.set(key=key, value=test_df, timeout=60)
        result_df = self.client.get_dataframe(key=key, filters=[
            RowFilter(column='1', logic='eq', value=False, data_type='bool')
        ])
        python_filtered = test_df[test_df['1'] == False]
        assert python_filtered.equals(result_df)

    def test_number_datatypes(self):
        key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        int_types = ['int64', 'Int64', 'int32', 'int16', 'Int8',
                     'uint64', 'uint32', 'uint16', 'uint8']
        float_types = ['float64', 'Float64', 'float32']
        number_type_list = int_types + float_types

        test_df = pd.DataFrame({
            col: [i for i in range(100)]
            for col in number_type_list
        }, index=[i for i in range(100)]).astype({
            col: col
            for col in number_type_list
        })
        self.client.set(key=key, value=test_df, timeout=60)

        result_df = self.client.get_dataframe(key=key, compression_type='lz4')
        assert test_df.equals(result_df)

        for int_type in int_types:
            random_filter_value = int(random.random() * 100)
            result_df = self.client.get_dataframe(key=key, filters=[
                RowFilter(column=int_type, logic='gt', value=random_filter_value, data_type='int')
            ])
            python_filtered = test_df[test_df[int_type] > random_filter_value]
            assert python_filtered.equals(result_df)

        for float_type in float_types:
            random_filter_value = random.random() * 100
            result_df = self.client.get_dataframe(key=key, filters=[
                RowFilter(column=float_type, logic='lte', value=random_filter_value, data_type='float')
            ])
            python_filtered = test_df[test_df[float_type] <= random_filter_value]
            assert python_filtered.equals(result_df)
