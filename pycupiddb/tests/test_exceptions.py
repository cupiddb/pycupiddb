from datetime import date

from pycupiddb import CupidClient, RowFilter
from pycupiddb.tests.utils import create_df
from pycupiddb.exceptions import InvalidArrowData, InvalidPickleData, InvalidDataType, InvalidQuery


class TestClient:

    @classmethod
    def setup_class(cls):
        cls.client = CupidClient(host='localhost', port=5995)
        cls.test_df = create_df()

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_set_get_wrong_types(self):
        # Set non dataframe and try to get dataframe
        data_key = 'test_wrong_data_key'
        data = {
            'message': 'test',
        }
        self.client.set(key=data_key, value=data)
        get_data = self.client.get(key=data_key)
        assert get_data == data

        try:
            _ = self.client.get_dataframe(key=data_key)
            assert False
        except InvalidArrowData:
            assert True

        # Set dataframe and try to get non dataframe
        self.client.set(key=data_key, value=self.test_df)
        df = self.client.get_dataframe(key=data_key)
        assert self.test_df.equals(df)

        df = self.client.get(key=data_key)
        assert self.test_df.equals(df)

        self.client.delete(key=data_key)

    def test_invalid_query(self):
        key = 'test invalid filter'
        self.client.set(key=key, value=self.test_df, timeout=60)
        df = self.client.get_dataframe(key=key)
        assert self.test_df.equals(df)

        try:
            _ = self.client.get_dataframe(key=key, columns=['c2'], filter_operation=1, filters=[
                RowFilter(column='c1', logic='gte', value=date(2024, 1, 1), data_type='date'),
            ])
            assert False
        except InvalidQuery:
            assert True
        self.client.delete(key=key)
