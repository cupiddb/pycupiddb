import os
from pycupiddb import CupidClient


class TestClient:

    @classmethod
    def setup_class(cls):
        cupiddb_host = os.getenv('CUPIDDB_TEST_HOST', 'localhost')
        cupiddb_port = int(os.getenv('CUPIDDB_TEST_PORT', '5995'))
        cls.client = CupidClient(host=cupiddb_host, port=cupiddb_port)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_int_and_float(self):
        key = 'test number'
        # Test integer
        self.client.set(key=key, value=1, timeout=60)
        result = self.client.get(key=key)
        assert result == 1
        assert isinstance(result, int)
        result = self.client.incr(key=key)
        assert result == 2
        assert isinstance(result, int)
        result = self.client.incr(key=key, delta=2)
        assert result == 4
        assert isinstance(result, int)

        self.client.delete(key)
        result = self.client.incr(key=key)
        assert result == 1
        assert isinstance(result, int)
        self.client.delete(key)
        result = self.client.incr(key=key, delta=2)
        assert result == 2
        assert isinstance(result, int)
        result = self.client.incr(key=key, delta=-3)
        assert result == -1
        assert isinstance(result, int)

        # Test float
        self.client.set(key=key, value=1.0, timeout=60)
        result = self.client.get(key=key)
        assert result == 1.0
        assert isinstance(result, float)
        result = self.client.incr_float(key=key)
        assert result == 2.0
        assert isinstance(result, float)
        result = self.client.incr_float(key=key, delta=2.0)
        assert result == 4.0
        assert isinstance(result, float)

        self.client.delete(key)
        result = self.client.incr_float(key=key)
        assert result == 1.0
        assert isinstance(result, float)
        self.client.delete(key)
        result = self.client.incr_float(key=key, delta=2.0)
        assert result == 2.0
        assert isinstance(result, float)
        result = self.client.incr_float(key=key, delta=-3.0)
        assert result == -1.0
        assert isinstance(result, float)
