import pytest
import time
from expiringpdict import ExpiringDict

@pytest.fixture
def ed2():
    ed2 = ExpiringDict(2)
    ed2['k1'] = 'v1'
    ed2['k2'] = 'v2'
    return ed2


class TestExpiringDict(object):

    def test_get_item(self, ed2):
        assert ed2['k1'] == 'v1'
        assert ed2['k2'] == 'v2'

    def test_get_item_expired(self, ed2):
        assert ed2['k1'] == 'v1'
        time.sleep(3)
        with pytest.raises(KeyError):
            v2 = ed2['k2']

    def test_get(self, ed2):
        k1v = ed2.get('k1')
        assert k1v == 'v1'
        k3v = ed2.get('k3')
        assert k3v is None
        time.sleep(3)
        k2v = ed2.get('k2')
        assert k2v is None

    def test_pop(self, ed2):
        assert len(ed2) == 2
        k1v = ed2.pop('k1')
        assert len(ed2) == 1
        assert k1v == 'v1'
        k1v2 = ed2.pop('k1')
        assert k1v2 is None
        assert len(ed2) == 1

    def test_refresh(self, ed2):
        assert len(ed2) == 2
        time.sleep(1)
        assert len(ed2) == 2
        ed2.refresh('k1')
        time.sleep(1)
        assert len(ed2) == 1
