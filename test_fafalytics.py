from fafalytics.pyutils import negate

def test_negate():
    true = lambda: True
    assert negate(true)() is False
