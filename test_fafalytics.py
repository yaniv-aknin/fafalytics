from fafalytics.pyutils import negate, Query, restructure_dict, Literal
from unittest import TestCase

def test_negate():
    true = lambda: True
    assert negate(true)() is False

def test_query():
    def missing(obj, component):
        assert component == 'bar'
        return 5
    assert Query('foo')({'foo': 1}) == 1
    assert Query('foo/bar')({'foo': {'bar': 1}}) == 1
    assert Query('foo/bar', missing=missing)({'foo': {}}) == 5
    assert Query(Literal(None))({}) is None
    with TestCase().assertRaises(RuntimeError):
        Query('foo', reraise=RuntimeError)({})

def test_restructure_dict():
    src = {'foo': 'bar', 'qux': {'baz': 5}}
    queries = {
        'shlaq': Query('foo', cast=lambda s: s[0]),
        'shliq/shlaq': Query('qux/baz'),
        'shloq': Query('qux/quux', missing=lambda x,y: 7),
    }
    result = restructure_dict(src, queries)
    expected = {'shlaq': 'b', 'shliq': {'shlaq': 5}, 'shloq': 7}
    TestCase().assertDictEqual(expected, result)
