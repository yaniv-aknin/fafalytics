import pandas as pd

class QueryColumns:
    """Adds hierarchical column accessors when patched into the DataFrame class.

    This helps deal with dataframes with many columns, e.g., after flattening a nested
    dictionary with pd.json_normalize().

    For example:
    >>> df = pd.DataFrame([{'a.b.c': 3, 'a.b.d': 4}])
    >>> df.qc.a
    <QueryColumns @ a: b>
    >>> df.qc.a.b
    <QueryColumns @ a.b: c, d>
    >>> df.qc.a.b.d
    0    4
    Name: a.b.d, dtype: int64
    >>>
    """
    def __init__(self, separator, df=None, path=''):
        self._separator = separator
        self._df = df
        self._path = path

    def _prefixes(self, path):
        potential = [c for c in self._df.columns if c.startswith(path)]
        chop = '' if not path else (path + '.')
        return {p.replace(chop, '').partition(self._separator)[0] for p in potential}

    def __getattribute__(self, attr):
        if attr.startswith('_') or attr == 'P':
            return super().__getattribute__(attr)
        if self._path:
            new_path = self._path + self._separator + attr
        else:
            new_path = attr
        if new_path in self._df:
            return self._df[new_path]
        prefixes = self._prefixes(new_path)
        if not prefixes:
            raise AttributeError("'%s' object has no attribute %r'" % (self._df.__class__.__name__, new_path))
        retval = self.__class__(self._separator, self._df, new_path)
        # HACK: not it doesn't matter what values we put in .__dict__ (because we've overridden __getattribute__),
        #       we only add the keys to .__dict__(), so that IDEs can autocomplete the next set of fields
        retval.__dict__.update({p: None for p in prefixes})
        return retval

    def __get__(self, obj, obj_type):
        assert isinstance(obj, pd.DataFrame)
        self._df = obj
        self.__dict__.update({p: None for p in self._prefixes(self._path)})
        return self

    def __repr__(self):
        return '<%s @ %s: %s>' % (self.__class__.__name__, self._path or '[root]', ", ".join(self._prefixes(self._path)))
        

def patch_dataframe_with_query_columns(separator='.'):
    pd.DataFrame.qc = QueryColumns(separator)
