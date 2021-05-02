import pytest
import pandas as pd
from fafalytics.pdutils import patch_dataframe_with_query_columns
patch_dataframe_with_query_columns()


@pytest.fixture
def df():
    data = {'a': {
        'b': 1,
        'c': 3,
    }}
    return pd.json_normalize([data.copy() for x in range(3)])

def test_qc(df):
    assert 'b' in df.qc.a.__dict__
    assert 'c' in df.qc.a.__dict__
    assert df.qc.a.b.sum() == 3
    assert len(df[df.qc.a].columns) == 2
