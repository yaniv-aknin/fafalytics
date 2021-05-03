import testutils

import pytest

import pandas as pd
from querycolumns import patch_dataframe_with_query_columns
patch_dataframe_with_query_columns()

from fafalytics.clean import clean_dataframe

def test_clean(df):
    assert df.shape == (200, 129)
    cdf = clean_dataframe(df)
    assert cdf.shape == (182, 134)
    assert cdf.qc.meta.duration.max().seconds == 4077

@pytest.fixture
def df():
    return pd.read_parquet(testutils.testdata / 'test.parquet')
