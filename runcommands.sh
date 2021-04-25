# source this file from bin/activate of your venv
export FAFAL_LOGLEVEL=info
alias redis='redis-cli -s /tmp/.fafalytics.d/redis.sock'
function newfafa {
    local data_dir=../data-dumps/dump-01
    [ -d $data_dir ] || { echo missing $data_dir ; return 1 ; }
    fafalytics datastore restart
    fafalytics load $data_dir/jsons/*
    fafalytics extract --max-errors 0 $data_dir/unpacked/*
    fafalytics export /tmp/test.parquet curated
}
function pqfafa {
    ipython -c 'import pandas as pd ; df=pd.read_parquet("/tmp/test.parquet")' -i
}
