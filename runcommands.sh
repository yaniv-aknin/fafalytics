# source this file from bin/activate of your venv
export FAFAL_LOGLEVEL=info
alias redis='redis-cli -s /tmp/.fafalytics.d/redis.sock'
function newfafa {
    local data_dir="${1:-../data-dumps/dump-01}"
    [ -d $data_dir ] || { echo missing $data_dir ; return 1 ; }
    fafalytics datastore restart
    fafalytics load $data_dir/jsons/*
    fafalytics extract --max-errors 0 $data_dir/unpacked/*
    fafalytics export --format=pickle /tmp/dirty.pickle.gz curated
    fafalytics manual clean /tmp/dirty.pickle.gz /tmp/clean.pickle.gz
}
function bigfafa {
    newfafa ../data-dumps/dump-02
}
function pqfafa {
    fafalytics manual dataframe /tmp/clean.pickle.gz
}
