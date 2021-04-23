# source this file from bin/activate of your venv
alias redis='redis-cli -s /tmp/.fafalytics.d/redis.sock'
alias randomkey='redis-cli -s /tmp/.fafalytics.d/redis.sock get $(redis-cli -s /tmp/.fafalytics.d/redis.sock randomkey) | python3 -m json.tool'

