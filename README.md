# expiringpdict
Python Expiring Dictionary

[![Build Status](https://travis-ci.org/Pi3rral/expiringpdict.svg?branch=master)](https://travis-ci.org/Pi3rral/expiringpdict)

## Usage
```
from expiringpdict import ExpiringDict

#Create a dictionary where every key will expire after 2 seconds
expiring_dict_2sec = ExpiringDict(2)

# Use as a classic dict...
expiring_dict_2sec['key1'] = 'value1'
```

## Redis Support

If you want your dictionary stored in Redis, set hostname/port at initialization.
```
from expiringpdict import ExpiringDict

#Create a dictionary on redis instance, where every key will expire after 2 seconds
expiring_dict_2sec = ExpiringDict(2, redis_hostname="my_redis_host", redis_port=6379)
```
