import arrow
import logging
import pickle
import redis
import random
from collections import MutableMapping
from datetime import timedelta

CONST_GC_MAX = 100
CONST_GC_PROB = 50


class ExpiringDictStore(MutableMapping):
    """ Store for ExpiringDict.
    Implement MutableMapping interface to act as a collection.
    """

    def __init__(self, max_age):
        self._max_age = max_age

    def __getitem__(self, key):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def _gc(self, force=False):
        pass

    def set_max_age(self, max_age):
        self._max_age = max_age

    def refresh(self, key):
        raise NotImplementedError()


class ExpiringDictStoreDict(ExpiringDictStore):
    """ dict store for ExpiringDict.
    Store datas in a python dict as a tuple, containing storage time.
    """

    def __init__(self, max_age):
        logging.debug('Creating Dict ExpiringDict')
        self._max_age = max_age
        self._real_storage = dict()

    def __getitem__(self, key):
        tuple_value = self._real_storage.__getitem__(key)
        value_age = arrow.now() - tuple_value[1]
        if value_age.seconds < self._max_age:
            return tuple_value[0]
        else:
            self._real_storage.__delitem__(key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        self._gc()
        self._real_storage.__setitem__(key, (value, arrow.now()))

    def __delitem__(self, key):
        self._real_storage.__delitem__(key)

    def __iter__(self):
        return self._real_storage.__iter__()

    def __len__(self):
        return self._real_storage.__len__()

    def refresh(self, key):
        try:
            tuple_value = self.__getitem__(key)
            self.__setitem__(key, tuple_value[0])
        except KeyError:
            pass

    def _gc(self, force=False):
        """ Remove expired key from the dict.
        Use a probability for Garbage Collector to run or not.
        """
        probability = random.randint(0, CONST_GC_MAX)
        if probability > CONST_GC_PROB and not force:
            return
        logging.debug('ExpiringDict Garbage Collector')
        max_date = (arrow.now() - timedelta(seconds=self._max_age))
        keys_to_remove = [k for k, v in self._real_storage.items()
                          if v[1] < max_date]
        logging.debug('Number of keys to remove: %d' % len(keys_to_remove))
        for k in keys_to_remove:
            self._real_storage.__delitem__(k)


class ExpiringDictStoreRedis(ExpiringDictStore):
    """ redis store for ExpiringDict.
    Store datas as key on a redis server.
    """
    def __init__(self, max_age, redis_hostname=None, redis_port=6379,
                 key_prefix=None, redis_instance=None, redis_url=None):
        self._max_age = max_age
        self._key_prefix = key_prefix if key_prefix else id(self)
        logging.debug('Creating Redis ExpiringDict with prefix: {}'.format(
            self._key_prefix))
        if redis_url:
            self._redis = redis.StrictRedis.from_url(redis_url)
        else:
            self._redis = redis_instance if redis_instance \
                else redis.StrictRedis(host=redis_hostname, port=redis_port)

    def _get_real_key(self, key):
        return str(self._key_prefix) + str(key)

    def __setitem__(self, key, value):
        logging.debug('Setting Redis Key: %s' % self._get_real_key(key))
        self._redis.set(self._get_real_key(key), pickle.dumps(value),
                        ex=self._max_age)

    def __getitem__(self, key):
        logging.debug('Getting Redis Key: %s' % self._get_real_key(key))
        try:
            return pickle.loads(self._redis.get(self._get_real_key(key)))
        except Exception:
            raise KeyError(key)

    def __delitem__(self, key):
        self._redis.delete(self._get_real_key(key))

    def __iter__(self):
        prefix_len = len(self._key_prefix)
        for i in self._redis.keys("^%s.*" % self._key_prefix):
            real_key = i[prefix_len:]
            yield real_key

    def __len__(self):
        return len(self._redis.keys("^%s.*" % self._key_prefix))

    def refresh(self, key):
        try:
            value = self.__getitem__(key)
            self.__setitem__(key, value)
        except KeyError:
            pass


class ExpiringDict(object):
    """ ExpiringDict class.
    Act as a python dict but with expiration for each data stored.
    Can store data in a redis server if available. Otherwise, uses a python dict
    """

    def __init__(self, max_age, redis_hostname=None, redis_port=6379,
                 redis_key_prefix=None, redis_url=None):
        if redis_hostname and redis_port:
            self._store = ExpiringDictStoreRedis(
                max_age, redis_hostname=redis_hostname, redis_port=redis_port,
                key_prefix=redis_key_prefix, redis_url=redis_url
            )
        else:
            self._store = ExpiringDictStoreDict(max_age)

    def __getitem__(self, key):
        return self._store.__getitem__(key)

    def __setitem__(self, key, value):
        self._store.__setitem__(key, value)

    def __delitem__(self, key):
        self._store.__delitem__(key)

    def __iter__(self):
        self._store._gc(force=True)
        return self._store.__iter__()

    def __len__(self):
        self._store._gc(force=True)
        return self._store.__len__()

    def set_max_age(self, max_age):
        self._store.set_max_age(max_age)

    def get(self, key, default=None):
        """ Get item from the dict
        Return default value if key does not exist.
        """
        try:
            return self._store.__getitem__(key)
        except KeyError:
            pass
        return default

    def refresh(self, key):
        """ Reset TTL to max_age for given key
        """
        self._store.refresh(key)

    def pop(self, key, default=None):
        """ Get item and remove it.
        Return default if expired or does not exist.
        """
        try:
            item = self._store.__getitem__(key)
            self._store.__delitem__(key)
            return item
        except KeyError:
            return default
