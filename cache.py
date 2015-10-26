'''Concepts:

Cache interface

Cache implementation

Cache decorator
- model and decorate the source of ground truth
- chain it up
'''

def _return_x(x, *argc, **argv):
    return x

class BaseCache(object):
    '''The base cache.

    This class handles the routine work for (most of) the cache implementations
    - key function
    - serialization / deserialization of key and value
    - negative cache (set)
    - logging: cache missing rate, succeed qps and latency, failed qps and
      latency, sample rate
    - helper functions: get/set/delete one

    Other things like expiration should be handled by the lower cache impl.'''

    def __init__(self, key_fn=None, serde=None, negative_cache=None, logger=None):
        '''convensions:

        key = key_fn(key_object)
        obj = serde.deserialize(serde.serialize(obj))
        exist_keys = negative_cache.filter(keys)'''
        self._key_fn = key_fn if key_fn else _return_x
        if serde:
            self._serialize = serde.serialize
            self._deserialize = serde.deserialize
        else:
            self._serialize = _return_x
            self._deserialize = _return_x
        if negative_cache:
            self._neg_filter = negative_cache.filter
            self._neg_add = negative_cache.add
            self._neg_remove = negative_cache.remove
        else:
            self._neg_filter = _return_x
            self._neg_add = _return_x
            self._neg_remove = _return_x
        self._logger = logger

    # Do not override this method.
    def get_one(self, key, default=None):
        '''Returns the value of this given key.

        If the key does not exist, return None or default.'''
        results = self.get_many([key])
        if key in results:
            return results[key]
        return default or None

    # Do not override this method.
    def get_many(self, keys):
        '''Calls self._get_many to do the real work.

        A wrapper function who handles logging and data marshaling.'''
        cache_keys = {self._key_fn(k): k for k in keys}
        exist_keys = self._neg_filter(cache_keys.keys())
        if not exist_keys:
            return {}
        cache_mappings = self._get_many(exist_keys)
        self._neg_add([k for k in exist_keys if k not in cache_mappings])
        return {
            cache_keys[k]: self._deserialize(v)
            for k, v in cache_mappings.iteritems()}

    def _get_many(self, keys):
        '''Returns a {key: val} mapping for the given list of keys.

        If the key does not exist, this key will not show in the returned
        mapping. If none of the key exists, this function should return an
        empty {}'''
        raise NotImplementedError

    # Do not override this method.
    def set_one(self, key, val, confirm=False):
        '''Returns True if it's succeed and confirm is True, otherwise
           always returns False.'''
        ret = self.set_many({key: val}, confirm)
        return confirm and not not ret  # len(ret) == 1

    # Do not override this method.
    def set_many(self, mappings, confirm=False):
        '''Calls self._set_many to do the real work.

        A wrapper function who handles logging and data marshaling.'''
        cache_keys = {}
        cache_mappings = {}
        for k, v in mappings.iteritems():
            cache_key = self._key_fn(k)
            cache_keys[cache_key] = k
            cache_mappings[cache_key] = self._serialize(v)
        neg_suc_keys = self._neg_remove(cache_keys.keys(), confirm)
        cache_suc_keys = self._set_many(mappings, confirm)
        if not confirm:
            return {}
        return [cache_mappings[k] for k in (
            set(neg_suc_keys) & set(cache_suc_keys))]


    def _set_many(self, mappings, confirm=False):
        '''Returns a list of succeed keys if confirm is True, otherwise
           always returns an empty list.

        Please note that setting confirm to True may cause severe performance
        issue, and some cache backend may not support this anyways.'''
        raise NotImplementedError

    # Do not override this method.
    def delete_one(self, key, confirm=False):
        '''Returns True if it's succeed and confirm is True, otherwise
           always returns False.'''
        ret = self.delete_many([key], confirm)
        return confirm and not not ret  # len(ret) == 1

    # Do not override this method.
    def delete_many(self, keys, confirm=False):
        '''Calls self._delete_many to do the real work.

        A wrapper function who handles logging and data marshaling.'''
        return self._delete_many(keys, confirm)

    def _delete_many(self, keys, confirm=False):
        '''Returns a list of succeed keys if confirm is True, otherwise
           always returns an empty list.

        Please note that setting confirm to True may cause severe performance
        issue, and some cache backend may not support this anyways.'''
        raise NotImplementedError

    # Do not override this method.
    def clear(self, confirm=False):
        '''Calls self._clear to do the real work.

        A wrapper function who handles logging and data marshaling.'''
        return self._clear(confirm)

    def _clear(self, confirm=False):
        '''Returns True if it's succeed and confirm is True, otherwise
           always returns False.

        Please note that setting confirm to True may cause severe performance
        issue, and some cache backend may not support this anyways.'''
        raise NotImplementedError


class SetNegativeCache(object):

    def __init__(self):
        self._store = set()

    def filter(self, keys):
        return [k for k in keys if k not in self._store]

    def add(self, keys, confirm=False):
        self._store |= set(keys)
        return keys

    def remove(self, keys, confirm=False):
        self._store -= set(keys)
        return keys


class DictCache(BaseCache):

    def __init__(self, negative_cache=None):
        self._store = {}
        super(DictCache, self).__init__(negative_cache=negative_cache)

    def _get_many(self, keys):
        ret = {key: self._store[key] for key in keys if key in self._store}
        return ret

    def _set_many(self, mappings, confirm=False):
        self._store.update(mappings)
        return mappings.keys()

    def _delete_many(self, keys, confirm=False):
        for key in keys:
            self._store.pop(key, None)
        return keys

    def _clear(self, confirm=False):
        self._store = {}
        return True


if __name__ == "__main__":
    # import ipdb; ipdb.set_trace()
    # n_cache = SetNegativeCache()
    # cache = DictCache(negative_cache=n_cache)
    cache = DictCache()
    cache.set_one(1, 2, confirm=True)
    print cache.get_one(1)
    print cache.get_one(2)
    print cache.get_one(2)
    cache.delete_one(1, confirm=True)
    cache.delete_one(1)
    print cache.get_one(1)
    print cache.get_one(1)

