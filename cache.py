class BaseCache(object):

    def __init__(self, key_fn=None, serde=None):
        # set up key builder function and value serialization functions
        self._key_fn = key_fn if key_fn is not None else lambda x: x
        self._serialize = serde.serialize \
            if serde is not None else lambda x: x
        self._deserialize = serde.deserialize \
            if serde is not None else lambda x: x

    # Do not override this method.
    def get_one(self, key, default=None):
        '''Returns the value of this given key.

        If the key does not exist, return None or default.'''
        results = self.get_many([key])
        if key in results:
            return results[key]
        return default or None

    def get_many(self, keys):
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
        import ipdb; ipdb.set_trace()
        return confirm and not not ret  # len(ret) == 1

    def set_many(self, mappings, confirm=False):
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

    def delete_many(self, keys, confirm=False):
        '''Returns a list of succeed keys if confirm is True, otherwise
           always returns an empty list.

        Please note that setting confirm to True may cause severe performance
        issue, and some cache backend may not support this anyways.'''
        raise NotImplementedError

    def clear(self, confirm=False):
        '''Returns True if it's succeed and confirm is True, otherwise
           always returns False.

        Please note that setting confirm to True may cause severe performance
        issue, and some cache backend may not support this anyways.'''
        raise NotImplementedError


class DictCache(BaseCache):

    def __init__(self):
        self._store = {}
        super(DictCache, self).__init__()

    def get_many(self, keys):
        ret = {key: self._store[key] for key in keys if key in self._store}
        return ret

    def set_many(self, mappings, confirm=False):
        self._store.update(mappings)
        return mappings.keys()

    def delete_many(self, keys, confirm=False):
        for key in keys:
            self._store.pop(key, None)
        return keys

    def clear(self, confirm=False):
        self._store = {}
        return True


if __name__ == "__main__":
    cache = DictCache()
    cache.set_one(1, 2, confirm=True)
    cache.delete_one(1, confirm=True)
    cache.delete_one(1)
    cache.get_one(1)

