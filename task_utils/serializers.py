from abc import abstractmethod


class Serializer:
    @abstractmethod
    def serialize(self, value):
        raise NotImplemented  # pragma: nocover

    @abstractmethod
    def deserialize(self, msg):
        raise NotImplemented  # pragma: nocover


try:  # pragma: nocover
    import cPickle
    pickle = cPickle
except:  # pragma: nocover
    cPickle = None
    import pickle


class Pickle(Serializer):
    def __init__(self, protocol=pickle.DEFAULT_PROTOCOL):
        self.protocol = protocol

    def serialize(self, value):
        return pickle.dumps(value, self.protocol)

    def deserialize(self, msg):
        return pickle.loads(msg)


try:
    import msgpack
except:
    pass
else:
    class Msgpack(Serializer):
        def __init__(self, pack_kwargs=None, unpack_kwargs=None):
            self.pack_kwargs = pack_kwargs or dict(use_bin_type=True)
            self.unpack_kwargs = unpack_kwargs or dict(raw=False)

        def serialize(self, value):
            return msgpack.packb(value, **self.pack_kwargs)

        def deserialize(self, msg):
            return msgpack.unpackb(msg, **self.unpack_kwargs)
