import pytest
from dataclasses import dataclass

from concurrent_utils.serializers import Pickle, Msgpack


@dataclass
class Dataclass: pass


def test_pickle():
    serializer = Pickle()

    for value in ['foo', 1, [0.1], {'baz': True}, (), {'bar'}, Dataclass()]:
        assert serializer.deserialize(serializer.serialize(value)) == value


def test_msgpack():
    serializer = Msgpack()

    for value in ['foo', 1, [0.1], {'baz': True}]:
        assert serializer.deserialize(serializer.serialize(value)) == value

    # tuples are serialized as lists
    for value in [()]:
        assert serializer.deserialize(serializer.serialize(value)) != value

    # sets & custom classes are not supported
    for value in [{'bar'}, Dataclass()]:
        with pytest.raises(TypeError):
            serializer.serialize(value)
