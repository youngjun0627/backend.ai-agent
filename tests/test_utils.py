import pytest

from ai.backend.agent import utils


def test_update_nested_dict():
    o = {
        'a': 1,
        'b': 2,
    }
    utils.update_nested_dict(o, {'a': 3, 'c': 4})
    assert o == {
        'a': 3,
        'b': 2,
        'c': 4,
    }

    o = {
        'a': {
            'x': 1,
        },
        'b': 2,
    }
    with pytest.raises(AssertionError):
        utils.update_nested_dict(o, {'a': 3})

    o = {
        'a': {
            'x': 1,
        },
        'b': 2,
    }
    utils.update_nested_dict(o, {'a': {'x': 3, 'y': 4}, 'b': 5})
    assert o['a'] == {
        'x': 3,
        'y': 4,
    }
    assert o['b'] == 5

    o = {
        'a': [1, 2],
        'b': 3,
    }
    utils.update_nested_dict(o, {'a': [4, 5], 'b': 6})
    assert o['a'] == [1, 2, 4, 5]
    assert o['b'] == 6
