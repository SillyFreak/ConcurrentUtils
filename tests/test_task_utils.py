import pytest

import task_utils


def test_task_utils():
    assert task_utils.good_foo() == 0

    with pytest.raises(ValueError):
        task_utils.bad_foo()
