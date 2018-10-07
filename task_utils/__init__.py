def good_foo():
    """\
    >>> good_foo()
    0
    """
    return 0

def bad_foo():
    """\
    >>> bad_foo()
    Traceback (most recent call last):
      ...
    ValueError
    """
    raise ValueError
