# cython: language_level=3
# cython: freethreading_compatible=True

"""Cython equivalents of the benchmark-only Python reference kernels."""


def filter_by_type(events, event_type):
    cdef list matched = []
    cdef object event
    for event in events:
        if isinstance(event, event_type):
            matched.append(event)
    return tuple(matched)


def merge_subscribers(dict unkeyed, dict keyed):
    """Merge stable, integer-keyed maps while the owning bus lock is held."""
    cdef object left_iterator
    cdef object right_iterator
    cdef object left_key
    cdef object right_key
    cdef object value
    cdef list merged = []

    if not unkeyed:
        return tuple(keyed.values())
    if not keyed:
        return tuple(unkeyed.values())

    left_iterator = iter(unkeyed)
    right_iterator = iter(keyed)
    left_key = next(left_iterator, None)
    right_key = next(right_iterator, None)

    while left_key is not None and right_key is not None:
        if left_key <= right_key:
            merged.append(unkeyed[left_key])
            left_key = next(left_iterator, None)
        else:
            merged.append(keyed[right_key])
            right_key = next(right_iterator, None)

    while left_key is not None:
        merged.append(unkeyed[left_key])
        left_key = next(left_iterator, None)
    while right_key is not None:
        merged.append(keyed[right_key])
        right_key = next(right_iterator, None)
    return tuple(merged)
