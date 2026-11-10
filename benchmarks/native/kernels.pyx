# cython: language_level=3
# cython: freethreading_compatible=True

"""Cython equivalents of the benchmark-only Python reference kernels."""


def filter_by_type(tuple events, object event_type, object limit=None):
    cdef list matched = []
    cdef object event
    cdef Py_ssize_t index

    if limit is None:
        for event in events:
            if isinstance(event, event_type):
                matched.append(event)
        return tuple(matched)
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("history query limit must be a positive integer")

    for index in range(len(events) - 1, -1, -1):
        event = events[index]
        if isinstance(event, event_type):
            matched.append(event)
            if len(matched) == limit:
                break
    matched.reverse()
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


def merge_subscribers_items(dict unkeyed, dict keyed):
    """Merge item iterators without looking up each yielded key a second time."""
    cdef object left_iterator
    cdef object right_iterator
    cdef object left_item
    cdef object right_item
    cdef list merged = []

    if not unkeyed:
        return tuple(keyed.values())
    if not keyed:
        return tuple(unkeyed.values())

    left_iterator = iter(unkeyed.items())
    right_iterator = iter(keyed.items())
    left_item = next(left_iterator, None)
    right_item = next(right_iterator, None)
    while left_item is not None and right_item is not None:
        if left_item[0] <= right_item[0]:
            merged.append(left_item[1])
            left_item = next(left_iterator, None)
        else:
            merged.append(right_item[1])
            right_item = next(right_iterator, None)

    if left_item is not None:
        merged.append(left_item[1])
        merged.extend(value for _identifier, value in left_iterator)
    if right_item is not None:
        merged.append(right_item[1])
        merged.extend(value for _identifier, value in right_iterator)
    return tuple(merged)
