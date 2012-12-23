# -*- coding: utf-8 -*-
from itertools import islice


def chunker(iterable, chunksize):
    """Return elements from the iterable in `chunksize`-ed lists. The last
    returned chunk may be smaller (if length of collection is not divisible
    by `chunksize`).

    >>> print list(chunker(xrange(10), 3))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    """
    i = iter(iterable)
    while True:
        wrapped_chunk = [list(islice(i, int(chunksize)))]
        if not wrapped_chunk[0]:
            break
        yield wrapped_chunk.pop()