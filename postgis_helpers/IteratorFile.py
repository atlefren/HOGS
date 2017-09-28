# -*- coding: utf-8 -*-

import io
import sys
import json

from helpers import escape


class IteratorFile(io.TextIOBase):
    """
        Use this class to support writing geometries to PostGIS using COPY
        based on https://gist.github.com/jsheedy/ed81cdf18190183b3b7d
    """

    def __init__(self, dataset_id, version, records):
        self._records = records
        self._dataset_id = dataset_id
        self._version = version
        self._f = io.StringIO()
        self._count = 0

    def read(self, length=sys.maxsize):

        try:
            while self._f.tell() < length:
                self._f.write(self._get_next() + '\n')

        except StopIteration as e:
            # soak up StopIteration. this block is not necessary because
            # of finally, but just to be explicit
            pass

        except Exception as e:
            print('uncaught exception: {}'.format(e))

        finally:
            self._f.seek(0)
            data = self._f.read(length)

            # save the remainder for next read
            remainder = self._f.read()
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(remainder)
            return data

    def readline(self):
        return self._get_next()

    def _get_next(self):

        n = next(self._records)
        geom = '\N'
        if n['geom'] is not None:
            geom = n['geom']
        r = '%s\t%s\t%s\t%s' % (
            self._dataset_id,
            self._version,
            geom,
            escape(json.dumps(n['properties'], ensure_ascii=False))
        )

        self._count += 1
        return unicode(r)
