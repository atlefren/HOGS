# -*- coding: utf-8 -*-

import os
import sys
import select
from contextlib import contextmanager


@contextmanager
def stdout_redirector(f):

    pipe_out, pipe_in = os.pipe()

    def more_data():
        try:
            r, _, _ = select.select([pipe_out], [], [], 0)
            return bool(r)
        except ValueError, e:
            return False

    def read_pipe():
        out = ''
        while more_data():
            out += os.read(pipe_out, 1024)
        return out

    # save a copy of stdout
    stdout = os.dup(2)
    # replace stdout with our write pipe
    os.dup2(pipe_in, 2)
    try:
        yield
    finally:
        os.dup2(stdout, 2)
        err = read_pipe().strip()
        if err != '':
            f.write(unicode(err))
        os.close(pipe_out)
        os.close(pipe_in)
        os.close(stdout)
