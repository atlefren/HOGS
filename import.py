# -*- coding: utf-8 -*-
import sys
import json
# from importer import do_import_paralell

from importer import do_import


def group_by_interval():
    pass


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as infile:
        conf = json.loads(infile.read())
        do_import(conf)
