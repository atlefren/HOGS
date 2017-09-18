# -*- coding: utf-8 -*-
from db import Db

db = Db()


def import_dataset(schema, name, files, dataset_id=None):
    if not db.check_schema_exists(schema):
        print 'create'
        db.create_schema(schema)


if __name__ == '__main__':
    import_dataset('dok', 'test', [])
