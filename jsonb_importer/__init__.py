# -*- coding: utf-8 -*-
import datetime
import uuid


from db import Db
from ogr_helpers import OgrFile

db = Db()


def load_files(schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        print 'No files specified'
        return
    for file in files:
        fields = file.fields()
        file = OgrFile(file)
        print 'Write %s features to db' % file.num_features
        db.write_features(schema, dataset_id, version, fields, file.records())
        file.destroy()
    db.create_dataset_view(schema, dataset_id, name, version, fields)


def loop_files(files):
    for file in files:
        file = OgrFile(file)
        for record in file.records():
            yield record
        file.destroy()


def load_files_single(schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        print 'No files specified'
        return
    file = OgrFile(files[0])
    print 'Write %s files to db' % len(files)
    fields = file.fields()
    db.write_features(schema, dataset_id, version, fields, loop_files(files))
    db.create_dataset_view(schema, dataset_id, name, version, fields)


def import_dataset(schema, name, files, dataset_id=None, append=False):

    if not db.check_schema_exists(schema):
        print 'create schema %s' % schema
        db.create_schema(schema)

    version = 1
    if dataset_id is None or not db.dataset_exists(schema, dataset_id):
        if dataset_id is None:
            dataset_id = str(uuid.uuid4())
        print 'create dataset %s.%s' % (schema, dataset_id)
        # TODO: Create indicies
        # TODO: Create views
        db.create_dataset(schema, dataset_id, name, 1, datetime.datetime.now())
    elif append:
        version = db.get_dataset_version(schema, dataset_id)
        print 'Append to dataset %s.%s version %s' % (schema, dataset_id, version)
    else:
        version = db.get_dataset_version(schema, dataset_id) + 1
        print 'Update dataset %s.%s to version %s' % (schema, dataset_id, version)
        db.create_dataset(schema, dataset_id, name, version, datetime.datetime.now())
    
    load_files_single(schema, dataset_id, version, files, name)
