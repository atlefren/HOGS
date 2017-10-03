# -*- coding: utf-8 -*-
import datetime
import uuid
import os
from dotenv import load_dotenv, find_dotenv

from importer.postgis import JsonbDb
from importer.File import File

load_dotenv(find_dotenv())

db = JsonbDb(os.environ.get('DATABASE_URI', None))

'''
def load_files(schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        print 'No files specified'
        return
    for file in files:
        file = OgrFile(file)
        fields = file.fields()
        # print 'Write %s features to db' % file.num_features
        db.write_features(schema, dataset_id, version, fields, file.records())
    db.create_dataset_view(schema, dataset_id, name, version, fields)
'''


def loop_files(files):
    for file in files:
        file = File(file)
        filename = os.path.basename(file.filename)
        for record in file.records():
            record['filename'] = filename
            yield record


def load_files_single(schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        print 'No files specified'
        return
    file = File(files[0])

    print 'Write %s files to db' % len(files)
    fields = file.fields()
    '''
    for r in loop_files(files):
        print r
    '''
    db.write_features(schema, dataset_id, version, fields, loop_files(files))
    db.create_dataset_view(schema, dataset_id, name, version, fields)


def import_dataset(schema, name, files, dataset_id=None, append=False):
    if not db.check_adm_exists():
        print 'Create adm schema'
        db.create_adm_table()

    if not db.check_schema_exists(schema):
        print 'Create schema %s' % schema
        db.create_schema(schema)

    version = 1
    created = datetime.datetime.now()
    if dataset_id is None or not db.dataset_exists(schema, dataset_id):
        if dataset_id is None:
            dataset_id = str(uuid.uuid4())
        print 'Create dataset %s.%s' % (schema, dataset_id)
        version = 1
    elif append:
        version = db.get_dataset_version(schema, dataset_id)
        print 'Append to dataset %s.%s version %s' % (schema, dataset_id, version)
    else:
        version = db.get_dataset_version(schema, dataset_id) + 1
        print 'Update dataset %s.%s to version %s' % (schema, dataset_id, version)

    db.create_dataset(schema, dataset_id, name, version, created)
    load_files_single(schema, dataset_id, version, files, name)
    db.mark_dataset_imported(schema, dataset_id, version)
