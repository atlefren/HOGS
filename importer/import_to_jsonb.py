# -*- coding: utf-8 -*-
import datetime
import uuid
import os
import logging

from importer.postgis import JsonbDb
from importer.FileMulti import File


def loop_single(file):
    filename = os.path.basename(file.filename)
    for record in file.records():
        record['filename'] = filename
        yield record


def load_files(db, schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        logging.warn('[%s] No files specified' % (dataset_id))
        return
    for file in files:
        print file
        file = File(file)
        fields = file.fields()
        db.write_features(schema, dataset_id, version, fields, loop_single(file))
    return fields


def loop_files(files):
    for file in files:
        file = File(file)
        filename = os.path.basename(file.filename)
        for record in file.records():
            record['filename'] = filename
            yield record


def load_files_single(db, schema, dataset_id, version, files, name):
    if files is None or len(files) == 0:
        logging.warn('[%s] No files specified' % (dataset_id))
        return
    file = File(files[0])
    logging.info('[%s] Write %s files to db' % (dataset_id, len(files)))
    fields = file.fields()
    #print 'write!'
    num_records = db.write_features(schema, dataset_id, version, fields, loop_files(files))
    #print 'written'
    logging.info('[%s] Number of rows copied : %s' % (dataset_id, num_records))
    return fields


def import_dataset(schema, name, files, dataset_id=None, append=False, database=None):
    db = JsonbDb(database)

    version = 1
    created = datetime.datetime.now()
    if dataset_id is None or not db.dataset_exists(schema, dataset_id):
        if dataset_id is None:
            dataset_id = str(uuid.uuid4())
        logging.info('[%s] Create dataset %s.%s' % (dataset_id, schema, dataset_id))
        version = 1
    elif append:
        version = db.get_dataset_version(schema, dataset_id)
        logging.info('[%s] Append to dataset %s.%s version %s' % (dataset_id, schema, dataset_id, version))
    else:
        version = db.get_dataset_version(schema, dataset_id) + 1
        logging.info('[%s] Update dataset %s.%s to version %s' % (dataset_id, schema, dataset_id, version))

    db.create_dataset(schema, dataset_id, name, version, created)
    start = datetime.datetime.now()
    fields = load_files_single(db, schema, dataset_id, version, files, name)
    elapsed = datetime.datetime.now()
    logging.info('[%s] Time spent on copy    : %s' % (dataset_id, elapsed - start))
    db.create_dataset_view(schema, dataset_id, name, version, fields)
    db.mark_dataset_imported(schema, dataset_id, version)
