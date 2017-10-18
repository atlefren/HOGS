# -*- coding: utf-8 -*-
import logging
import datetime


def prepare_database_for_dataset(db, dataset):
    append = not dataset.get('new_version', True)

    schema = dataset['schema']
    name = dataset['dataset_name'],
    dataset_id = dataset.get('dataset_id', None)

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
    dataset['version'] = version
    dataset['schema'] = schema
    dataset['num_handled'] = 0
    return dataset
