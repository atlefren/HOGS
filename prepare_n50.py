# -*- coding: utf-8 -*-
import os
import zipfile
import re
import json
from shutil import copyfile
from collections import defaultdict


def unzip(dir_name):
    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith('.zip'):
            file_name = os.path.join(dir_name, item)  # get full path of files
            zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
            for file in zip_ref.namelist():
                if file.endswith('.sos') or file.endswith('.SOS'):
                    zip_ref.extract(file, dir_name)
            zip_ref.close()  # close file


def get_files(dir_name):
    regex = r"\d*_N50_(\w*).sos|SOS"
    p = re.compile(regex)
    types = defaultdict(list)
    for item in os.listdir(dir_name):  # loop through items in dir
        if item.endswith('.sos') or item.endswith('.SOS'):
            objtype = p.search(item).group(1).lower()
            types[objtype].append(os.path.join(dir_name, item))
    return types


def copy_files(files, dir):
    for file in files:
        copyfile(file, os.path.join(dir, os.path.basename(file)))


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == '__main__':

    in_folder = '/mnt/d/code/kartverksdata/dl/160792'
    out_folder = '/mnt/d/data/n50'

#    unzip(in_folder)
    ensure_dir(out_folder)

    datasets = []
    for dataset, files in get_files(in_folder).iteritems():
        dest = os.path.join(out_folder, dataset)
#        shutil.rmtree(dest)
        ensure_dir(dest)
#        copy_files(files, dest)
        datasets.append({
            'dataset_name': dataset,
            'dataset_id': dataset,
            'driver': 'SOSI',
            'schema': 'n50',
            'new_version': True,
            'files': [
                os.path.join(dest, f) for f in os.listdir(dest)
                if os.path.isfile(os.path.join(dest, f))
            ]
        })

    config = {
        "database": "postgres://dvh2:pass@localhost:5432/dvh2",
        "data_layout": "jsonb",
        "threads": len(datasets) if len(datasets) < 7 else 7,
        "datasets": datasets
    }
    with open('n50_import.json', 'w') as out:
        out.write(json.dumps(config, indent=4))
