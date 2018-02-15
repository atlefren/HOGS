#HOGS - Heterogenous Open Geodata Storage
A system for storing hetrogenous geodatasets in postrgeSQL using either a tables or jsonb-layout.

Works on all file types that can be read by OGR.

## Importing data

1. activate virtualenv
2. Run: ```python import.py import_conf.json```


## Other operations

- query.py: run a series of queries, timing them
- count.py: count number of files in an import config
- prepare_n50.py: creates an import config based on a directory of zipped n50 sosi files

## Setup

1. launch a postgis-docker container

    docker run --name dvh2 -p 5432:5432 -e POSTGRES_PASSWORD=pass -e POSTGRES_USER=dvh2 -d mdillon/postgis

2. create a .env, set DATABASE_URI=postgres://dvh2:pass@localhost:5432/dvh2

3. Build gdal/ogr with sosi-support

    git clone https://github.com/kartverket/fyba
    cd fyba
    autoreconf --force --install
    ./configure
    make -j 8
    sudo make install
    cd ..

    sudo apt-get install libproj-dev

    wget http://download.osgeo.org/geos/geos-3.6.2.tar.bz2
    tar xjf geos*bz2
    cd geos*
    ./configure --enable-python
    make -j 10
    sudo make install
    cd ..

    svn checkout https://svn.osgeo.org/gdal/trunk/gdal gdal
    cd gdal
    sh autogen.sh
    ./configure --with-sosi --with-python --with-static-proj --with-geos
    make -j 8
    sudo make install


OR: du whatever you like, as long as you end up with gdal/ogr with python-bindings, geos, sosi and proj4...


4. create a virtualenv, install packages

    virtualenv --system-site-packages venv
    source venv/bin/activate
    pip install -r requirements.txt

