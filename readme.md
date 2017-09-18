
## Setup

1. launch a postgis-docker container

    docker run --name dvh2 -p 5432:5432 -e POSTGRES_PASSWORD=pass -e POSTGRES_USER=dvh2 -d mdillon/postgis

2. create a .env, set DATABASE_URI=postgres://dvh2:pass@localhost:5432/dvh2

3. create a virtualenv, install packages

    virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt

4. upgrade db
    
    alembic upgrade head

## Alembic

source venv/scripts/activate
alembic revision -m "init database"
alembic upgrade head


