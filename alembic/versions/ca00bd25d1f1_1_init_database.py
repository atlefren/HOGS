"""1. init database

Revision ID: ca00bd25d1f1
Revises: 
Create Date: 2017-09-18 16:44:10.753982

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ca00bd25d1f1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute('CREATE SCHEMA adm;')
    op.execute('''
        CREATE TABLE adm.datasetstore (
            dataset_id varchar(255) not null PRIMARY KEY,
            name varchar(255) not null,
            schema varchar(100) not null
        );
    ''')


def downgrade():
    op.execute('DROP SCHEMA adm CASCADE;')
