"""alter database

Revision ID: 280f97998152
Revises: 2dc1cb05eeeb
Create Date: 2017-01-26 16:24:57.771000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '280f97998152'
down_revision = '2dc1cb05eeeb'
branch_labels = None
depends_on = None


def upgrade():

    op.execute('''
        ALTER TABLE adm.datsetstore RENAME to datasetstore;
        ALTER TABLE adm.datasetstore ADD COLUMN tablename varchar(64);
        ALTER TABLE adm.datasetstore ADD COLUMN description text;
    ''')


def downgrade():
    op.execute('''
        ALTER TABLE adm.datasetstore RENAME to datsetstore;
        ALTER TABLE adm.datsetstore DROP COLUMN tablename;
        ALTER TABLE adm.datsetstore DROP COLUMN description;
    ''')
