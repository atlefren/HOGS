"""add geom_type to dataset table

Revision ID: bba9b0869133
Revises: 5c6735d2e5bd
Create Date: 2017-01-27 01:34:38.582000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bba9b0869133'
down_revision = '5c6735d2e5bd'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''
        ALTER TABLE adm.datasetstore ADD COLUMN geom_type varchar(50);
    ''')


def downgrade():
    op.execute('''
        ALTER TABLE adm.datasetstore DROP COLUMN geom_type;
    ''')
