"""init database

Revision ID: 2dc1cb05eeeb
Revises: 
Create Date: 2017-01-25 16:35:13.814000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2dc1cb05eeeb'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute('CREATE SCHEMA adm;')
    op.execute('''
        CREATE TABLE adm.datsetstore (
            dataset_id serial PRIMARY KEY,
            name varchar(255) not null,
            schema varchar(100) not null
        );
    ''')


def downgrade():
    op.execute('DROP SCHEMA adm CASCADE;')
