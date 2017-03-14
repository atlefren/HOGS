"""add schema table

Revision ID: 6902abd84a82
Revises: 280f97998152
Create Date: 2017-01-26 18:21:52.563000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6902abd84a82'
down_revision = '280f97998152'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''
        CREATE TABLE adm.schemastore (
            id serial PRIMARY KEY,
            dataset_id int NOT NULL,
            name varchar(80),
            datatype varchar(80)
        )
    ''')


def downgrade():
    op.execute('''DROP TABLE adm.schemastore''')
