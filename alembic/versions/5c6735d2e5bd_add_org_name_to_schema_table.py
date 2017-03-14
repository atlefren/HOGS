"""add org_name to schema table

Revision ID: 5c6735d2e5bd
Revises: 6902abd84a82
Create Date: 2017-01-26 22:21:13.375000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5c6735d2e5bd'
down_revision = '6902abd84a82'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''
        ALTER TABLE adm.schemastore ADD COLUMN orig_name varchar(255);
    ''')


def downgrade():
    op.execute('''
        ALTER TABLE adm.schemastore DROP COLUMN orig_name;
    ''')
