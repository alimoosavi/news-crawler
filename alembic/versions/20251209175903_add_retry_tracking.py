"""Add tried_count and last_tried_at to news_links

Revision ID: add_retry_tracking
Revises: 9df46746b17c
Create Date: 2024-12-09 14:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_retry_tracking'
down_revision = '9df46746b17c'  # Update this to your latest migration ID
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add retry tracking fields to news_links table:
    - tried_count: Integer field to track number of attempts
    - last_tried_at: Timestamp field to track last attempt time
    - Add 'failed' status to StatusEnum
    """
    # Add 'failed' value to news_link_status enum
    op.execute("ALTER TYPE news_link_status ADD VALUE IF NOT EXISTS 'failed'")
    
    # Add tried_count column with default value of 0
    op.add_column(
        'news_links',
        sa.Column(
            'tried_count', 
            sa.Integer(), 
            nullable=False, 
            server_default='0',
            comment='Number of times the link was attempted to be crawled'
        )
    )
    
    # Add last_tried_at column (nullable)
    op.add_column(
        'news_links',
        sa.Column(
            'last_tried_at', 
            sa.DateTime(timezone=True), 
            nullable=True,
            comment='Last time the link was attempted'
        )
    )
    
    # Create index on tried_count for faster queries filtering by retry count
    op.create_index(
        'ix_news_links_tried_count', 
        'news_links', 
        ['tried_count']
    )
    
    # Create index on status and tried_count for common queries
    op.create_index(
        'ix_news_links_status_tried_count',
        'news_links',
        ['status', 'tried_count']
    )


def downgrade() -> None:
    """
    Remove retry tracking fields from news_links table.
    
    Note: Cannot remove 'failed' value from enum in PostgreSQL easily,
    so we leave it in place.
    """
    # Drop indexes
    op.drop_index('ix_news_links_status_tried_count', table_name='news_links')
    op.drop_index('ix_news_links_tried_count', table_name='news_links')
    
    # Drop columns
    op.drop_column('news_links', 'last_tried_at')
    op.drop_column('news_links', 'tried_count')
    
    # Note: We don't remove 'failed' from enum because PostgreSQL doesn't 
    # support removing enum values directly. If you need to remove it,
    # you would need to recreate the enum type.