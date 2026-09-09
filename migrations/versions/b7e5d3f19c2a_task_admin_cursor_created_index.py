"""admin task hall cursor pagination index

Revision ID: b7e5d3f19c2a
Revises: 3a9f1c2b7e4d
Create Date: 2026-09-08 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7e5d3f19c2a"
down_revision: Union[str, Sequence[str], None] = "3a9f1c2b7e4d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """支撑任务大厅无筛选时的 ORDER BY created_at DESC, task_id ASC 索引扫描。

    显式列排序与排序子句一致，keyset 游标翻页在默认视图下每页只扫
    page_size 行，不再对整表排序或 OFFSET 深翻页。
    """
    op.create_index(
        "ix_task_records_created_at_id",
        "task_records",
        [sa.text("created_at DESC"), sa.text("task_id ASC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_task_records_created_at_id", table_name="task_records")
