"""admin task hall index

Revision ID: 3a9f1c2b7e4d
Revises: 1c6021d6984b
Create Date: 2026-09-08 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3a9f1c2b7e4d"
down_revision: Union[str, Sequence[str], None] = "1c6021d6984b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Admin 任务大厅按 status 过滤 + created_at 倒序排序时避免全表扫描。"""
    op.create_index(
        "ix_task_records_status_created_at",
        "task_records",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_task_records_status_created_at", table_name="task_records")
