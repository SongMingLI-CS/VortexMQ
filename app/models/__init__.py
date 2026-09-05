"""ORM 模型聚合导出，外部统一从 app.models 导入。"""

from app.models.task import TaskRecord
from app.models.tenant import Tenant

__all__ = ["Tenant", "TaskRecord"]
