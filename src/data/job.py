import dataclasses
import textwrap

from src.data.task import Task

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True)
class Job:
    job_id: int
    batch_id: int
    task: Task

    def __repr__(self) -> str:
        return textwrap.dedent(f"""
            Job [
                job_id:   {self.job_id}
                batch_id: {self.batch_id}
                Task [
                    task_id:         {self.task.task_id}
                    name:            {self.task.name}
                    cmd:             {self.task.cmd}
                    sql:             {self.task.sql!r}
                    timeout_seconds: {self.task.timeout_seconds}
                    retries:         {self.task.retries}
                ]
            ]
        """)
