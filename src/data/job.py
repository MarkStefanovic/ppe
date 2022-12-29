import dataclasses
import textwrap

from src.data.task import Task

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Job:
    job_id: int
    batch_id: int
    task: Task

    def __repr__(self) -> str:
        return textwrap.dedent(f"""
            Job [
                job_id:   {self.job_id}
                batch_id: {self.batch_id}
                task:     {self.task!r}
            ]
        """).strip()
