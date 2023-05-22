"""MapReduce framework task for a Worker node."""

from dataclasses import dataclass, field
from enum import Enum
import logging
import pathlib
from typing import List


class TaskStatus(Enum):
    """Status of a Task."""

    UNASSIGNED = 0
    RUNNING = 1
    COMPLETED = 2
    LOST = 3


class TaskType(Enum):
    """Type of a Task."""

    MAP = 'map'
    REDUCE = 'reduce'


LOGGER = logging.getLogger(__name__)


@dataclass
class Task:
    """A task for a Worker node to perform.

    This is what is assigned by the Manager to a Worker node.
    """

    type: TaskType
    task_id: int  # Unique ID
    files: List[pathlib.Path] = field(default_factory=list)  # Files to process
    status: TaskStatus = TaskStatus.UNASSIGNED  # Status of the task
    worker: "WorkerProxy" = None  # Worker node assigned to the task

    def detach(self):
        """Detach the task from its worker."""
        self.worker = None
        self.status = TaskStatus.UNASSIGNED

    def assign_to_worker(self, worker: "WorkerProxy"):
        """Assign the task to a worker."""
        self.worker = worker
        self.status = TaskStatus.RUNNING

    def get_file_paths(self) -> List[str]:
        """Get a string representation of the files to process."""
        return [str(file) for file in self.files]

    def __str__(self):
        """Return a string representation of all attributes in the task."""
        # Each attribute on one line
        return (
            f"Task Type: {self.type} \n"
            f"Task ID: {self.task_id} \n"
            f"Files: {self.files} \n"
            f"Status: {self.status} \n"
            f"Worker:\n--\n{self.worker}\n-- \n"
        )
