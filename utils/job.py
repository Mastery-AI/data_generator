"""Job that is assigned to a Manager in the MapReduce framework."""

from dataclasses import dataclass
import logging
import pathlib
from typing import Dict, Tuple

from mapreduce.utils.task import Task, TaskStatus, TaskType

LOGGER = logging.getLogger(__name__)


@dataclass
class Job:
    """A Job for the Manager to execute."""

    # Job configuration
    job_id: int  # Unique ID
    mapper_path: pathlib.Path  # Path to mapper
    reducer_path: pathlib.Path  # Path to reducer
    # For py1int, this needs to be one vatibale
    counts: Tuple[int, int]  # (mapper_count, reducer_count)
    input_dir: pathlib.Path  # Path to input directory

    # Job status
    output_dir: pathlib.Path = None  # Path to output directory
    tasks: Dict[int, Task] = None  # Tasks for the job

    def __str__(self):
        """Return a string representation of all attributes in the job."""
        return (f"Job("
                f"job_id={self.job_id}\n"
                f"mapper_path={self.mapper_path}\n"
                f"reducer_path={self.reducer_path}\n"
                f"mapper_count={self.counts[0]}\n"
                f"reducer_count={self.counts[1]}\n"
                f"input_dir={self.input_dir}\n"
                f"output_dir={self.output_dir}\n"
                f"tasks={self.tasks}\n"
                )

    def reset(self):
        """Reset the job."""
        self.tasks.clear()
        # self.output_dir = None
        LOGGER.debug("Job reset")

    def get_mapper_count(self) -> int:
        """Get the number of mappers for the job."""
        return self.counts[0]

    def get_reducer_count(self) -> int:
        """Get the number of reducers for the job."""
        return self.counts[1]

    def add_map_tasks(self):
        """Add the map tasks to the job."""
        # Create the map tasks
        if self.tasks is None:
            self.tasks = {}
        else:
            self.tasks.clear()
        for i in range(self.get_mapper_count()):
            self.tasks[i] = Task(TaskType.MAP, i)
        # Loop over the input files and assign
        for idx, file in enumerate(sorted(self.input_dir.iterdir())):
            task: Task = self.tasks[idx % self.get_mapper_count()]
            task.files.append(file)
        # Determine if some Tasks have no files, and mark them as complete
        for task in self.tasks.values():
            if task.type == TaskType.MAP and not task.files:
                task.status = TaskStatus.COMPLETED

    def add_reduce_tasks(self, temp_dir: pathlib.Path):
        """Add the reduce tasks to the job."""
        # Get the current task count
        self.tasks.clear()
        # Create the reduce tasks, starting at the current task count
        for i in range(self.get_reducer_count()):
            self.tasks[i] = Task(TaskType.REDUCE, i)
        # Loop over the temporary files, and assign to part in filename
        for file in sorted(temp_dir.iterdir()):
            part = int(file.name.split('part')[-1])
            # Get the task for the part
            task: Task = self.tasks[part]
            task.files.append(file)
