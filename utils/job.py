"""Job that is assigned to a Manager in the MapReduce framework."""
from dataclasses import dataclass
import logging
import pathlib
from typing import Dict, Tuple

from data_generator.utils.task import Task, TaskStatus, TaskType

LOGGER = logging.getLogger(__name__)


@dataclass
class Job:
    """A Job for the Manager to execute."""

    # Job configuration
    job_id: int  # Unique ID
    source_path: pathlib.Path  # Path to source work
    prompt_path: pathlib.Path  # Path to gpt prompt
    # For py1int, this needs to be one vatibale
    count: int  # worker count
    input_dir: pathlib.Path  # Path to input directory

    # Job status
    output_dir: pathlib.Path = None  # Path to output directory
    tasks: Dict[int, Task] = None  # Tasks for the job

    # NOT NECESSARY
    # in_map_stage: bool = True  # Whether the job is in the map stage
    

    def __str__(self):
        """Return a string representation of all attributes in the job."""
        return (f"Job("
                f"job_id={self.job_id}\n"
                f"source_path={self.source_path}\n"
                f"prompt_path={self.prompt_path}\n"
                f"worker_count={self.count}\n"
                f"input_dir={self.input_dir}\n"
                f"output_dir={self.output_dir}\n"
                f"tasks={self.tasks}\n"
                )

    def reset(self):
        """Reset the job."""
        self.tasks.clear()
        # self.output_dir = None
        LOGGER.debug("Job reset")

    def get_worker_count(self) -> int:
        """Get the number of workers for the job."""
        return self.count
    
    def get_remaining_tasks(self) -> int:
        """Get the number of remaining map tasks."""
        if self.tasks is None:
            return self.count
        return sum([1 for task in self.tasks.values()
                    if task.status != TaskStatus.COMPLETED])


    def add_tasks(self) -> int:
        """Add the tasks to the job."""
        # Create the map tasks
        if self.tasks is None:
            self.tasks = {}
        else:
            self.tasks.clear()
        for i in range(self.get_worker_count()):
            self.tasks[i + 1] = Task(i + 1)
        # Loop over the input files and assign
        for idx, file in enumerate(sorted(self.input_dir.iterdir())):
            task: Task = self.tasks[idx % self.get_worker_count() + 1]
            task.files.append(file)
        # Determine if some Tasks have no files, and mark them as complete
        for task in self.tasks.values():
            if not task.files:
                task.status = TaskStatus.COMPLETED
