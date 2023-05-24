from enum import Enum
import logging

from dataclasses import dataclass
import time

from data_generator.utils.task import Task, TaskStatus

LOGGER = logging.getLogger(__name__)


class WorkerStatus(Enum):
    """Status of a Worker node in the Generator framework."""

    AVAILABLE = 1
    BUSY = 2
    DEAD = 3


NUM_HEARTBEATS = 5


@dataclass
class WorkerProxy:
    """Proxy for a Worker node in the Generator framework."""

    host: str  # Hostname
    port: int  # Port
    status: WorkerStatus  # Status of the Worker node
    heartbeats: int = NUM_HEARTBEATS  # Number of heartbeats received
    current_task: Task = None  # Current task being executed
    last_heartbeat: float = time.time()  # Time of last heartbeat

    def __str__(self):
        """Return a string representation of the object."""
        task_id = None if not self.current_task else self.current_task.task_id
        return (
            f"host: {self.host} \n"
            f"port: {self.port} \n"
            f"status: {self.status} \n"
            f"heartbeats: {self.heartbeats} \n"
            f"cur_task: {task_id} \n"
        )

    def reset_heartbeat_count(self):
        """Reset the number of heartbeats received."""
        self.heartbeats = NUM_HEARTBEATS
        self.last_heartbeat = time.time()
        # LOGGER.debug("Heartbeats reset for worker %s:%i",
        # self.host, self.port)

    def give_task(self, task: Task):
        """Give a task to the Worker node."""
        self.current_task = task
        self.status = WorkerStatus.BUSY
        self.current_task.assign_to_worker(self)
        LOGGER.debug("Worker=%s:%i received task %s",
                    self.host, self.port, task.task_id)

    def clear_task(self):
        """Clear the current task from the Worker node."""
        self.current_task = None
        if self.status != WorkerStatus.DEAD:
            self.status = WorkerStatus.AVAILABLE
        LOGGER.debug("Worker=%s:%i task cleared", self.host, self.port)

    def get_task(self) -> Task:
        """Get the current task from the Worker node."""
        return self.current_task

    def lose_task(self):
        """Lose the current task from the Worker node."""
        if not self.current_task:
            return
        self.current_task.worker = None
        self.current_task.status = TaskStatus.LOST
        self.current_task = None
        LOGGER.debug("Worker=%s:%i task lost", self.host, self.port)

    def revive(self):
        """
        Revive the worker node.

        This will clear its current task,
        reset its heartbeat count, and
        set its status to AVAILABLE.
        """
        if self.current_task:
            self.current_task.status = TaskStatus.LOST
        self.reset_heartbeat_count()
        self.status = WorkerStatus.AVAILABLE
        LOGGER.debug("Worker=%s:%i revived", self.host, self.port)

    def kill(self):
        """
        Kills the Worker node.

        This will lose the current task and set the status to DEAD.
        """
        self.lose_task()
        self.status = WorkerStatus.DEAD
        LOGGER.debug("Worker=%s:%i killed", self.host, self.port)