"""Different messages used in the Generator framework."""

from enum import Enum


class MessageType(Enum):
    """Message types for the Generator framework."""

    SHUTDOWN = "shutdown"
    REGISTER = "register"
    REGISTER_ACK = "register_ack"
    NEW_TASK = "new_task"
    FINISHED = "finished"
    HEARTBEAT = "heartbeat"
    NEW_MANAGER_JOB = "new_manager_job"


MessageArgs = {
    MessageType.SHUTDOWN: {},
    MessageType.REGISTER: {
        "worker_host": str,
        "worker_port": int
    },
    MessageType.REGISTER_ACK: {
        "worker_host": str,
        "worker_port": int
    },
    MessageType.NEW_TASK: {
        "task_id": int,
        "input_paths": list,
        "executable": str,
        "output_directory": str,
        "num_partitions": int,
        "worker_host": str,
        "worker_port": int
    },
    MessageType.FINISHED: {
        "task_id": int,
        "worker_host": str,
        "worker_port": int,
    }
}
