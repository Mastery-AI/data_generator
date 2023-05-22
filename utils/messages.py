"""Different ADT's used in the mapreduce framework."""

from enum import Enum


class MessageType(Enum):
    """Message types for the mapreduce framework."""

    SHUTDOWN = "shutdown"
    REGISTER = "register"
    REGISTER_ACK = "register_ack"
    NEW_MAP_TASK = "new_map_task"
    NEW_REDUCE_TASK = "new_reduce_task"
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
    MessageType.NEW_MAP_TASK: {
        "task_id": int,
        "input_paths": list,
        "executable": str,
        "output_directory": str,
        "num_partitions": int,
        "worker_host": str,
        "worker_port": int
    },
    MessageType.NEW_REDUCE_TASK: {
        "task_id": int,
        "input_paths": list,
        "executable": str,
        "output_directory": str,
        "worker_host": str,
        "worker_port": int
    },
    MessageType.FINISHED: {
        "task_id": int,
        "worker_host": str,
        "worker_port": int,
    }
}
