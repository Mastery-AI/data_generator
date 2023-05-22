# goal is to take a folder of example files and create a folder containing all newly generated files
#   - step 1 -> receive folder of files, store text from each
#   - step 2 -> create a new thread for every file's text
#   - step 3 -> thread passes example with prompt to gpt api
#   - step 4 -> each thread creates an output file in the given output directory
#   - step 5 -> close thread

# questions:
# - 1 How do you define a thread's action
#   - answer: network communications
# - 2 How do you pass variables into threads
#   - answer: once again, network communications

## This will essentially be copy of the Manager class from mapreduce but with adapted messages

"""Implementation of the Manager for the data generation framework."""
import os
from pathlib import Path
import shutil
import tempfile
import logging
import json
import socket
import threading
import time
import queue
from typing import Dict, Tuple
from data_generator.utils.node import Node, NodeType
from data_generator.utils.task import Task, TaskStatus, TaskType
from data_generator.utils import network
from data_generator.utils.job import Job
from data_generator.manager.worker_proxy import \
    WorkerProxy, WorkerStatus as WorkerStatus
from mapreduce.utils.messages import MessageType

LOGGER = logging.getLogger(__name__)


class Manager(Node):
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        super().__init__(NodeType.MANAGER,
                         host=host,
                         port=port,
                         wants_shutdown=False)

        self.__workers: Dict[Tuple[str, int], WorkerProxy] = {}
        self.__jobs: queue.Queue = queue.Queue()
        self.__total_jobs = 0

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.__execute()

    def process_message(self, message_dict: dict):
        """Interpret and handle TCP message."""
        try:
            message_type = MessageType(message_dict.get('message_type', None))
        except ValueError:
            LOGGER.error("Received unknown message type %s",
                         message_dict.get('message_type', None))
            return
        if message_type == MessageType.SHUTDOWN:
            LOGGER.info("Shutting down workers.")
            self.shutdown_workers()
            self.wants_shutdown = True

        elif message_type == MessageType.NEW_MANAGER_JOB:

            self.__create_job(
                input_dir=message_dict.get('input_directory', None),
                output_dir=message_dict.get('output_directory', None),
                mapper_path=message_dict.get('mapper_executable', None),
                reducer_path=message_dict.get('reducer_executable', None),
                # For py1int, need to make this one variable
                counts=(message_dict.get('num_mappers', None),
                        message_dict.get('num_reducers', None))
            )
        # Register a worker
        elif message_type == MessageType.REGISTER:
            worker_host = message_dict.get('worker_host', None)
            worker_port = message_dict.get('worker_port', None)
            if not worker_host or not worker_port:
                LOGGER.error("Received message without worker host or port.")
                return
            # See if the worker is already registered
            worker: WorkerProxy = self.__workers.get(
                (worker_host, worker_port), None)
            # If not, register it
            if not worker:
                self.__workers[(worker_host, worker_port)] = \
                    WorkerProxy(
                        host=worker_host,
                        port=worker_port,
                        status=WorkerStatus.AVAILABLE
                    )
                LOGGER.info("Registered worker %s:%i",
                            worker_host, worker_port)
                # Attempt to send acknowledgement
                msg_t = MessageType.REGISTER_ACK
                if not network.attempt_message(sender=self,
                                               logger=LOGGER,
                                               dest_host=worker_host,
                                               dest_port=worker_port,
                                               message_type=msg_t,
                                               worker_host=worker_host,
                                               worker_port=worker_port):
                    # If the message fails, remove the worker
                    LOGGER.error("Failed to send \
                                 acknowledgement to worker %s:%i",
                                 worker_host, worker_port)
                    self.__workers.pop((worker_host, worker_port))
            # If it is, lose its task and become available
            else:
                # TODO - we need to make sure that if this revive
                # happens, we dont lose the task and do not mark the
                # as dead, because after this message the
                # RIP worker message is logged
                LOGGER.info("Worker %s:%i already registered!",
                            worker_host, worker_port)
                worker.lose_task()
                worker.status = WorkerStatus.AVAILABLE
        # Finished
        elif message_type == MessageType.FINISHED:
            task_id = message_dict.get('task_id', None)
            if task_id is None:
                LOGGER.error("Received message without task id.")
                return
            # Get the current job
            current_job = self.__get_current_job()
            task = current_job.tasks.get(task_id, None)
            if not task:
                LOGGER.error("Received message for unknown task.")
                return
            worker_for_task: WorkerProxy = task.worker
            worker_for_task.clear_task()
            task.detach()
            task.status = TaskStatus.COMPLETED

    def __str__(self):
        """Return a string representation of the Manager."""
        return f"Manager(host={self.host}, port={self.port})"

    def __execute(self):
        """
        Start the main threads for the Manager node.

        This method starts the following threads:
        - A TCP thread for listening to incoming messages.
        - A job thread for executing MapReduce jobs.
        - A fault thread for handling failed workers.
        - A UDP heartbeat thread for monitoring worker status.

        The threads are started in a specific order and joined at
        the end to ensure proper execution. Each thread is started
        with a delay of 1 second to allow for initialization and
        avoid race conditions.
        """
        # Main thread for listening to TCP messages
        main_thread = threading.Thread(
            target=network.run_main_thread,
            args=(self, LOGGER))
        # Job thread for executing jobs
        job_thread = threading.Thread(target=self.run_job_thread)
        # Fault thread for handling failed workers
        fault_thread = threading.Thread(target=self.run_fault_thread)
        # Heartbeat thread for listening to heartbeat messages
        heartbeat_thread = threading.Thread(target=self.run_heartbeat_thread)
        # For each thread, start and sleep 1 second
        for thread in (job_thread, fault_thread,
                       heartbeat_thread, main_thread):
            thread.start()
            time.sleep(0.25)
        # Join all threads
        for thread in (heartbeat_thread, main_thread,
                       fault_thread, job_thread):
            thread.join()
        LOGGER.info("Manager shutting down. %s:%i is available.",
                    self.host, self.port)

    def __create_job(self, input_dir: Path, output_dir: Path,
                     mapper_path: Path, reducer_path: Path,
                     counts: Tuple[int, int]) -> Job:
        """Create a new job."""
        if None in (input_dir, output_dir, mapper_path, reducer_path,
                    counts):
            LOGGER.error("Received message without all required fields.")
            return
        input_dir, output_dir = Path(input_dir), Path(output_dir)
        LOGGER.info("New job %i received", self.__total_jobs)
        new_job = Job(
            job_id=self.__total_jobs,
            mapper_path=mapper_path,
            reducer_path=reducer_path,
            counts=counts,
            input_dir=input_dir,
            output_dir=output_dir
        )
        # Make the output directory
        # If the output directory already exists, delete it
        if Path(output_dir).exists():
            LOGGER.info("Deleting pre-existing \
                         output directory.")
            # Delete it even if it is not empty
            shutil.rmtree(output_dir)
        # Create the output directory
        output_dir.mkdir()
        LOGGER.info("Created output directory %s",
                    output_dir)
        # Add the job to the queue

        self.__jobs.put(new_job)
        self.__total_jobs += 1

    def __get_current_job(self) -> Job:
        """Return the current job being executed."""
        # Get the job at the front of the queue
        return self.__jobs.queue[0]

    def shutdown_workers(self):
        """Send shutdown message to all alive workers."""
        for worker in self.__workers.values():
            LOGGER.info("Shutting down worker %s:%i", worker.host, worker.port)
            if not worker.status == WorkerStatus.DEAD:
                network.attempt_message(sender=self,
                                        logger=LOGGER,
                                        message_type=MessageType.SHUTDOWN,
                                        dest_host=worker.host,
                                        dest_port=worker.port,)

    def run_job_thread(self):
        """Get next job from queue and begin execution."""
        LOGGER.info("Starting a new job thread.")
        while not self.wants_shutdown:
            time.sleep(0.25)  # For busy waiting
            # If there are no jobs in the queue, wait
            if self.__jobs.empty():
                continue
            # Get the next job from the queue
            current_job: Job = self.__get_current_job()

            with tempfile.TemporaryDirectory(
                  prefix=f"mapreduce-shared-job{current_job.job_id:05d}-"
                 ) as temp_dir:
                LOGGER.debug("Created a temporary directory: %s",
                             temp_dir)
                LOGGER.info('Created temp directory')
                LOGGER.info("Created beginning job %i",
                            current_job.job_id)
                # Add and assign map tasks
                self.__assign_tasks(Path(temp_dir), TaskType.MAP)
                LOGGER.debug("Waiting for map tasks to finish.")
                # Add and assign reduce tasks
                self.__assign_tasks(Path(temp_dir), TaskType.REDUCE)

            LOGGER.info("Job %i complete, Output in %s",
                        current_job.job_id,
                        current_job.output_dir)
            self.__jobs.get()  # Remove job from queue
        # End the job thread
        LOGGER.info("Job thread shutting down.")

    def __assign_tasks(self, temp_dir: Path, task_type: TaskType):
        """Assign tasks until they are all finished."""
        # Get the current job
        current_job = self.__get_current_job()
        # Add tasks to the job
        if task_type == TaskType.MAP:
            LOGGER.debug("Assigning map task for job %i",
                         current_job.job_id)
            current_job.add_map_tasks()
        elif task_type == TaskType.REDUCE:
            LOGGER.debug("Assigning reduce task for job %i",
                         current_job.job_id)
            current_job.add_reduce_tasks(temp_dir)
        # Assign tasks to workers
        LOGGER.info("Assigning %s workers for job %i",
                    task_type.value, current_job.job_id)
        # LOGGER.debug(str(current_job))
        while True:
            # Flag to determine if all tasks are complete
            has_incomplete_task = False
            # Iterate through all tasks in the job
            for task in current_job.tasks.values():
                # If the task is not complete, assign it to a worker
                if task.status in (TaskStatus.UNASSIGNED, TaskStatus.LOST):
                    self.__assign_task_to_worker(task, temp_dir)
                    has_incomplete_task = True  # Must keep looping
                # If there is a currently running task, must keep looping
                elif task.status == TaskStatus.RUNNING:
                    has_incomplete_task = True
            # If all tasks are complete or wants shutdown, break ouT
            if self.wants_shutdown or not has_incomplete_task:
                break
            # Sleep for 0.5 seconds to avoid busy waiting
            LOGGER.debug("Sleeping for 0.25 seconds.")
            time.sleep(0.25)
        LOGGER.info("All %s tasks assigned for job %i.",
                    task_type.value, current_job.job_id)

    def __assign_task_to_worker(self, task: Task, temp_dir: Path):
        """Assign task to next avaliable worker."""
        while not self.wants_shutdown:
            # Get the current job
            current_job = self.__get_current_job()
            # LOGGER.debug(str(current_job))
            # Iterate through all workers
            for worker in self.__workers.values():
                # Only assign task to available workers
                if worker.status == WorkerStatus.AVAILABLE:
                    LOGGER.info("Assigning %s task %i to worker %s:%i",
                                task.type.value,
                                task.task_id, worker.host, worker.port)
                    # Give it the task
                    worker.give_task(task)
                    task.status = TaskStatus.RUNNING
                    # If the task is a map task
                    if task.type == TaskType.MAP:
                        # Attempt to send the message
                        if not network.attempt_message(
                            sender=self,
                            logger=LOGGER,
                            message_type=MessageType.NEW_MAP_TASK,
                            dest_host=worker.host,
                            dest_port=worker.port,
                            task_id=task.task_id,
                            input_paths=task.get_file_paths(),
                            executable=current_job.mapper_path,
                            output_directory=str(temp_dir),
                            num_partitions=current_job.get_reducer_count(),
                            worker_host=worker.host,
                            worker_port=worker.port
                        ):
                            # TODO this is when there is a connection
                            # refused error. Make sure the worker is marked
                            # as dead, and that the task is sent to the
                            # next available worker.
                            LOGGER.error("Failed to assign \
                                         map task %i to worker %s:%i",
                                         task.task_id, worker.host,
                                         worker.port)
                            # If the message fails, kill the worker
                            worker.kill()
                    elif task.type == TaskType.REDUCE:
                        # Attempt to send the message
                        if not network.attempt_message(
                            sender=self,
                            logger=LOGGER,
                            message_type=MessageType.NEW_REDUCE_TASK,
                            dest_host=worker.host,
                            dest_port=worker.port,
                            task_id=task.task_id,
                            executable=current_job.reducer_path,
                            input_paths=task.get_file_paths(),
                            output_directory=str(current_job.output_dir),
                            worker_host=worker.host,
                            worker_port=worker.port
                        ):
                            # TODO this is when there is a connection
                            # refused error. Make sure the worker is marked
                            # as dead, and that the task is sent to the
                            # next available worker.
                            worker.kill()
                    return
            # To avoid busy waiting
            time.sleep(0.25)

    def run_heartbeat_thread(self):
        """Listen for heartbeats from workers."""
        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to host
            sock.bind((self.host, self.port))
            LOGGER.info("New manager wants UDP listening on: %s:%i",
                        self.host, self.port)

            # Set timeout
            sock.settimeout(1)
            # Allow reuse of address
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Begin listening for messages
            while not self.wants_shutdown:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                decoded_message = data.decode("utf-8")
                message_dict: dict = json.loads(decoded_message)
                # Get the message type
                try:
                    message_type = \
                        MessageType(message_dict.get('message_type'))
                except ValueError:
                    LOGGER.error("Received unknown message type %s",
                                 message_dict.get('message_type'))
                    return
                if message_type == MessageType.HEARTBEAT:
                    # Get host and port of worker
                    worker_host = message_dict.get('worker_host', None)
                    worker_port = message_dict.get('worker_port', None)
                    if not worker_host or not worker_port:
                        LOGGER.error("Received message without worker \
                                    host or port.")
                        return
                    # Get the worker object
                    worker_obj = self.__workers.get(
                        (worker_host, worker_port), None)
                    if not worker_obj:
                        LOGGER.info("Heartbeat message received from \
                                    unregistered worker.")
                    else:
                        # Reset the heartbeat count
                        worker_obj.reset_heartbeat_count()

        LOGGER.info('Heratbeat (UDP) thread shutting down.')

    def run_fault_thread(self):
        """Handle the death of a worker (RIP Fella)."""
        time.sleep(1)
        LOGGER.info("Starting a new fault thread.")
        while not self.wants_shutdown:
            for worker in self.__workers.values():
                if worker.status == WorkerStatus.DEAD:
                    continue

                if worker.heartbeats == 0:
                    LOGGER.info("Rest in peace worker %s:%i",
                                worker.host, worker.port)
                    worker.kill()
                else:
                    worker.heartbeats -= 1
            # Sleep for a bit
            time.sleep(2)
        LOGGER.info("Fault tolerence thread shutting down.")

