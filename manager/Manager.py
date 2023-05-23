"""Implementation of the Manager for the MapReduce framework."""
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
from data_generator.utils.messages import MessageType

LOGGER = logging.getLogger(__name__)

TIME_FACTOR = 0.75  # TO DO


class Manager(Node):
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        super().__init__(NodeType.MANAGER,
                         host=host,
                         port=port,
                         wants_shutdown=False)

        self.__workers: Dict[Tuple[str, int], WorkerProxy] = {}
        self.__jobs: queue.Queue[Job] = queue.Queue()
        self.__running_job_count = 1

        LOGGER.info(
            "starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.__execute()

    ###############
    # All Threads #
    ###############

    def __execute(self):
        """
        Run all the threads for the Manager node.

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
        job_thread = threading.Thread(target=self.job_thread)
        # Fault thread for handling failed workers
        fault_thread = threading.Thread(target=self.fault_thread)
        # Heartbeat thread for listening to heartbeat messages
        heartbeat_thread = threading.Thread(target=self.heartbeat_thread)
        # For each thread, start and sleep 1 second
        for thread in (job_thread, fault_thread,
                       heartbeat_thread, main_thread):
            thread.start()  # Start thread
            time.sleep(0.25 * TIME_FACTOR)  # Allow thread to initialize
        # Join all threads, for proper shutdown
        for thread in (heartbeat_thread, main_thread,
                       fault_thread, job_thread):
            thread.join()
        LOGGER.info("all Manager threads shut down. %s:%i is available",
                    self.host, self.port)

    ####################
    # Message Handling #
    ####################

    def process_message(self, message_dict: dict):
        """Interpret and handle TCP message."""
        try:
            message_type = MessageType(message_dict.get('message_type', None))
        except ValueError:
            LOGGER.error("Received unknown message type %s",
                         message_dict.get('message_type', None))
            return
        # If we receive a shutdown message, shut down all he workers
        if message_type == MessageType.SHUTDOWN:
            LOGGER.info("shutting down workers...")
            self.__shutdown_workers()
            self.wants_shutdown = True
        # If we receive a new job, add it to the queue
        elif message_type == MessageType.NEW_MANAGER_JOB:
            # Pass along the message to __add_job
            self.__add_job(
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
            self.__register_worker(worker_host, worker_port)
        # Finished
        elif message_type == MessageType.FINISHED:
            task_id = message_dict.get('task_id', None)
            worker_host = message_dict.get('worker_host', None)
            worker_port = message_dict.get('worker_port', None)
            if task_id is None:
                LOGGER.error("Received message without task id.")
                return
            # Get the current job
            current_job = self.__get_current_job()
            task = current_job.tasks.get(task_id, None)
            if not task:
                LOGGER.error("Received message for unknown task.")
                return
            LOGGER.info("finished %s task %s for job %i (wrkr=%s:%i)",
                        task.type.value, task.task_id, 
                        current_job.job_id,
                        worker_host, worker_port)
            # Now say how many remaiing tasks there are
            remaining_map_tasks = current_job.get_remaining_map_tasks()
            remaining_reduce_tasks = current_job.get_remaining_reduce_tasks()
            LOGGER.info("job %i remaining tasks: map=%i reduce=%i",
                        current_job.job_id,
                        remaining_map_tasks, remaining_reduce_tasks)

            
            worker_for_task: WorkerProxy = task.worker
            worker_for_task.clear_task()
            task.detach()
            task.status = TaskStatus.COMPLETED

    ##################################
    # Worker Registration & Shutdown #
    ##################################

    def __register_worker(self, worker_host: str, worker_port: int):
        """Register a new worker with the manager.

        If the worker is not already registered, then it is a new
        worker and will be added to the __workers dict,
        and sent an acknowledgement message.

        If this worker is already registered, then it
        must be a revived worker, so we should clear its
        current task and mark it as available.
        """
        # Search __workers to see if the worker is already registered
        worker: WorkerProxy = self.__workers.get(
            (worker_host, worker_port), None)
        # If the worker is already registered
        if worker:
            LOGGER.info('worker %s:%i revived!')
            LOGGER.debug("Worker %s:%i revived! "
                        "Clearing its task and resetting heartbeats.",
                        worker_host, worker_port)

            # Reset the worker's heartbeat count,
            # mark it as available, and if it had
            # a task, mark it as lost
            #
            # This could be done in the WorkerProxy class,
            # but it is done here because I was frustrated
            worker.reset_heartbeat_count()
            worker.status = WorkerStatus.AVAILABLE
            if worker.current_task:
                worker.current_task.status = TaskStatus.LOST
            

            num_connected_workers = self.__get_num_connected_workers()
            LOGGER.info('available workers: %i', num_connected_workers)

        # Since we need to send an acknowledgement message to the worker
        # regardless of whether it is new or revived, we can just
        # send the message here
        msg_t = MessageType.REGISTER_ACK
        if network.attempt_message(sender=self,
                                   logger=LOGGER,
                                   subject="Register Ack",
                                   dest_host=worker_host,
                                   dest_port=worker_port,
                                   message_type=msg_t,
                                   worker_host=worker_host,
                                   worker_port=worker_port):
            # If the message succeeds, add the worker (or revive it)
            LOGGER.info("worker %s:%i registered",
                        worker_host, worker_port)
            self.__workers[(worker_host, worker_port)] = \
                WorkerProxy(
                    host=worker_host,
                    port=worker_port,
                    status=WorkerStatus.AVAILABLE
                )
            num_connected_workers = self.__get_num_connected_workers()
            LOGGER.info('available workers: %i', num_connected_workers)
        else:
            # If the message fails, remove the worker
            LOGGER.error(("Failed to send "
                         "acknowledgement to worker %s:%i"),
                         worker_host, worker_port)
            # If the worker is already registered, we need
            # to mark it as dead
            if worker:
                worker.status = WorkerStatus.DEAD

    def __shutdown_workers(self):
        """Send shutdown message to all alive workers."""
        for worker in self.__workers.values():
            LOGGER.debug("shutting down worker %s:%i", worker.host, worker.port)
            if not worker.status == WorkerStatus.DEAD:
                network.attempt_message(sender=self,
                                        logger=LOGGER,
                                        subject="Shutdown",
                                        message_type=MessageType.SHUTDOWN,
                                        dest_host=worker.host,
                                        dest_port=worker.port,)
                

    def __get_num_connected_workers(self) -> int:
        """Return the number of connected workers."""
        num_connected_workers = 0
        for worker in self.__workers.values():
            if worker.status != WorkerStatus.DEAD:
                num_connected_workers += 1
        return num_connected_workers

    #################
    # Job Add & Get #
    #################

    def __add_job(self, input_dir: Path, output_dir: Path,
                  mapper_path: Path, reducer_path: Path,
                  counts: Tuple[int, int]):
        """
        Create a new job.

        This will create a new job object, create the
        output directory (delete it if it already exists),
        and add it to the __jobs queue.
        """
        if None in (input_dir, output_dir, mapper_path, reducer_path,
                    counts):
            LOGGER.error("Received message without all required fields.")
            return
        input_dir, output_dir = Path(input_dir), Path(output_dir)
        LOGGER.info("new job %i received", self.__running_job_count)
        # Get the number connected workers
        num_connected_workers = self.__get_num_connected_workers()
        LOGGER.info('available workers: %i', num_connected_workers)
        new_job = Job(
            job_id=self.__running_job_count,
            mapper_path=mapper_path,
            reducer_path=reducer_path,
            counts=counts,  # (num_mappers, num_reducers)
            input_dir=input_dir,
            output_dir=output_dir
        )
        
        # Make the output directory
        # If the output directory already exists, delete it
        if Path(output_dir).exists():
            LOGGER.info("Deleting pre-existing output directory.")
            # Delete it even if it is not empty
            shutil.rmtree(output_dir)
        # Create the output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Created output directory %s",
                    output_dir)
        # Add the job to the queue
        self.__jobs.put(new_job)
        self.__running_job_count += 1

        

    def __get_current_job(self) -> Job:
        """Return the current job being executed."""
        # Get the job at the front of the queue
        return self.__jobs.queue[0]

    def job_thread(self):
        """Get next job from queue and begin execution."""
        LOGGER.info("starting job thread.")
        while not self.wants_shutdown:
            time.sleep(0.25 * TIME_FACTOR)  # For busy waiting
            # If there are no jobs in the queue, wait
            if self.__jobs.empty():
                continue

            # Get the next job from the queue
            current_job: Job = self.__get_current_job()

            LOGGER.info("beginning job %i...",
                        current_job.job_id)

            with tempfile.TemporaryDirectory(
                  prefix=f"mapreduce-shared-job{current_job.job_id:05d}-"
                 ) as temp_dir:
                LOGGER.debug("Created a temporary directory: %s",
                             temp_dir)
                # LOGGER.info('Created temp directory')

                # Perform the job to completion
                self.__perform_job(current_job, Path(temp_dir))

            self.__jobs.get()  # Remove job from queue

        # End the job thread
        LOGGER.debug("job thread shutting down.")

    ####################
    # Completing Tasks #
    ####################

    def __perform_job(self, current_job: Job, temp_dir: Path):
        """Perform a mapreduce job.

        This will create the output directory or delete
        the pre-existing one, perform all map tasks, and
        then perform all reduce tasks.
        """
        # Perform and complete map tasks
        self.__perform_map_tasks(current_job, temp_dir)
        LOGGER.debug("map tasks complete for job %i", current_job.job_id)
        # Perform and complete reduce tasks
        self.__perform_reduce_tasks(current_job, temp_dir)
        LOGGER.debug("meduce tasks complete for job %i", current_job.job_id)

        LOGGER.info("job %i complete",
                    current_job.job_id,)
        LOGGER.info("job %i output directory: %s",
                    current_job.job_id,
                    current_job.output_dir)

    def __perform_map_tasks(self, current_job: Job, temp_dir: Path):
        """Perform all of the map tasks for a particular job.

        This will calculate all of the map tasks for the job, and
        then indefinitely loop, assigning tasks to workers until
        all tasks are complete.
        """
        # Create a queue for tasks.
        # This will act as a buffer for tasks that are not yet assigned,
        # so that we only move on to the next task when the previous one
        # is complete.
        ###############################
        incomplete_tasks = queue.Queue()
        ###############################

        # Assign map tasks to the job
        LOGGER.debug("assigning map tasks for job %i...",
                    current_job.job_id)
        current_job.add_map_tasks()
        # Add map tasks to the queue
        for task in current_job.tasks.values():
            task: Task
            if task.type == TaskType.REDUCE:
                LOGGER.error('unexpected reduce task')
            else:
                incomplete_tasks.put(task)

        # Wait for all map tasks to finish
        while not incomplete_tasks.empty() and not self.wants_shutdown:
            # Get the next task
            task: Task = incomplete_tasks.get()

            # If the task is a reduce task
            if task.type == TaskType.REDUCE:
                LOGGER.error('unexpected reduce task')
                break
            # If the task is complete
            if task.status == TaskStatus.COMPLETED:
                continue
            if task.status == TaskStatus.RUNNING:
                # If the task is running, put it back in the queue
                incomplete_tasks.put(task)
                continue
            # Assign the task to a worker
            if task.status in (TaskStatus.UNASSIGNED, TaskStatus.LOST):
                incomplete_tasks.put(task)  # Put it back in the queue
                # Keep trying to assign the task until it is assigned
                self.__assign_map_task(current_job, task, temp_dir)

            time.sleep(0.5 * TIME_FACTOR)  # To avoid busy waiting

    def __perform_reduce_tasks(self, current_job: Job, temp_dir: Path):
        """Perform all of the reduce tasks for a particular job.

        This will calculate all of the reduce tasks for the job, and
        then indefinitely loop, assigning tasks to workers until
        all tasks are complete.
        """
        # Create a queue for tasks.
        # This will act as a buffer for tasks that are not yet assigned,
        # so that we only move on to the next task when the previous one
        # is complete.
        ###############################
        incomplete_tasks = queue.Queue()
        ###############################

        # Assign reduce tasks to the job
        LOGGER.debug("assigning reduce tasks for job %i...",
                    current_job.job_id)
        current_job.add_reduce_tasks(temp_dir)
        # Assign reduce tasks to the job
        for task in current_job.tasks.values():
            task: Task
            if task.type == TaskType.MAP:
                LOGGER.error('unexpected map task')
            else:
                incomplete_tasks.put(task)

        # Wait for all reduce tasks to finish
        while not incomplete_tasks.empty() and not self.wants_shutdown:
            # Remove a task from the queue
            task: Task = incomplete_tasks.get()
            # If the task is a map task
            if task.type == TaskType.MAP:
                LOGGER.error('unexpected map task')
                break
            # If the task is complete
            if task.status == TaskStatus.COMPLETED:
                continue
            if task.status == TaskStatus.RUNNING:
                incomplete_tasks.put(task)  # Put it back in the queue
                continue
            # Assign the task to a worker
            if task.status in (TaskStatus.UNASSIGNED, TaskStatus.LOST):
                incomplete_tasks.put(task)  # Put it back in the queue
                # Keep trying to assign the task until it is assigned
                self.__assign_reduce_task(current_job, task)

            time.sleep(0.5 * TIME_FACTOR)  # To avoid busy waiting

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def __assign_map_task(self, current_job: Job,
                          map_task: Task, temp_dir: Path):
        """Assign a particular map task for a job.

        This will indefinitely loop through all of the workers until
        it finds one that is available. It will then send the worker
        a message to perform the task.

        If a worker is found to be non-responsive, it will be marked
        as dead.
        """
        LOGGER.debug("Assigning map task %i", map_task.task_id)
        # Need to loop indefinitely until the task is assigned
        while not self.wants_shutdown:
            time.sleep(0.5 * TIME_FACTOR)  # Avoid busy waiting
            # Loop through all workers, looking for an available one
            for worker in self.__workers.values():
                # If the worker is busy or dead, skip it
                if worker.status in (WorkerStatus.BUSY, WorkerStatus.DEAD):
                    continue
                # If the worker is available
                if worker.status == WorkerStatus.AVAILABLE:
                    
                    # Attempt to send the message to the worker
                    if network.attempt_message(
                        sender=self,
                        logger=LOGGER,
                        subject='assign map task',
                        message_type=MessageType.NEW_MAP_TASK,
                        dest_host=worker.host,
                        dest_port=worker.port,
                        task_id=map_task.task_id,
                        input_paths=map_task.get_file_paths(),
                        executable=current_job.mapper_path,
                        output_directory=str(temp_dir),
                        num_partitions=current_job.get_reducer_count(),
                        worker_host=worker.host,
                        worker_port=worker.port,
                        job_id=current_job.job_id,
                    ):
                        # If the message was sent successfully,
                        # give the worker the task, which will set its status
                        # to busy, set the task's status to running,
                        # and set the task's worker to the worker
                        worker.give_task(map_task)
                        map_task.status = TaskStatus.RUNNING
                        LOGGER.info("assigned map task %i for job %i (wrkr=%s:%i)",
                                    map_task.task_id, current_job.job_id,
                                    worker.host, worker.port)
                        return  # The task has been assigned, so return

                    # If it gets to this point, the message was not sent
                    # successfully. This means a ConnectionRefusedError
                    # occurred. The worker should be killed.
                    LOGGER.info(("failed to assign "
                                 "map task %i for job %i, killing worker %s:%i..."),
                                 map_task.task_id, current_job.job_id,
                                 worker.host, worker.port)
                    # Killing the worker will set its status to dead,
                    # and make it lose its current task
                    worker.kill()
                    num_connected_workers = self.__get_num_connected_workers()
                    LOGGER.info('available workers: %i', num_connected_workers)
            LOGGER.debug("Can't find a worker for map task %i",
                         map_task.task_id)

    def __assign_reduce_task(self, current_job: Job, reduce_task: Task):
        """Assign a particular reduce task to a job.

        This will indefinitely loop through all of the workers until
        it finds one that is available. It will then send the worker
        a message to perform the task.

        If a worker is found to be non-responsive, it will be marked
        as dead.
        """
        # Need to loop indefinitely until the task is assigned
        while not self.wants_shutdown:
            time.sleep(0.5 * TIME_FACTOR)  # Avoid busy waiting
            # Loop through all workers, looking for an available one
            for worker in self.__workers.values():
                # If the worker is busy or dead, skip it
                if worker.status in (WorkerStatus.BUSY, WorkerStatus.DEAD):
                    continue
                if worker.status == WorkerStatus.AVAILABLE:
                    # Attempt to send the message to the worker
                    if network.attempt_message(
                        sender=self,
                        logger=LOGGER,
                        subject='assign reduce task',
                        message_type=MessageType.NEW_REDUCE_TASK,
                        dest_host=worker.host,
                        dest_port=worker.port,
                        task_id=reduce_task.task_id,
                        executable=current_job.reducer_path,
                        input_paths=reduce_task.get_file_paths(),
                        output_directory=str(current_job.output_dir),
                        worker_host=worker.host,
                        worker_port=worker.port,
                        job_id=current_job.job_id,
                    ):
                        # If the message was sent successfully,
                        # give the worker the task, which will set its status
                        # to busy, set the task's status to running,
                        # and set the task's worker to the worker
                        worker.give_task(reduce_task)
                        reduce_task.status = TaskStatus.RUNNING
                        LOGGER.info("assigned reduce task %i for job %i (wrkr=%s:%i)",
                                    reduce_task.task_id, current_job.job_id,
                                    worker.host, worker.port)
                        return  # The task has been assigned, so return

                    # If it gets to this point, the message was not sent
                    # successfully. This means a ConnectionRefusedError
                    # occurred. The worker should be killed.
                    LOGGER.info(("failed to assign "
                                 "reduce task %i for job %i, killing worker %s:%i..."),
                                 reduce_task.task_id, current_job.job_id,
                                 worker.host, worker.port)
                    # Killing the worker will set its status to dead,
                    # and make it lose its current task
                    worker.kill()
            LOGGER.debug("Can't find a worker for reduce task %i",
                         reduce_task.task_id)

    ###################
    # Fault Tolerance #
    ###################

    def heartbeat_thread(self):
        """Listen for heartbeats from workers."""
        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to host
            sock.bind((self.host, self.port))
            LOGGER.info("starting heartbeat thread (%s:%i/udp)",
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
                    LOGGER.info(self.__workers)
                    LOGGER.info((worker_host, worker_port))
                    worker_obj = self.__workers.get(
                        (worker_host, worker_port), None)
                    if not worker_obj:
                        LOGGER.info(("heartbeat message received from"
                                    " unregistered worker %s:%i. shutting it down..."),
                                    worker_host, worker_port)
                        # Shutdown the worker
                        network.attempt_message(
                            sender=self,
                            logger=LOGGER,
                            subject='shutdown worker',
                            message_type=MessageType.SHUTDOWN,
                            dest_host=worker_host,
                            dest_port=worker_port,
                        )
                    else:
                        # Reset the heartbeat count
                        worker_obj.reset_heartbeat_count()

        LOGGER.debug('Heartbeat (UDP) thread shutting down.')

    def fault_thread(self):
        """Handle the death of a worker (RIP Fella)."""
        time.sleep(1)
        LOGGER.info("starting fault thread.")
        while not self.wants_shutdown:
            for worker in self.__workers.values():
                if worker.status == WorkerStatus.DEAD:
                    continue
                if worker.heartbeats == 0:
                    LOGGER.info("worker %s:%i died. rest in peace...",
                                worker.host, worker.port)
                    worker.kill()
                    num_connected_workers = self.__get_num_connected_workers()
                    LOGGER.info('available workers: %i', num_connected_workers)
                else:
                    worker.heartbeats -= 1
            # Sleep for a bit
            time.sleep(2)
        LOGGER.debug("Fault tolerence thread shutting down.")