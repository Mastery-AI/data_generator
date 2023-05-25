"""Implementation of the Worker for the MapReduce framework."""

from contextlib import ExitStack
import hashlib
import heapq
import json
import logging
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from typing import BinaryIO, List
from data_generator.utils.messages import MessageType

from data_generator.utils.node import Node, NodeType
from data_generator.utils import network


LOGGER = logging.getLogger(__name__)


class Worker(Node):
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port,
                 manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        super().__init__(NodeType.WORKER,
                         host=host,
                         port=port,
                         wants_shutdown=False)
        self.__manager_host = manager_host  # Hostname of the manager
        self.__manager_port = manager_port  # Port number of the manager
        self.__current_job: int = None  # ID of the current job

        LOGGER.info(
            "starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        # LOGGER.info(
        #     "manager_host=%s manager_port=%s",
        #     manager_host, manager_port,
        # )

        # Set up the threads
        self.__main_thread: threading.Thread = None
        self.__heartbeat_thread: threading.Thread = None
        self.__set_up_threads()

        # Send a register message to the manager
        network.attempt_message(self, LOGGER,
                                subject='register',
                                message_type=MessageType.REGISTER,
                                dest_host=self.__manager_host,
                                dest_port=self.__manager_port,
                                worker_host=host,
                                worker_port=port)

        # Wait for the main thread to finish
        self.__main_thread.join()
        # If the heartbeat thread is still running, wait for it to finish
        if self.__heartbeat_thread.is_alive():
            self.__heartbeat_thread.join()

    def __set_up_threads(self):
        """Set up the threads for the Worker."""
        # Setup TCP server thread
        self.__main_thread = threading.Thread(
            target=network.run_main_thread,
            args=(self, LOGGER),
        )
        # Set up heartbeat thread
        self.__heartbeat_thread = threading.Thread(
            target=self.heartbeat_thread
        )
        # Start TCP server
        self.__main_thread.start()
        # Sleep for a second to give the main thread time to start
        time.sleep(0.5)

    def __str__(self):
        """Return a string representation of the Worker."""
        return f"Worker(host={self.host}, port={self.port})"

    def heartbeat_thread(self):
        """Heartbeat UDP thread.

        This thread sends a heartbeat message to the manager every second.
        """
        LOGGER.debug("heartbeat thread started")
        # Start the heartbeat loop
        while not self.wants_shutdown:
            # Create a UDP socket
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                # Connect to the manager
                sock.connect((self.__manager_host, self.__manager_port))
                # Create a heartbeat message to the manager
                message_dict = {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
                # Serialize the message to JSON
                message = json.dumps(message_dict).encode("utf-8")
                # Send the message
                sock.sendall(message)
                # LOGGER.debug("Worker %s:%i sent heartbeat to manager %s:%d",
                #              self.host, self.port, self.__manager_host,
                #              self.__manager_port)
            # Wait for 2 seconds until next heartbeat
            time.sleep(2)

    def process_message(self, message_dict: dict):
        """Handle a message from the manager."""
        try:
            message_type = MessageType(message_dict.get("message_type"))
        except ValueError:
            LOGGER.error("Received unknown message type %s",
                         message_dict.get("message_type"))
            return
        # Handle the message based on its type, starting with the shutdown
        if message_type == MessageType.SHUTDOWN:
            LOGGER.info("received shutdown message")
            self.wants_shutdown = True
            return
        # New map task
        if message_type == MessageType.NEW_MAP_TASK:
            
            # Get the task from the message
            task_id = message_dict.get("task_id")
            input_paths = message_dict.get("input_paths")
            executable = Path(message_dict.get("executable"))
            destination = Path(message_dict.get("output_directory"))
            partitions = message_dict.get("num_partitions")
            job_id = message_dict.get("job_id")
            self.__current_job = job_id
            # Run the map task
            LOGGER.info("starting map task %s for job %s (mngr=%s:%i)", 
                        task_id, self.__current_job, self.__manager_host,
                        self.__manager_port)
            
            self.do_map_task(task_id, destination, input_paths,
                             executable, partitions)
        # New reduce task
        elif message_type == MessageType.NEW_REDUCE_TASK:
            task_id = message_dict.get("task_id")
            input_paths = message_dict.get("input_paths")
            executable = Path(message_dict.get("executable"))
            destination = Path(message_dict.get("output_directory"))
            job_id = message_dict.get("job_id")
            self.__current_job = job_id
            # Run the reduce task
            LOGGER.info("starting reduce task %s for job %s (mngr=%s:%i)", 
                        task_id, self.__current_job, self.__manager_host,
                        self.__manager_port)
            
            self.do_reduce_task(task_id, destination, input_paths, executable)
        elif message_type == MessageType.REGISTER_ACK:
            LOGGER.info("received register ack from manager %s:%i",
                        self.__manager_host, self.__manager_port)
            # Start the heartbeat thread
            LOGGER.info("ready to receive tasks")
            self.__heartbeat_thread.start()

    def do_map_task(self, task_id: int, destination: Path,
                    input_paths: List[str], executable: Path, partitions: int):
        """Execute a Map task."""
        # Create a temporary directory for the map task
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as temp_dir, ExitStack() as context_manager:
            LOGGER.debug("Created temporary directory %s for task:%s",
                         temp_dir, task_id)
            # LOGGER.info('Created temporary directory for task:%s',
            #             task_id)
            # Create seperate temporary files for the number of partitions
            temp_files = [
                context_manager.enter_context(
                    open(
                        f"{temp_dir}/maptask{task_id:05d}-part{i:05d}",
                        mode='w+',
                        encoding='utf-8',
                    )
                )
                for i in range(partitions)
            ]
            # Run the map task on each file
            for temp_file in input_paths:
                # Open each file to run a subprocess
                with open(Path(temp_file).resolve(), "r",
                          encoding='utf-8') as file:
                    # Create a subprocess to run the map task

                    with subprocess.Popen(
                        [executable],
                        stdin=file,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    ) as process:
                        # Wait for the process to finish
                        stdout, _ = process.communicate()
                        # Check if the process exited successfully
                        if process.returncode != 0:
                            LOGGER.error(
                                "Map task %s failed with return code %d",
                                task_id,
                                process.returncode,
                            )
                            # LOGGER.error("stdout: %s", stdout)
                            # LOGGER.error("stderr: %s", stderr)
                            # Stop the heartbeat thread
                            self.wants_shutdown = True
                            return
                        # If the process exited successfully
                        for line in stdout.decode("utf-8").splitlines():
                            # Get the partition number for the line
                            partition_idx = \
                                self.__get_partition_number(line, partitions)
                            # Write to the corresponding partition file
                            temp_files[partition_idx].write(line + '\n')

            # Sort the lines of each partition file
            self.__sort_lines(temp_files)

            # Move the partition files to the destination directory
            for temp_file in temp_files:
                # Move the partition file to the destination directory
                LOGGER.debug("Moving %s to %s", temp_file.name, destination)
                shutil.move(str(temp_file.name), str(destination))

            LOGGER.info("finished map task %s for job %s (mngr=%s:%i)", 
                        task_id, self.__current_job, self.__manager_host,
                        self.__manager_port)
            LOGGER.debug("Finished map task:%s, cleaned temp directory: %s",
                         task_id, temp_dir)
            # Send a finished message to the manager
            network.attempt_message(self, LOGGER,
                                    subject='Map task finished',
                                    message_type=MessageType.FINISHED,
                                    dest_host=self.__manager_host,
                                    dest_port=self.__manager_port,
                                    task_id=task_id,
                                    worker_host=self.host,
                                    worker_port=self.port)

    def do_reduce_task(self, task_id: int, destination: Path,
                       input_paths: List[str], executable: Path):
        """Execute a Reduce task."""
        # Create a temporary directory for the reduce task
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{task_id:05d}-"
        ) as temp_dir, ExitStack() as stack:
            LOGGER.debug("Created temporary directory %s for task:%s",
                         temp_dir, task_id)
            # LOGGER.info('Created temporary directory for task:%s',
            #             task_id)
            # Open the files for the reduce task
            task_files = [stack.enter_context(
                open(file, encoding="utf-8")) for file in input_paths
            ]
            # Create a temporary file for the reduce task
            output_file = \
                Path(f"{temp_dir}/part-{task_id:05d}")
            # Run the reduce task on the files
            with open(output_file, "w", encoding='utf-8') as file:
                # Create a subprocess to run the reduce task
                with subprocess.Popen(
                    [executable],
                    stdin=subprocess.PIPE,
                    stdout=file,
                    stderr=subprocess.PIPE,
                ) as process:
                    # Catch errors from the subprocess

                    # Merge the files into a single stream
                    for line in heapq.merge(*task_files):
                        # Write the line to the subprocess
                        process.stdin.write(line.encode("utf-8"))
                    # Close the input stream
                    process.stdin.close()
                # Move the output file to the destination directory
                LOGGER.debug("Moving %s to %s", output_file, destination)
                # LOGGER.info('Moving completed reduce task'
                #             ' to destination directory')
                shutil.move(str(output_file), str(destination))

            LOGGER.info("finished reduce task %s for job %s (mngr=%s:%i)",
                        task_id, self.__current_job, self.__manager_host,
                        self.__manager_port)
            LOGGER.debug("Finished reduce task %s and cleaned temp dir %s",
                         task_id, temp_dir)

            network.attempt_message(self, LOGGER,
                                    subject='Reduce task finished',
                                    message_type=MessageType.FINISHED,
                                    dest_host=self.__manager_host,
                                    dest_port=self.__manager_port,
                                    task_id=task_id,
                                    worker_host=self.host,
                                    worker_port=self.port)

    def __get_partition_number(self, line: str, partitions: int,
                               key_index: int = 0,
                               delimiter: str = "\t") -> int:
        """Determine the partition number for a given output line."""
        fields = line.split(delimiter)
        key = fields[key_index]
        keyhash = int(hashlib.md5(key.encode("utf-8")).hexdigest(), base=16)
        partition = keyhash % partitions
        return partition

    def __sort_lines(self, temp_files: List[BinaryIO],
                     chunk_size: int = 1024 * 1024):
        """Sorts the lines of each partition file in a directory."""
        # Sort the lines of each partition file
        for temp_file in temp_files:
            temp_file.seek(0)
            # Create a new temporary file for the sorted data
            with open(f"{temp_file.name}.sorted", "w+", encoding='utf-8') \
                    as sorted_temp_file:
                # Read, sort and write the file in chunks
                for lines in read_lines_in_chunks(temp_file, chunk_size):
                    lines.sort()
                    sorted_temp_file.writelines(line + '\n' for line in lines)

                # Close and replace the original file with the sorted file
                temp_file.close()
                sorted_temp_file.close()
                os.remove(temp_file.name)
                os.rename(sorted_temp_file.name, temp_file.name)


def read_lines_in_chunks(file: BinaryIO, chunk_size: int = 1024 * 1024):
    """Read a file in chunks and yields the lines in each chunk."""
    buffer = file.read(chunk_size)
    while buffer:
        # Ensure the buffer ends at a line boundary
        last_newline = buffer.rfind('\n')
        if last_newline != -1:
            buffer = buffer[:last_newline + 1]
        yield buffer.splitlines()
        buffer = file.read(chunk_size)