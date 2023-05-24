"""
MapReduce job submission script.

Before using this script, start the MapReduce server.
$ ./bin/mapreduce start

Then, submit a job.  Everything has a default.
$ mapreduce-submit

You can change any of the options.
$ mapreduce-submit --help
"""

import socket
import json
import click


# Configure command line options
@click.command()
@click.option(
    "--host", "-h", "host", default="localhost",
    help="Manager host, default=localhost",
)
@click.option(
    "--port", "-p", "port", default=6000,
    help="Manager port number, default=6000",
)
@click.option(
    "--input", "-i", "input_directory", default="tests/testdata/input",
    help="Input directory, default=tests/testdata/input",
    type=click.Path(file_okay=False, dir_okay=True),
)
@click.option(
    "--output", "-o", "output_directory", default="output",
    help="Output directory, default=output",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
)
@click.option(
    "--source", "-m", "source_path",
    default="ENTER A DEFAULT FILE HERE",
    help="Source path, default=_________",
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option(
    "--prompt", "-r", "prompt_path",
    default="ENTER A DEFAULT FILE HERE",
    help="Prompt path, default=_________",
    type=click.Path(file_okay=True, dir_okay=False),
)
@click.option(
    "--nworkers", "num_workers", default=2, type=int,
    help="Number of workers, default=2",
)
# @click.option(
#     "--nreducers", "num_reducers", default=2, type=int,
#     help="Number of reducers, default=2",
# )
def main(host: str,
         port: int,
         input_directory: str,
         output_directory: str,
         source_path: str,
         prompt_path: str,
         num_workers: int) -> None:
    """Top level command line interface."""
    # We want a bunch of arguments, this is the top level CLI.
    # pylint: disable=too-many-arguments
    job_dict = {
        "message_type": "new_manager_job",
        "input_directory": input_directory,
        "output_directory": output_directory,
        "source_path": source_path,
        "prompt_path": prompt_path,
        "num_workers": num_workers
    }

    # Send the data to the port that Manager is on
    message = json.dumps(job_dict)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(str.encode(message))

    except socket.error as err:
        print("Failed to send job to Manager.")
        print(err)

    # Print to CLI
    print(f"Submitted job to Manager {host}:{port}")
    print("input directory     ", input_directory)
    print("output directory    ", output_directory)
    print("source path   ", source_path)
    print("prompt path  ", prompt_path)
    print("num workers        ", num_workers)


if __name__ == "__main__":
    # Click will provide the arguments, disable this pylint check.
    # pylint: disable=no-value-for-parameter
    main()