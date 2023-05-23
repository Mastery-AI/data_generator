import json
import logging
import pathlib
import socket

from mapreduce.utils.messages import MessageArgs, MessageType
from mapreduce.utils.node import Node


# Make a json.JSONEncoder subclass that handles pathlib.Path objects
class _PathEncoder(json.JSONEncoder):
    """JSON encoder for pathlib.Path objects."""

    def default(self, o):
        """Return a string representation of the object."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return json.JSONEncoder.default(self, o)


def run_main_thread(node: Node, logger: logging.Logger):
    """Create a standard TCP socket for the supplicant.

    This function is used by both the Manager and the Worker,
    and it will begin to listen for incoming connections.
    """
    # Use context manager for the socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Bind the socket to the supplicant's host and port
        sock.bind((node.host, node.port))
        # Don't require the socket to wait for a timeout before reusing
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Listen for incoming connections
        sock.listen()
        # Socket accept() will block for a maximum of 1 second.  If you
        # omit this, it blocks indefinitely, waiting for a connection.
        sock.settimeout(1)
        # Log the supplicant's host and port
        logger.info("new %s listening on %s:%i/tcp",
                    node.type.value, node.host, node.port)
        # Start listening for incoming connections,
        # and wait for a shutdown signal
        while not node.wants_shutdown:
            # Wait for a connection for 1s.  The socket library avoids
            # consuming CPU while waiting for a connection.
            try:
                clientsocket, address = sock.accept()
            except socket.timeout:
                continue
            # Socket recv() will block for a maximum of 1 second.  If you omit
            # this, it blocks indefinitely, waiting for packets.
            clientsocket.settimeout(1)
            # Receive data from the client, one chunk at a time.  If recv()
            # times out before we can read a chunk, then go back to the top of
            # the loop and try again.  When the client closes the connection,
            # recv() returns empty data, which breaks out of the loop.  We make
            # a simplifying assumption that the client will always cleanly
            # close the connection.
            with clientsocket:
                message_chunks = []  # List of bytes
                while True:
                    try:
                        data = clientsocket.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    message_chunks.append(data)

            # Message has been received, now convert it to json
            raw_message = b"".join(message_chunks)
            raw_message_str = raw_message.decode("utf-8")
            # Try to convert the message to json
            try:
                message: dict = json.loads(raw_message_str)
            except json.JSONDecodeError:
                # Log the error TODO do we want to log this?
                # logger.error("Could not decode message: %s", raw_message_str)
                # Skip the rest of the loop
                continue

            # Log the message and process it
            logger.debug("%s=%s:%i recieved message from %s -> \n%s",
                         node.type.value, node.host, node.port,
                         get_hostname(address[0]),
                         json.dumps(message, indent=2))
            node.process_message(message)

        # Log that the supplicant is shutting down
        logger.info("%s=%s:%i main thread shutting down.",
                    node.type.value, node.host, node.port)


def __send_message(node: Node, logger: logging.Logger,
                   dest_host: str, dest_port: int,
                   message: dict,
                   subject: str) -> bool:
    """Send a message to a node using TCP."""
    logger.debug("Sending %s message to %s:%i",
                 subject, dest_host, dest_port)
    try:
        # Create a TCP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the node
            sock.connect((dest_host, dest_port))
            # Encode the message as json
            message_str = json.dumps(message, cls=_PathEncoder)
            message_bytes = message_str.encode("utf-8")
            # Send the message
            sock.sendall(message_bytes)
            # Log the message
            # logger.debug("%s=%s:%i sent message to %s -> \n%s",
            #              node.type.value, node.host, node.port,
            #              f"{dest_host}:{dest_port}",
            #              json.dumps(message, indent=2))
    except ConnectionRefusedError:
        # Log the error
        msg = f"Could not connect to {node.type.value}={node.host}:{node.port}"
        logger.error(msg)
        return False
    # Return True if the message was sent successfully
    return True


def attempt_message(sender: Node, logger: logging.Logger,
                    message_type: MessageType,
                    dest_host: str, dest_port: int,
                    subject: str,
                    **kwargs) -> bool:
    """Attempt to send a message of the specified type to the given node.

    Args:
        sender: The Node object representing the node sending the message.
        message_type: The MessageType enum value representing
                        the type of message to send.
        **kwargs: Any additional keyword arguments required
                        for the specific message type.

    Returns:
        A boolean indicating whether the message was successfully sent.

    Raises:
        ValueError: If any of the required keyword arguments are missing
                    or have an invalid type.
    """
    # Validate the input arguments
    required_args: dict = MessageArgs[message_type]
    for a_name, a_type in required_args.items():
        if a_name not in kwargs or not isinstance(kwargs[a_name], a_type):
            raise ValueError((f"Invalid argument {a_name} or type {a_type}"
                             f"for message type {message_type}"))

    # Craft the message dictionary
    message_dict = {"message_type": message_type.value, **kwargs}

    # Send the message
    return __send_message(sender, logger, dest_host, dest_port, message_dict,
                          subject)


def get_hostname(host: str):
    """Get the hostname of a node."""
    if host == "127.0.0.1":
        return "localhost"
    return host