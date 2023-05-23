"""A node in the Data Generator network."""


from dataclasses import dataclass
from enum import Enum
from abc import abstractmethod


class NodeType(Enum):
    """The type of node.

    Note that this implementation only supports Manager and Worker nodes.
    """

    MANAGER = 'Manager'
    WORKER = 'Worker'


@dataclass
class Node:
    """A Node makes a request to start some server on a network.

    In the case of the MapReduce framework, a Node is a Manager or Worker.
    """

    type: NodeType  # Manager or Worker
    host: str  # Hostname
    port: int  # Port number
    wants_shutdown: bool  # If the node needs the server to shutdown

    @abstractmethod
    def __str__(self):
        """Return a string representation of the node."""

    @abstractmethod
    def process_message(self, message_dict: dict):
        """Handle a message for a node."""
