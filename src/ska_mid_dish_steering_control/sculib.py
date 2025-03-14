"""
Steering Control Unit (SCU) for a SKA-Mid Dish Structure Controller OPC UA server.

This module contains an OPC UA client class that simplifies connecting to a server and
calling methods on it, reading or writing attributes.

How to use SCU
--------------

The simplest way to initialise a ``SteeringControlUnit``, is to use the ``SCU()``
object generator method. It creates an instance, connects to the server, and can also
take command authority immediately. Provided here are some of the defaults which can be
overwritten by specifying the named parameter:

.. code-block:: python

    from ska_mid_dish_steering_control import SCU
    scu = SCU(
        host="localhost",
        port=4840,
        endpoint="",
        namespace="",
        timeout=10.0,
        authority_name="LMC", # Default is None - then take_authority() must be used.
    )
    # Do things with the scu instance..
    scu.disconnect_and_cleanup()

Altenatively the ``SteeringControlUnit`` class can be used directly as a context
manager without calling the teardown method explicitly:

.. code-block:: python

    from ska_mid_dish_steering_control import SteeringControlUnit
    with SteeringControlUnit(host="localhost") as scu:
        scu.take_authority("LMC")

All nodes from and including the Server node are stored in the nodes dictionary:
``scu.nodes``. The keys are the full node names, the values are the Node objects.
The full names of all nodes can be retrieved with:

.. code-block:: python

    scu.get_node_list()

Every value in ``scu.nodes`` exposes the full OPC UA functionality for a node.
Note: When accessing nodes directly, it is mandatory to await any calls:

.. code-block:: python

    node = scu.nodes["Logic.Application.PLC_PRG"]
    node_name = (await node.read_display_name()).Text

The command methods that are below the PLC_PRG node's hierarchy can be accessed through
the commands dictionary:

.. code-block:: python

    scu.get_command_list()

When you want to call a command, please check the ICD for the parameters that the
commands expects. Checking for the correctness of the parameters is not done here
in the SCU class, but in the PLC's OPC UA server. Once the parameters are in order,
calling a command is really simple:

.. code-block:: python

    result = scu.commands["COMMAND_NAME"](YOUR_PARAMETERS)

You can also use the ``Command`` enum, as well as the helper method for converting types
from the OPC UA server to the correct base integer type:

.. code-block:: python

    from ska_mid_dish_steering_control.constants import Command
    axis = scu.convert_enum_to_int("AxisSelectType", "Az")
    result = scu.commands[Command.ACTIVATE.value](axis)

For instance, command the PLC to slew to a new position:

.. code-block:: python

    az = 182.0; el = 21.8; az_v = 1.2; el_v = 2.1
    code, msg, _ = scu.commands[Command.SLEW2ABS_AZ_EL.value](az, el, az_v, el_v)

The OPC UA server also provides read-writeable and read-only variables, commonly
called in OPC UA "attributes". An attribute's value can easily be read:

.. code-block:: python

    scu.attributes["Azimuth.p_Set"].value

If an attribute is writable, then a simple assignment does the trick:


.. code-block:: python

    scu.attributes["Azimuth.p_Set"].value = 1.2345

In case an attribute is not writeable, the OPC UA server will report an error:

`*** Exception caught: User does not have permission to perform the requested operation.
(BadUserAccessDenied)`

"""

# pylint: disable=too-many-lines, broad-exception-caught, too-many-positional-arguments
import asyncio
import datetime
import json
import logging
import queue
import socket
import threading
import time
from concurrent.futures import Future
from enum import Enum, IntEnum
from functools import cached_property
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Final, Protocol, Type, TypedDict

from asyncua import Client, Node, ua
from asyncua.crypto.cert_gen import setup_self_signed_certificate
from asyncua.crypto.security_policies import SecurityPolicyBasic256
from cryptography.x509.oid import ExtendedKeyUsageOID
from packaging.version import InvalidVersion, Version
from typing_extensions import NotRequired

from . import utils
from .constants import USER_CACHE_DIR, CmdReturn, Command, ResultCode, __version__
from .static_pointing import StaticPointingModel
from .track_table import TrackTable

logger = logging.getLogger("ska-mid-ds-scu")

COMPATIBLE_CETC_SIM_VER: Final = Version("4.6")
SPM_SCHEMA_PATH: Final = Path(
    resources.files(__package__) / "schemas/ska-mid-dish-gpm.json"  # type: ignore
)

TOP_NODE: Final = "Server"
TOP_NODE_ID: Final = "i=2253"
SKIP_NODE_ID: Final = "ns=22;s=Trace"
SERVER_STATUS_ID: Final = "i=2256"
PLC_PRG: Final = "Logic.Application.PLC_PRG"
APP_STATE: Final = "Logic.Application.ApplicationState"
CURRENT_MODE: Final = "System.CurrentMode"


async def handle_exception(e: Exception, msg: str = "") -> None:
    """
    Handle and log an exception.

    :param e: The exception that was caught.
    """
    try:
        # pylint: disable:no-member
        e.add_note(msg)  # type: ignore
        logger.exception("*** Exception caught: %s", e)
    except AttributeError:
        logger.error("*** Exception caught: %s [context: %s]", e, msg)


class ROAttribute(Protocol):  # noqa: D101
    """Protocol type for an OPC UA read-only attribute."""

    # pylint: disable=missing-function-docstring
    @property
    def ua_node(self) -> Node:  # noqa: D102
        pass

    @property
    def value(self) -> Any:  # noqa: D102
        pass


class RWAttribute(Protocol):  # noqa: D101
    """Protocol type for an OPC UA read-write attribute."""

    # pylint: disable=missing-function-docstring
    @property
    def ua_node(self) -> Node:  # noqa: D102
        pass

    @property
    def value(self) -> Any:  # noqa: D102
        pass

    @value.setter
    def value(self, value: Any) -> None:
        pass


def create_rw_attribute(
    node: Node, event_loop: asyncio.AbstractEventLoop, node_name: str
) -> RWAttribute:
    """
    Create a read-write attribute for an OPC UA node.

    The opc_ua_rw_attribute class has the following methods:
    - value: A property that gets the value of the OPC UA node.
    - value.setter: A setter method that sets the value of the OPC UA node.

    :param node: The OPC UA node to create the attribute for.
    :param event_loop: The asyncio event loop to use for running coroutines.
    :param node_name: The name of the OPC UA node.
    :return: An instance of a read-write attribute for the given OPC UA node.
    :raises Exception: If an exception occurs during getting or setting the value.
    """

    class opc_ua_rw_attribute:  # noqa: N801
        # pylint: disable=too-few-public-methods,missing-class-docstring,invalid-name
        # pylint: disable=missing-function-docstring

        @property
        def ua_node(self) -> Node:
            return node

        @property
        def value(self) -> Any:
            try:
                return asyncio.run_coroutine_threadsafe(
                    node.get_value(), event_loop
                ).result()
            except Exception as e:
                msg = f"Failed to read value of node: {node_name}"
                asyncio.run_coroutine_threadsafe(handle_exception(e, msg), event_loop)
                return None

        @value.setter
        def value(self, _value: Any) -> None:
            try:
                asyncio.run_coroutine_threadsafe(
                    node.set_value(_value), event_loop
                ).result()
            except Exception as e:
                msg = f"Failed to write value of node: {node_name}={_value}"
                asyncio.run_coroutine_threadsafe(handle_exception(e, msg), event_loop)

    return opc_ua_rw_attribute()


def create_ro_attribute(
    node: Node, event_loop: asyncio.AbstractEventLoop, node_name: str
) -> ROAttribute:
    """
    Create a read-only attribute for an OPC UA Node.

    :param node: The OPC UA Node from which to read the attribute.
    :param event_loop: The asyncio event loop to use.
    :param node_name: The name of the OPC UA node.
    :return: An object with a read-only 'value' property.
    :raises Exception: If an error occurs while getting the value from the OPC UA Node.
    """

    class opc_ua_ro_attribute:  # noqa: N801
        # pylint: disable=too-few-public-methods,missing-class-docstring,invalid-name
        # pylint: disable=missing-function-docstring
        @property
        def ua_node(self) -> Node:
            return node

        @property
        def value(self) -> Any:
            try:
                return asyncio.run_coroutine_threadsafe(
                    node.get_value(), event_loop
                ).result()
            except Exception as e:
                msg = f"Failed to read value of node: {node_name}"
                asyncio.run_coroutine_threadsafe(handle_exception(e, msg), event_loop)
                return None

    return opc_ua_ro_attribute()


# pylint: disable=too-few-public-methods
class SubscriptionHandler:
    """
    A class representing a Subscription Handler.

    :param subscription_queue: The queue to store subscription notifications.
    :param nodes: A dictionary mapping nodes to their names.
    """

    def __init__(
        self,
        subscription_queue: queue.Queue,
        nodes: dict[Node, str],
        bad_shutdown_callback: Callable[[str], None] | None = None,
    ) -> None:
        """
        Initialize the object with a subscription queue and nodes.

        :param subscription_queue: A queue for subscriptions.
        :param nodes: A dictionary of nodes.
        :param bad_shutdown_callback: will be called if a BadShutdown subscription
            status notification is received, defaults to None.
        """
        self.subscription_queue = subscription_queue
        self.nodes = nodes
        self.bad_shutdown_callback = bad_shutdown_callback

    def datachange_notification(
        self, node: Node, value: Any, data: ua.DataChangeNotification
    ) -> None:
        """
        Callback for an asyncua subscription.

        This method will be called when an asyncua.Client receives a data change message
        from an OPC UA server.
        """
        name = self.nodes[node]
        source_timestamp = data.monitored_item.Value.SourceTimestamp
        server_timestamp = data.monitored_item.Value.ServerTimestamp.timestamp()
        value_for_queue = {
            "name": name,
            "node": node,
            "value": value,
            "source_timestamp": source_timestamp,
            "server_timestamp": server_timestamp,
            "data": data,
        }
        self.subscription_queue.put(value_for_queue, block=True, timeout=0.1)

    def status_change_notification(self, status: ua.StatusChangeNotification) -> None:
        """Callback for every status change notification from server."""
        try:
            status.Status.check()  # Raises an exception if the status code is bad.
            logger.info(
                "Received status change notification: %s",
                status.Status.name,
            )
        except ua.UaStatusCodeError as e:
            logger.error(
                "Received bad status change notification: %s",
                e,
            )
            if status.Status.value == ua.StatusCodes.BadShutdown:
                self.bad_shutdown_callback(
                    f"Connection is closed - asyncua exception: {e}"
                )


class CachedNodeDetails(TypedDict):
    """Inidividual cached node details."""

    opcua_node_str: str
    node_class: int
    attribute_type: NotRequired[list[str]]


class CachedNodesDict(TypedDict):
    """Cached nodes dictionary type."""

    node_ids: dict[str, CachedNodeDetails]
    timestamp: str


# Type aliases
NodeDict = dict[str, tuple[Node, ua.NodeClass]]
AttrDict = dict[str, ROAttribute | RWAttribute]
CmdDict = dict[str, Callable[..., CmdReturn]]


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class SteeringControlUnit:
    """
    Steering Control Unit for a SKA-Mid Dish Structure Controller OPC UA server.

    An OPC UA client class that simplifies connecting to a server and calling
    methods on it, reading or writing attributes.

    :param host: The hostname or IP address of the server. Default is 'localhost'.
    :param port: The port number of the server. Default is 4840.
    :param endpoint: The endpoint on the server. Default is ''.
    :param namespace: The namespace for the server. Default is ''.
    :param username: The username for authentication. Default is None.
    :param password: The password for authentication. Default is None.
    :param timeout: The timeout value for connection. Default is 10.0.
    :param eventloop: The asyncio event loop to use. Default is None.
    :param scan_parameter_node: Whether to scan the 'Parameter' tree. Default is False.
    :param use_nodes_cache: Whether to use any existing caches of node IDs.
        Default is False.
    :param nodes_cache_dir: Directory where to save the cache and load it from.
        Default is the user cache directory determined with platformdirs.
    :param app_name: application name for OPC UA client description.
        Default 'SKA-Mid-DS-SCU v{__version__}'
    """

    # ------------------
    # Setup and teardown
    # ------------------
    # pylint: disable=too-many-arguments,too-many-locals
    def __init__(
        self,
        host: str = "localhost",
        port: int = 4840,
        endpoint: str = "",
        namespace: str = "",
        username: str | None = None,
        password: str | None = None,
        timeout: float = 10.0,
        eventloop: asyncio.AbstractEventLoop | None = None,
        scan_parameter_node: bool = False,
        use_nodes_cache: bool = False,
        nodes_cache_dir: Path = USER_CACHE_DIR,
        app_name: str = f"SKA-Mid-DS-SCU v{__version__}",
    ) -> None:
        """Initialise SCU with the provided parameters."""
        logger.info("Initialising SCU")
        # Intialised variables
        self.host = host
        self.port = port
        self.endpoint = endpoint
        self.namespace = namespace
        self.username = username
        self.password = password
        self.timeout = timeout
        if eventloop is None:
            self._create_and_start_asyncio_event_loop()
        else:
            self.event_loop = eventloop
        logger.info("Event loop: %s", self.event_loop)
        self._scan_parameter_node = scan_parameter_node
        self._use_nodes_cache = use_nodes_cache
        self._nodes_cache_dir = nodes_cache_dir
        self._app_name = app_name
        # Other local variables
        self._client: Client | None = None
        self._event_loop_thread: threading.Thread | None = None
        self._subscriptions: dict = {}
        self._subscription_queue: queue.Queue = queue.Queue()
        self._user = ua.UInt16(0)  # NoAuthority
        self._session_id = ua.UInt16(0)
        self._server_url: str = ""
        self._server_str_id: str = ""
        self._ns_idx: int = 2  # Default is first physical controller
        self._nodes: NodeDict = {}
        self._nodes_reversed: dict[Node, str] = {}
        self._attributes: AttrDict = {}
        self._commands: CmdDict = {}
        self._nodes_timestamp: str = ""
        self._parameter_ns_idx: int | None = None
        self._parameter_nodes: NodeDict = {}
        self._parameter_attributes: AttrDict = {}
        self._track_table_queue: queue.Queue | None = None
        self._stop_track_table_schedule_task_event: threading.Event | None = None
        self._track_table_scheduled_task: Future | None = None
        self._track_table: TrackTable | None = None
        self._opcua_enum_types: dict[str, Type[IntEnum]] = {}
        self._static_pointing: dict[str, StaticPointingModel] = {}
        self._attribute_types_cache: dict[str, list[str]] = {}

    def connect_and_setup(self) -> None:
        """
        Connect to the server and setup the SCU client.

        :raises Exception: If connection to OPC UA server fails.
        """
        try:
            self._client = self.connect()
        except Exception as e:
            msg = (
                "Cannot connect to the OPC UA server. Please "
                "check the connection parameters that were "
                "passed to instantiate the sculib!"
            )
            try:
                # pylint: disable:no-member
                e.add_note(msg)  # type: ignore
            except AttributeError:
                logger.error(msg)
            raise e
        if self.server_version is None:
            self._server_str_id = f"{self._server_url} - version unknown"
        else:
            self._server_str_id = f"{self._server_url} - v{self.server_version}"
            try:
                if (
                    self.namespace == "CETC54"
                    and Version(self.server_version) < COMPATIBLE_CETC_SIM_VER
                ):
                    raise RuntimeError(
                        f"SCU not compatible with v{self.server_version} of CETC "
                        f"simulator, only v{COMPATIBLE_CETC_SIM_VER} and up"
                    )
            except InvalidVersion:
                logger.warning(
                    "Server version (%s) does not conform to semantic versioning",
                    self.server_version,
                )
        self._populate_node_dicts()
        self._validate_enum_types()  # Ensures enum types are defined
        logger.info("Successfully connected to server and initialised SCU client")

    def __enter__(self) -> "SteeringControlUnit":
        """Connect to the server and setup the SCU client."""
        self.connect_and_setup()
        return self

    def disconnect_and_cleanup(self) -> None:
        """
        Disconnect from server and clean up SCU client resources.

        Release any command authority, unsubscribe from all subscriptions, disconnect
        from the server, and stop the event loop if it was started in a separate thread.
        """
        self.release_authority()
        self.unsubscribe_all()
        self.disconnect()
        # Store any updates to attribute data types.
        self._cache_node_ids(self._nodes_cache_dir / f"{TOP_NODE}.json", self._nodes)
        self.cleanup_resources()

    def cleanup_resources(self) -> None:
        """
        Clean up SCU client resources.

        Stop the event loop if it was started in a separate thread.
        """
        if self._event_loop_thread is not None:
            # Signal the event loop thread to stop.
            self.event_loop.call_soon_threadsafe(self.event_loop.stop)
            # Join the event loop thread once it is done processing tasks.
            self._event_loop_thread.join()

    def __exit__(self, *args: Any) -> None:
        """Disconnect from server and clean up client resources."""
        self.disconnect_and_cleanup()

    def _run_event_loop(
        self,
        event_loop: asyncio.AbstractEventLoop,
        thread_started_event: threading.Event,
    ) -> None:
        # The self.event_loop needs to be stored here. Otherwise asyncio
        # complains that it has the wrong type when scheduling a coroutine.
        # Sigh.
        """
        Run the event loop using the specified event loop and thread started event.

        :param event_loop: The asyncio event loop to run.
        :param thread_started_event: The threading event signaling that the thread has
            started.
        """
        self.event_loop = event_loop
        asyncio.set_event_loop(event_loop)
        thread_started_event.set()  # Signal that the event loop thread has started
        event_loop.run_forever()

    def _create_and_start_asyncio_event_loop(self) -> None:
        """
        Create and start an asyncio event loop in a separate thread.

        This function creates a new asyncio event loop, starts it in a separate thread,
        and waits for the thread to start.
        """
        event_loop = asyncio.new_event_loop()
        thread_started_event = threading.Event()
        self._event_loop_thread = threading.Thread(
            target=self._run_event_loop,
            args=(event_loop, thread_started_event),
            name=f"asyncio event loop for sculib instance {self.__class__.__name__}",
            daemon=True,
        )
        self._event_loop_thread.start()
        thread_started_event.wait(5.0)  # Wait for the event loop thread to start

    # --------------------------
    # Client connection handling
    # --------------------------
    def _set_up_encryption(self, client: Client, user: str, pw: str) -> None:
        """
        Set up encryption for the client with the given user credentials.

        :param client: The client to set up encryption for.
        :param user: The username for the server.
        :param pw: The password for the server.
        """
        # this is generated if it does not exist
        opcua_client_key = Path(str(resources.files(__package__) / "certs/key.pem"))
        # this is generated if it does not exist
        opcua_client_cert = Path(str(resources.files(__package__) / "certs/cert.der"))
        # get from simulator/PKI/private/SimpleServer_2048.der in tarball
        opcua_server_cert = Path(
            str(resources.files(__package__) / "certs/SimpleServer_2048.der")
        )
        client.set_user(user)
        client.set_password(pw)

        _ = asyncio.run_coroutine_threadsafe(
            setup_self_signed_certificate(
                key_file=opcua_client_key,
                cert_file=opcua_client_cert,
                app_uri=client.application_uri,
                host_name="localhost",
                cert_use=[ExtendedKeyUsageOID.CLIENT_AUTH],
                subject_attrs={
                    "countryName": "ZA",
                    "stateOrProvinceName": "Western Cape",
                    "localityName": "Cape Town",
                    "organizationName": "SKAO",
                },
            ),
            self.event_loop,
        ).result()
        _ = asyncio.run_coroutine_threadsafe(
            client.set_security(
                SecurityPolicyBasic256,
                certificate=str(opcua_client_cert),
                private_key=str(opcua_client_key),
                server_certificate=str(opcua_server_cert),
                mode=ua.MessageSecurityMode.Sign,
            ),
            self.event_loop,
        ).result()

    def connect(self) -> Client:
        """
        Connect to an OPC UA server.

        :raises: Exception if an error occurs during the connection process.
        """
        server_url = f"opc.tcp://{self.host}:{self.port}{self.endpoint}"
        logger.info("Connecting to: %s", server_url)
        client = Client(server_url, self.timeout)
        hostname = socket.gethostname()
        # Set the ClientDescription fields
        client.application_uri = f"urn:{hostname}:{self._app_name.replace(' ', '-')}"
        client.product_uri = "gitlab.com/ska-telescope/ska-mid-dish-qualification"
        client.name = f"{self._app_name} @{hostname}"
        client.description = f"{self._app_name} @{hostname}"
        if self.username is not None and self.password is not None:
            self._set_up_encryption(client, self.username, self.password)
        _ = asyncio.run_coroutine_threadsafe(client.connect(), self.event_loop).result()
        self._server_url = server_url
        try:
            _ = asyncio.run_coroutine_threadsafe(
                client.load_data_type_definitions(), self.event_loop
            ).result()
        except Exception as exc:
            logger.warning("Exception trying load_data_type_definitions(): %s", exc)
        # Get the namespace index for the PLC's Parameter node
        try:
            self._parameter_ns_idx = asyncio.run_coroutine_threadsafe(
                client.get_namespace_index(
                    "http://boschrexroth.com/OpcUa/Parameter/Objects/"
                ),
                self.event_loop,
            ).result()
        except Exception:
            self._parameter_ns_idx = None
            message = (
                "*** Exception caught while trying to access the namespace "
                "'http://boschrexroth.com/OpcUa/Parameter/Objects/' for the parameter "
                "nodes on the OPC UA server. "
                "It will be assumed that the CETC54 simulator is running."
            )
            logger.warning(message)

        try:
            if self.namespace != "" and self.endpoint != "":
                self._ns_idx = asyncio.run_coroutine_threadsafe(
                    client.get_namespace_index(self.namespace), self.event_loop
                ).result()
            else:
                # Force namespace index for first physical controller
                self._ns_idx = 2
        except ValueError as e:
            namespaces = None
            try:
                namespaces = asyncio.run_coroutine_threadsafe(
                    client.get_namespace_array(), self.event_loop
                ).result()
            except Exception:
                pass
            try:
                self.disconnect(client)
            except Exception:
                pass
            message = (
                "*** Exception caught while trying to access the requested "
                f"namespace '{self.namespace}' on the OPC UA server. Will NOT continue"
                " with the normal operation but list the available namespaces here for "
                f"future reference:\n{namespaces}"
            )
            try:
                # pylint: disable:no-member
                e.add_note(message)  # type: ignore
            except AttributeError:
                logger.error(message)
            raise e
        return client

    def disconnect(self, client: Client | None = None) -> None:
        """
        Disconnect a client connection.

        :param client: The client to disconnect. If None, disconnect self.client.
        """
        if client is None and self._client is not None:
            client = self._client
            self._client = None
        if client is not None:
            _ = asyncio.run_coroutine_threadsafe(
                client.disconnect(), self.event_loop
            ).result()

    def connection_reset(self) -> None:
        """Reset the client by disconnecting and reconnecting."""
        self.disconnect()
        self.connect()

    def is_connected(self) -> bool:
        """
        Check if the SCU is connected to an OPC-UA server.

        :return: True if the SCU has a connection, False otherwise.
        """
        return self._client is not None

    # ----------------
    # Class properties
    # ----------------
    @cached_property
    def server_version(self) -> str | None:
        """
        The software/firmware version of the server that SCU is connected to.

        Returns None if the server version info could not be read successfully.
        """
        try:
            version_node = asyncio.run_coroutine_threadsafe(
                self._client.nodes.objects.get_child(
                    [
                        f"{self._ns_idx}:Logic",
                        f"{self._ns_idx}:Application",
                        f"{self._ns_idx}:PLC_PRG",
                        f"{self._ns_idx}:Management",
                        f"{self._ns_idx}:NamePlate",
                        f"{self._ns_idx}:DscSoftwareVersion",
                    ]
                ),
                self.event_loop,
            ).result()
            server_version: str = asyncio.run_coroutine_threadsafe(
                version_node.get_value(), self.event_loop
            ).result()
        except Exception as e:
            msg = "Failed to read value of DscSoftwareVersion attribute."
            asyncio.run_coroutine_threadsafe(handle_exception(e, msg), self.event_loop)
            server_version = None
        return server_version

    @property
    def nodes(self) -> NodeDict:
        """Dictionary of the 'Server' node's entire tree of `asyncua.Node` objects."""
        return self._nodes

    @property
    def attributes(self) -> AttrDict:
        """
        Dictionary containing the attributes in the 'Server' node tree.

        The values are callables that return the current value.
        """
        return self._attributes

    @property
    def commands(self) -> CmdDict:
        """
        Dictionary containing command methods in the 'Server' node tree.

        The values are callables which require the expected parameters.
        """
        return self._commands

    @property
    def nodes_timestamp(self) -> str:
        """
        Generation timestamp of the 'Server' nodes.

        The timestamp is in 'yyyy-mm-dd hh:mm:ss' string format.
        """
        return self._nodes_timestamp

    @property
    def parameter_nodes(self) -> NodeDict:
        """Dictionary containing the parameter nodes of the PLC program."""
        return self._parameter_nodes

    @property
    def parameter_attributes(self) -> AttrDict:
        """
        Dictionary containing the attributes of the parameter nodes in the PLC program.

        The values are callables that return the current value.
        """
        return self._parameter_attributes

    @property
    def opcua_enum_types(self) -> dict[str, Type[IntEnum]]:
        """
        Dictionary mapping OPC-UA enum type names to their corresponding value.

        The value being an enumerated type.
        """
        return self._opcua_enum_types

    def _validate_enum_types(self) -> None:
        """
        Validate the DSC's OPC-UA enum types.

        Check if the expected OPC-UA enum types are defined after connecting to the
        server, and if not, try to manually retrieve them by their node ID and set the
        corresponding ua attributes. Add all found enum types and their values to the
        'opcua_enum_types' dictionary property.
        """
        missing_types = []
        expected_types = [
            "AxisSelectType",
            "AxisStateType",
            "BandType",
            "CmdResponseType",
            "DscCmdAuthorityType",
            "DscStateType",
            "DscTimeSyncSourceType",
            "InterpolType",
            "LoadModeType",
            "SafetyStateType",
            "StowPinStatusType",
            "TiltOnType",
        ]
        for type_name in expected_types:
            try:
                self._opcua_enum_types.update({type_name: getattr(ua, type_name)})
            except AttributeError:
                try:
                    enum_node = self._client.get_node(
                        f"ns={self._ns_idx};s=@{type_name}.EnumValues"
                    )
                    new_enum = self._create_enum_from_node(type_name, enum_node)
                except ua.UaStatusCodeError:
                    try:  # For CETC sim v4.4
                        enum_node = self._client.get_node(
                            f"ns={self._ns_idx};s={type_name}.EnumStrings"
                        )
                        new_enum = self._create_enum_from_node(type_name, enum_node)
                    except (ua.UaStatusCodeError, ValueError):
                        missing_types.append(type_name)
                        continue
                except ValueError:
                    missing_types.append(type_name)
                    continue
                self._opcua_enum_types.update({type_name: new_enum})  # type: ignore
                setattr(ua, type_name, new_enum)
        if missing_types:
            logger.warning(
                "OPC-UA server does not implement the following Enumerated types "
                "as expected: %s",
                str(missing_types),
            )

    # ----------------------
    # Command user authority
    # ----------------------
    def take_authority(self, user: str | int) -> tuple[ResultCode, str]:
        """
        Take command authority.

        :param user: Authority user name - DscCmdAuthorityType enum.
        :return: The result of the command execution.
        """
        user_int = (
            self.convert_enum_to_int("DscCmdAuthorityType", user)
            if isinstance(user, str)
            else ua.UInt16(user)
        )
        code, msg, vals = self.commands[Command.TAKE_AUTH.value](user_int)
        if code == ResultCode.COMMAND_DONE:
            self._user = user_int
            self._session_id = ua.UInt16(vals[0])
        else:
            logger.error("TakeAuth command failed with message '%s'", msg)
        return code, msg

    def release_authority(self) -> tuple[ResultCode, str]:
        """
        Release command authority.

        :return: The result of the command execution.
        """
        code, msg, _ = self.commands[Command.RELEASE_AUTH.value](self._user)
        if code == ResultCode.COMMAND_DONE:
            self._session_id = ua.UInt16(0)
        elif code == ResultCode.NO_CMD_AUTH:
            user = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
            logger.info(
                "SCU lost command authority as user '%s' to another client.", user
            )
            self._session_id = ua.UInt16(0)
        return code, msg

    # ---------------
    # General methods
    # ---------------
    def convert_enum_to_int(self, enum_type: str, name: str) -> ua.UInt16 | None:
        """
        Convert the name (string) of the given enumeration type to an integer value.

        :param enum_type: the name of the enumeration type to use.
        :param name: of enum to convert.
        :return: enum integer value, or None if the type does not exist.
        """
        try:
            value = getattr(ua, enum_type)[name]
            integer = value.value if isinstance(value, IntEnum) else value
            return ua.UInt16(integer)
        except KeyError:
            logger.error("'%s' enum does not have '%s key!", enum_type, name)
            return None
        except AttributeError:
            logger.error("OPC-UA server has no '%s' enum!", enum_type)
            return None

    def convert_int_to_enum(self, enum_type: str, value: int) -> str | int:
        """
        Convert the integer value of the given enumeration type to its name (string).

        :param enum_type: the name of the enumeration type to use.
        :param value: of enum to convert.
        :return: enum name, or the original integer value if the type does not exist.
        """
        try:
            return (
                str(getattr(ua, enum_type)(value).name)
                if hasattr(ua, enum_type)
                else value
            )
        except ValueError:
            logger.error("%s is not a valid '%s' enum value!", value, enum_type)
            return value

    def _create_enum_from_node(self, name: str, node: Node) -> IntEnum:
        enum_values = asyncio.run_coroutine_threadsafe(
            node.get_value(), self.event_loop
        ).result()
        if not isinstance(enum_values, list):
            raise ValueError(f"Expected a list of EnumValueType for node '{name}'.")
        enum_dict = {}
        for i, value in enumerate(enum_values):
            try:
                display_name = value.DisplayName.Text
                enum_dict[display_name] = value.Value
            except AttributeError:  # For CETC sim v4.4
                display_name = value.Text
                enum_dict[display_name] = i
        return IntEnum(name, enum_dict)

    async def _create_command_function(
        self,
        node: Node,
        event_loop: asyncio.AbstractEventLoop,
        node_name: str,
    ) -> Callable | None:
        """
        Create a command function to execute a method on a specified Node.

        :param node: The Node on which the method will be executed.
        :param event_loop: The asyncio event loop to run the coroutine on.
        :param node_name: The full name of the Node.
        :return: A function that can be used to execute a method on the Node,
            or None if a call_method was not found.
        """
        try:
            node_parent = await node.get_parent()
            if hasattr(node_parent, "call_method"):
                node_call_method = node_parent.call_method
            else:
                node_call_method = node.call_method
        except AttributeError as e:
            logger.error(
                "Caught an exception while trying to get the method for a command.\n"
                "Exception: %s\nNode name: %s\nParent object: %s",
                e,
                node_name,
                node_parent,
            )
            return None

        node_id = node.nodeid

        # Determine if the node method is an application command that requires authority
        split_name = node_name.split(".")
        if "Commands" in split_name:
            short_name = f"{split_name[-3]}.{split_name[-2]}.{split_name[-1]}"
            requires_authority = (
                short_name in [c.value for c in Command]
                and short_name != Command.TAKE_AUTH.value
            )
        else:
            requires_authority = False

        # pylint: disable=protected-access
        def fn(*args: Any) -> CmdReturn:
            """
            Execute function with arguments and return tuple with return code/message.

            :param args: Optional positional arguments to pass to the function.
            :return: A tuple containing the return code (int), return message (str),
                and a list of other returned values (Any), if any, otherwise None.

            Any exception raised during the execution of the function will be handled
            and a tuple with return code and exception message returned.

            Note: This function uses asyncio to run the coroutine in a separate thread.
            """
            result_code = None
            result_output = None
            try:
                cmd_args = [self._session_id, *args] if requires_authority else [*args]
                logger.debug(
                    "Calling command node '%s' with args list: %s",
                    node_name,
                    cmd_args,
                )
                result: None | int | list[Any] = asyncio.run_coroutine_threadsafe(
                    node_call_method(node_id, *cmd_args), event_loop
                ).result()
                # Unpack result if it is a list
                if isinstance(result, list):
                    result_code_int = result.pop(0)
                    result_output = result
                else:
                    result_code_int = result
                result_code, result_msg = self._validate_command_result_code(
                    result_code_int
                )
                # Handle specific result codes
                if result_code == ResultCode.UNKNOWN:
                    logger.warning(
                        "SCU has received an unknown result code: %s - "
                        "Check server's CmdResponseType and ResultCode enum!",
                        result_code_int,
                    )
                elif result_code == ResultCode.NO_CMD_AUTH:
                    user = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
                    logger.info(
                        "SCU lost command authority as user '%s' to another client.",
                        user,
                    )
                    self._session_id = ua.UInt16(0)
            except ConnectionError as e:
                result_code = ResultCode.CONNECTION_CLOSED
                result_msg = f"asyncua exception: {str(e)}"
                asyncio.run_coroutine_threadsafe(
                    handle_exception(
                        e, f"Command: {node_id} ({node_name}), args: {args}"
                    ),
                    event_loop,
                )
            except ua.UaError as e:  # Base asyncua exception
                result_code = ResultCode.UA_BASE_EXCEPTION
                result_msg = f"asyncua exception: {str(e)}"
                asyncio.run_coroutine_threadsafe(
                    handle_exception(
                        e, f"Command: {node_id} ({node_name}), args: {args}"
                    ),
                    event_loop,
                )
            return result_code, result_msg, result_output

        return fn

    def _validate_command_result_code(self, result_code: int) -> tuple[ResultCode, str]:
        """
        Validate a command's result code and return a ResultCode enum and string value.

        :param result_code: Raw command result code integer
        :return: A tuple containing the ResultCode enum and message.
        """
        # Check for valid result code
        result_enum = (
            ResultCode(result_code)
            if result_code in ResultCode.__members__.values()
            else ResultCode.UNKNOWN
        )
        # Set result message
        if result_enum != ResultCode.UNKNOWN:
            result_msg = (
                ua.CmdResponseType(result_enum).name
                if hasattr(ua, "CmdResponseType")
                else result_enum.name
            )
        else:
            result_msg = result_enum.name
        return result_enum, result_msg

    # -----------------------------
    # Node dictionaries and caching
    # -----------------------------
    def _retreive_cached_data(
        self, file_path: Path
    ) -> dict[str, CachedNodesDict] | None:
        """
        Retrieve cached nodes from json.

        Checks whether the cache file is in the old format (node details as list) or in
        the new format (node details as dict). If the cache file is still the old
        format, the data is updated to the new format before being returned.
        """
        data: dict[str, CachedNodesDict] | None = utils.load_json_file(
            file_path
        )  # type: ignore
        # Check cache file format
        try:
            # Pop an item and return to check load success.
            a, b = data.popitem()
            data[a] = b
        except (AttributeError, KeyError):
            # No file or empty dict.
            return None

        c, test_details = data[a]["node_ids"].popitem()
        data[a]["node_ids"][c] = test_details
        if isinstance(test_details, list):
            # Old format, update file to use dicts for node details.
            for server_id, server_info in data.items():
                for scu_node_str, node_info in server_info["node_ids"].items():
                    node_class = node_info[1]
                    details_dict = {
                        "opcua_node_str": node_info[0],
                        "node_class": node_class,
                    }
                    if node_class == ua.NodeClass.Variable:
                        details_dict["attribute_type"] = ["Unknown"]

                    data[server_id]["node_ids"][scu_node_str] = details_dict

        return data

    def _cache_node_ids(self, file_path: Path, nodes: NodeDict) -> None:
        """
        Cache Node IDs.

        Create a dictionary of Node names with their unique Node ID and node class
        and write it to a new or existing JSON file with the OPC-UA server address
        as key.
        For nodes of type attribute, also add the data type if known.

        :param file_path: of JSON file to load.
        :param nodes: dictionary of Nodes to cache.
        """
        node_ids = {}
        for key, tup in nodes.items():
            try:
                node, node_class = tup
                if key != "":
                    node_ids[key] = {
                        "opcua_node_str": node.nodeid.to_string(),
                        "node_class": node_class,
                    }
                    if node_class == ua.NodeClass.Variable:
                        node_ids[key][
                            "attribute_type"
                        ] = self._get_cached_attribute_data_type(key)

            except TypeError as exc:
                logger.debug("TypeError with dict value %s: %s", tup, exc)
        cached_data: dict[str, CachedNodesDict] = (
            self._retreive_cached_data(file_path) or {}
        )
        cached_data[self._server_str_id] = {
            "node_ids": node_ids,  # type: ignore
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w+", encoding="UTF-8") as file:
            json.dump(cached_data, file, indent=4, sort_keys=True)

    def _populate_node_dicts(self) -> None:
        """
        Populate dictionaries with Node objects.

        This method scans the OPC-UA server tree and populates dictionaries with
        Node objects for different categories like nodes, attributes, and commands.

        This method may raise exceptions related to asyncio operations.
        """
        # Create node dicts of the top node's tree
        (
            self._nodes,
            self._attributes,
            self._commands,
            self._nodes_timestamp,
        ) = self._check_cache_and_generate_node_dicts(
            top_level_node=self._client.get_node(TOP_NODE_ID),
            top_level_node_name=TOP_NODE,
            skip_node_id=SKIP_NODE_ID,
        )
        # Check if the PLC_PRG node exists, and if not, scan it separately and add its
        # nodes, attributes, and commands to the dictionaries. (needed for simulators)
        if PLC_PRG not in self._nodes:
            plc_prg_node = asyncio.run_coroutine_threadsafe(
                self._client.nodes.objects.get_child(
                    [f"{self._ns_idx}:{node}" for node in PLC_PRG.split(".")]
                ),
                self.event_loop,
            ).result()
            (
                nodes,
                attributes,
                commands,
                _,  # timestamp
            ) = self._check_cache_and_generate_node_dicts(
                top_level_node=plc_prg_node,
                top_level_node_name="PLC_PRG",
            )
            self._nodes.update(nodes)
            self._attributes.update(attributes)
            self._commands.update(commands)
        self._nodes_reversed = {val[0]: key for key, val in self.nodes.items()}
        if APP_STATE not in self._nodes:
            self.add_attribute_to_dicts(APP_STATE)
        if CURRENT_MODE not in self._nodes:
            self.add_attribute_to_dicts(CURRENT_MODE)

        # Check if we also want the PLC's parameters for the drives and the PLC program.
        # But only if we are not connected to the simulator.
        top_node_name = "Parameter"
        if self._scan_parameter_node and self._parameter_ns_idx is not None:
            parameter_node = asyncio.run_coroutine_threadsafe(
                self._client.nodes.objects.get_child(
                    [f"{self._parameter_ns_idx}:{top_node_name}"]
                ),
                self.event_loop,
            ).result()
            (
                self._parameter_nodes,
                self._parameter_attributes,
                _,  # Parameter tree has no commands, so it is just an empty dict
                _,  # timestamp
            ) = self._check_cache_and_generate_node_dicts(parameter_node, top_node_name)

    def add_attribute_to_dicts(self, node_dot_notation_name: str) -> None:
        """
        Add an attribute (variable node) to the `nodes` and `attributes` dicts.

        :param node_dot_notation_name: full dot notation name (path) of the variable
            node under the `Objects`  node.
        """
        try:
            node = asyncio.run_coroutine_threadsafe(
                self._client.nodes.objects.get_child(
                    [
                        f"{self._ns_idx}:{node}"
                        for node in node_dot_notation_name.split(".")
                    ]
                ),
                self.event_loop,
            ).result()
        except ua.UaStatusCodeError as e:
            msg = f"Node not found at {node_dot_notation_name}"
            asyncio.run_coroutine_threadsafe(handle_exception(e, msg), self.event_loop)
            return
        self._nodes[node_dot_notation_name] = (node, ua.NodeClass.Variable)
        self._nodes_reversed[node] = node_dot_notation_name
        self._attributes[node_dot_notation_name] = create_rw_attribute(
            node, self.event_loop, node_dot_notation_name
        )

    def _check_cache_and_generate_node_dicts(
        self,
        top_level_node: Node,
        top_level_node_name: str,
        skip_node_id: str | None = None,
    ) -> tuple[NodeDict, AttrDict, CmdDict, str]:
        """
        Check for an existing nodes cache to use and generate the dicts accordingly.

        If the ``use_nodes_cache`` var is set, check if there is an existing cache file
        matching the top level node name, load the JSON and generate the node dicts from
        it. Otherwise generate the node dicts by scanning the node's tree on the server.

        :param top_level_node: The top-level node for which to generate dictionaries.
        :param top_level_node_name: Name of the top-level node.
        :param skip_node_id: Optional ID of a sub-node to skip when generating the
            dictionaries, defaults to None.
        :return: A tuple containing dictionaries for nodes, attributes, and commands, as
            well as a string timestamp of when the dicts were generated.
        """
        cache_file_path = self._nodes_cache_dir / f"{top_level_node_name}.json"
        cache = (
            self._retreive_cached_data(cache_file_path)
            if self._use_nodes_cache
            else None
        )
        cached_nodes: CachedNodesDict | None = (
            cache.get(self._server_str_id)  # type: ignore
            if cache is not None
            else None
        )
        # Check for existing Node IDs cache
        if cached_nodes:
            node_dicts = self._generate_node_dicts_from_cache(
                cached_nodes["node_ids"], top_level_node_name
            )
            timestamp = cached_nodes["timestamp"]
        else:
            node_dicts = self.generate_node_dicts_from_server(
                top_level_node, top_level_node_name, skip_node_id
            )
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._cache_node_ids(cache_file_path, node_dicts[0])
        return *node_dicts, timestamp

    def _generate_node_dicts_from_cache(
        self,
        cache_dict: dict[str, CachedNodeDetails],
        top_level_node_name: str,
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Generate dicts for nodes, attributes, and commands from a cache.

        Also retrieves attribute data type information.

        :param top_level_node_name: Name of the top-level node.
        :return: A tuple containing dictionaries for nodes, attributes and commands.
        """
        logger.info(
            "Generating dicts of '%s' node's tree from existing cache in %s",
            top_level_node_name,
            self._nodes_cache_dir,
        )
        nodes: NodeDict = {}
        attributes: AttrDict = {}
        commands: CmdDict = {}
        for node_name, node_details in cache_dict.items():
            node_id = node_details["opcua_node_str"]
            node_class = node_details["node_class"]
            node: Node = self._client.get_node(node_id)
            nodes[node_name] = (node, node_class)
            if node_class == ua.NodeClass.Variable:
                # An attribute. Add it to the attributes dict and retrieve the type.
                attributes[node_name] = create_rw_attribute(
                    node, self.event_loop, node_name
                )
                self._attribute_types_cache[node_name] = node_details["attribute_type"]
            elif node_class == ua.NodeClass.Method:
                # A command. Add it to the commands dict.
                command_method = asyncio.run_coroutine_threadsafe(
                    self._create_command_function(
                        node,
                        self.event_loop,
                        node_name,
                    ),
                    self.event_loop,
                ).result()
                if command_method:
                    commands[node_name] = command_method
        return (
            nodes,
            attributes,
            commands,
        )

    def generate_node_dicts_from_server(
        self,
        top_level_node: Node,
        top_level_node_name: str,
        skip_node_id: str | None = None,
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Generate dicts for nodes, attributes, and commands for a given top-level node.

        This function generates dictionaries for nodes, attributes, and
        commands based on the structure of the top-level node and returns a tuple
        containing these dictionaries.

        The dictionaries contain mappings of keys to nodes, attributes, and commands.

        :param top_level_node: The top-level node for which to generate dictionaries.
        :param top_level_node_name: Name of the top-level node.
        :param skip_node_id: Optional ID of a sub-node to skip when generating the
            dictionaries, defaults to None.
        :return: A tuple containing dictionaries for nodes, attributes, and commands.
        """
        logger.info(
            "Generating dicts of '%s' node's tree from server. It may take a while...",
            top_level_node_name,
        )
        nodes, attributes, commands = asyncio.run_coroutine_threadsafe(
            self._get_sub_nodes(top_level_node, top_level_node_name, skip_node_id),
            self.event_loop,
        ).result()
        nodes.update({top_level_node_name: (top_level_node, ua.NodeClass.Object)})
        return nodes, attributes, commands

    async def _generate_full_node_name(
        self,
        node: Node,
        top_level_node_name: str,
        parent_names: list[str] | None,
        node_class: ua.NodeClass,
        node_name_separator: str = ".",
    ) -> tuple[str, list[str]]:
        """
        Generate the full name of a node by combining its name with its parent names.

        :param node: The node for which the full name is generated.
        :param top_level_node_name: Name of the top-level node.
        :param parent_names: A list of parent node names. Defaults to None if the node
            has no parent.
        :param node_name_separator: The separator used to join the node and parent
            names. Default is '.'.
        :return: A tuple containing the full node name and a list of ancestor names.
        """
        try:
            name: str = node.nodeid.Identifier.split(node_name_separator)[-1]
        except AttributeError:
            browse_name = await node.read_browse_name()
            name = browse_name.Name

        if parent_names:
            parents = parent_names.copy()
        # Add the top node name as only parent for non-hierarchical direct children
        elif node_class in [ua.NodeClass.Variable, ua.NodeClass.Method]:
            parents = [TOP_NODE]
        else:
            parents = []
        if name != top_level_node_name:
            parents.append(name)

        try:
            node_name = node_name_separator.join(parents)
        except Exception:
            logger.exception("Invalid node for: %s", parents)
            return "", [""]

        # Remove the full PLC_PRG prefix from the node name for backwards compatiblity
        if f"{PLC_PRG}." in node_name:
            node_name = node_name.removeprefix(f"{PLC_PRG}.")
        return node_name, parents

    async def _get_sub_nodes(
        self,
        node: Node,
        top_level_node_name: str,
        skip_node_id: str | None = None,
        parent_names: list[str] | None = None,
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Retrieve sub-nodes, attributes, and commands of a given node.

        :param node: The node to retrieve sub-nodes from.
        :param top_level_node_name: Name of the top-level node.
        :param skip_node_id: Optional ID of a sub-node to skip when generating the
            dictionaries, defaults to None.
        :param parent_names: List of parent node names (default is None).
        :return: A tuple containing dictionaries of nodes, attributes, and commands.
        """
        if skip_node_id and skip_node_id == node.nodeid.to_string():
            return {}, {}, {}

        nodes: NodeDict = {}
        attributes: AttrDict = {}
        commands: CmdDict = {}

        node_class = await node.read_node_class()
        node_name, parents = await self._generate_full_node_name(
            node, top_level_node_name, parent_names, node_class
        )
        # Do not add the InputArgument and OutputArgument nodes.
        if node_name.endswith(".InputArguments") or node_name.endswith(
            ".OutputArguments"
        ):
            return nodes, attributes, commands

        nodes[node_name] = (node, node_class)
        # Normal node with children
        # Note: 'ServerStatus' is a standard OPC-UA variable node that has children
        if (
            node_class == ua.NodeClass.Object
            or node.nodeid.to_string() == SERVER_STATUS_ID
        ):
            children = await node.get_children()
            tasks = [
                self._get_sub_nodes(child, top_level_node_name, skip_node_id, parents)
                for child in children
            ]
            results = await asyncio.gather(*tasks)
            for child_nodes, child_attributes, child_commands in results:
                nodes.update(child_nodes)
                attributes.update(child_attributes)
                commands.update(child_commands)
        elif node_class == ua.NodeClass.Variable:  # Attribute
            # Check if RO or RW and call the respective creator functions.
            # access_level_set = await node.get_access_level()
            # if ua.AccessLevel.CurrentWrite in access_level_set:
            attributes[node_name] = create_rw_attribute(
                node, self.event_loop, node_name
            )
            # elif ua.AccessLevel.CurrentRead in access_level_set:
            #     attributes[node_name] = create_ro_attribute(
            #         node, self.event_loop, node_name
            #     )
            # else:
            #     logger.warning(
            #         "AccessLevel for variable node '%s' is not expected 'CurrentRead'"
            #         " or 'CurrentWrite', but '%s'",
            #         node_name,
            #         access_level_set,
            #     )
        elif node_class == ua.NodeClass.Method:  # Command
            command_method = await self._create_command_function(
                node,
                self.event_loop,
                node_name,
            )
            if command_method:
                commands[node_name] = command_method
        return nodes, attributes, commands

    # --------------
    # Getter methods
    # --------------
    def get_node_list(self) -> list[str]:
        """
        Get a list of node names.

        :return: A list of node names.
        """
        return self.__get_node_list(self.nodes)

    def get_command_list(self) -> list[str]:
        """
        Get the list of commands associated with this object.

        :return: A list of strings representing the commands.
        """
        return self.__get_node_list(self.commands)

    def get_command_arguments(self, command_node: Node) -> list[tuple[str, str]] | None:
        """
        Get a list of the input arguments of a given command.

        :param command_node: Node of the command to retrieve.
        :return: List of tuples with each argument's name and its type, or None if the
            command does not exist.
        """
        # Browse the command node to find the InputArguments node
        browse_results = asyncio.run_coroutine_threadsafe(
            self._client.browse_nodes([command_node]), self.event_loop
        ).result()
        for _, result in browse_results:
            if not result.References:
                logger.warning(
                    "Cannot browse node with ID '%s'.", command_node.nodeid.Identifier
                )
                return None
            for ref in result.References:
                if ref.BrowseName.Name == "InputArguments":
                    input_args_node = self._client.get_node(ref.NodeId)
                    input_args_objects = asyncio.run_coroutine_threadsafe(
                        input_args_node.read_value(), self.event_loop
                    ).result()
                    if not input_args_objects:
                        logger.warning(
                            "No input arguments found for command node '%s'.",
                            command_node.nodeid.Identifier,
                        )
                        return []
                    # Prepare nodes for data type browse names
                    data_type_nodes = [
                        self._client.get_node(arg.DataType)
                        for arg in input_args_objects
                    ]
                    # Read browse names of all data type nodes in one call
                    data_type_browse_names = asyncio.run_coroutine_threadsafe(
                        self._client.read_attributes(
                            data_type_nodes, ua.AttributeIds.BrowseName
                        ),
                        self.event_loop,
                    ).result()
                    # Build a list of tuples with each input arg and its data type name
                    return [
                        (
                            (arg.Name, browse_name.Value.Value.Name)
                            if browse_name.Value
                            else (arg.Name, "Unknown")
                        )
                        for arg, browse_name in zip(
                            input_args_objects, data_type_browse_names
                        )
                    ]
        logger.warning(
            "InputArguments node not found for command node '%s'.",
            command_node.nodeid.Identifier,
        )
        return None

    def get_attribute_list(self) -> list[str]:
        """
        Get the list of attributes.

        :return: A list of attribute names.
        """
        return self.__get_node_list(self.attributes)

    def get_parameter_node_list(self) -> list[str]:
        """
        Get a list of parameter nodes.

        :return: A list of parameter nodes.
        """
        return self.__get_node_list(self.parameter_nodes)

    def get_parameter_attribute_list(self) -> list[str]:
        """
        Return a list of parameter attributes.

        :return: A list of parameter attribute names.
        """
        return self.__get_node_list(self.parameter_attributes)

    def __get_node_list(self, nodes: dict) -> list[str]:
        """
        Get a list of node keys from a dictionary of nodes.

        :param nodes: A dictionary of nodes where keys are node identifiers.
        :return: A list of node keys.
        """
        logger.debug("\n".join(nodes.keys()))
        return list(nodes.keys())

    def _get_cached_attribute_data_type(self, attribute: str) -> list[str]:
        try:
            # Return a copy so that self._attribute_types_cache cannot be touched.
            return self._attribute_types_cache[attribute].copy()
        except KeyError:
            return ["Unknown"]

    def get_attribute_data_type(self, attribute: str | ua.uatypes.NodeId) -> list[str]:
        """
        Get the data type for the given node.

        Returns a list of strings for the type or "Unknown" for a not yet known type.
        For most types the list is length one, for enumeration types the list is
        "Enumeration" followed by the strings of the enumeration, where the index of the
        string in the list is the enum value + 1.
        """
        dt_id = ""
        ret = ["Unknown"]
        if isinstance(attribute, str):
            cached_type = self._get_cached_attribute_data_type(attribute)
            if cached_type != ret:
                logger.debug("Found cached type %s for %s", cached_type, attribute)
                return cached_type

            if attribute.endswith("Pointing.Status.CurrentPointing"):
                # Special case where the ICD type is Double but the node actually
                # returns a 7-element array.
                self._attribute_types_cache[attribute] = [attribute]
                return [attribute]

            node, _ = self.nodes.get(attribute, (None, None))
            if node is None:
                return ret
            dt_id = asyncio.run_coroutine_threadsafe(
                node.read_data_type(), self.event_loop
            ).result()
        elif isinstance(attribute, ua.uatypes.NodeId):
            dt_id = attribute

        dt_node = self._client.get_node(dt_id)
        dt_node_info = asyncio.run_coroutine_threadsafe(
            dt_node.read_browse_name(), self.event_loop
        ).result()
        dt_name = dt_node_info.Name
        instant_types = [
            "Boolean",
            "ByteString",
            "DateTime",
            "Double",
            "Float",
            "String",
            "UInt16",
            "UInt32",
        ]
        if dt_name in instant_types:
            ret = [dt_name]

        # load_data_type_definitions() called in connect() adds new classes to the
        # asyncua.ua module.
        try:
            ua_type = getattr(ua, dt_name)
        except AttributeError:
            pass
        else:
            if issubclass(ua_type, Enum):
                enum_list = [""] * (max(e.value for e in ua_type) + 1)
                for e in ua_type:
                    enum_list[e.value] = e.name

                enum_list.insert(0, "Enumeration")
                ret = enum_list

        self._attribute_types_cache[attribute] = ret
        logger.debug("Found type %s for %s", ret, attribute)
        return self._get_cached_attribute_data_type(attribute)

    def fetch_all_attribute_types(self) -> None:
        """Prompt SCU to cache the types of all attributes."""
        for attribute in self.attributes:
            self.get_attribute_data_type(attribute)

    def get_node_descriptions(self, node_list: list[str]) -> list[tuple[str, str]]:
        """
        Get the descriptions of a list of nodes.

        :param node_list: A list of node names.
        :return: A list of tuples containing the node names and their descriptions.
        """

        async def get_descriptions() -> list[str]:
            coroutines = [
                self.nodes[nm].read_description() for nm in node_list  # type: ignore
            ]
            return await asyncio.gather(*coroutines)

        descriptions = asyncio.run_coroutine_threadsafe(
            get_descriptions(), self.event_loop
        ).result()
        # Convert the descriptions to text (or None)
        descriptions = [desc.Text for desc in descriptions]  # type: ignore
        result = list(zip(node_list, descriptions))
        return result

    # --------------------
    # OPC-UA subscriptions
    # --------------------
    # pylint: disable=dangerous-default-value,too-many-branches
    def subscribe(
        self,
        attributes: str | list[str],
        period: int = 100,
        data_queue: queue.Queue | None = None,
        bad_shutdown_callback: Callable[[str], None] | None = None,
        subscription_handler: SubscriptionHandler | None = None,
    ) -> tuple[int, list, list]:
        """
        Subscribe to OPC-UA attributes for event updates.

        :param attributes: A single OPC-UA attribute or a list of attributes to
            subscribe to.
        :param period: The period in milliseconds for checking attribute updates.
        :param data_queue: A queue to store the subscribed attribute data. If None, uses
            the default subscription queue.
        :return: unique identifier for the subscription and lists of missing/bad nodes.
        :param bad_shutdown_callback: will be called if a BadShutdown subscription
            status notification is received, defaults to None.
        :param subscription_handler: Allows for a SubscriptionHandler instance to be
            reused, rather than creating a new instance every time.
            There is a limit on the number of handlers a server can have.
            Defaults to None.
        """
        if data_queue is None:
            data_queue = self._subscription_queue
        if not subscription_handler:
            subscription_handler = SubscriptionHandler(
                data_queue, self._nodes_reversed, bad_shutdown_callback
            )
        if not isinstance(attributes, list):
            attributes = [
                attributes,
            ]
        nodes = []
        missing_nodes = []
        bad_nodes = []
        uid = time.monotonic_ns()
        for attribute in attributes:
            if attribute != "":
                if attribute in self.nodes:
                    nodes.append(self.nodes[attribute][0])
                else:
                    missing_nodes.append(attribute)
        if len(missing_nodes) > 0:
            logger.debug(
                "The following attributes were not found on the OPCUA server and not "
                "subscribed to for event updates: %s",
                missing_nodes,
            )
        subscription = asyncio.run_coroutine_threadsafe(
            self._client.create_subscription(period, subscription_handler),
            self.event_loop,
        ).result()
        if nodes:
            subscribe_nodes = list(set(nodes))  # Remove any potential node duplicates
            try:
                handles = asyncio.run_coroutine_threadsafe(
                    subscription.subscribe_data_change(subscribe_nodes), self.event_loop
                ).result()
            except ua.UaStatusCodeError as e:
                # Exceptions are only generated when subscribe_data_change is called
                # with a single node input.
                msg = (
                    "Failed to subscribe to node "
                    f"'{subscribe_nodes[0].nodeid.to_string()}'"
                )
                asyncio.run_coroutine_threadsafe(
                    handle_exception(e, msg), self.event_loop
                )
                bad_nodes.append(subscribe_nodes[0])
                subscribe_nodes.pop(0)

            # subscribe_data_change returns an int for a single node, and a list for
            # mulitple nodes
            if isinstance(handles, int):
                handles = list(handles)
            # The list contains ints for sucessful subscriptions, and status codes when
            # the subscription has failed.
            else:
                for i, node in enumerate(subscribe_nodes):
                    if isinstance(handles[i], ua.uatypes.StatusCode):
                        bad_nodes.append(node)
                        subscribe_nodes.pop(i)
                        handles.pop(i)

            self._subscriptions[uid] = {
                "handles": handles,
                "nodes": subscribe_nodes,
                "subscription": subscription,
            }

        return uid, missing_nodes, bad_nodes

    def unsubscribe(self, uid: int) -> None:
        """
        Unsubscribe a user from a subscription.

        :param uid: The ID of the user to unsubscribe.
        """
        subscription = self._subscriptions.pop(uid, None)
        if subscription is not None:
            try:
                _ = asyncio.run_coroutine_threadsafe(
                    subscription["subscription"].delete(), self.event_loop
                ).result()
            except ConnectionError as e:
                asyncio.run_coroutine_threadsafe(
                    handle_exception(e, f"Tried to unsubscribe from {uid}."),
                    self.event_loop,
                )

    def unsubscribe_all(self) -> None:
        """Unsubscribe all subscriptions."""
        for uid in self._subscriptions.copy():
            self.unsubscribe(uid)

    def get_subscription_values(self) -> list[dict]:
        """
        Get the values from the subscription queue.

        :return: A list of dictionaries containing the values from the subscription
            queue.
        """
        values = []
        while not self._subscription_queue.empty():
            values.append(self._subscription_queue.get(block=False, timeout=0.1))
        return values

    # -------------
    # Dish tracking
    # -------------
    def load_track_table(
        self,
        mode: str | int = 0,
        tai: list[float] = [],
        azi: list[float] = [],
        ele: list[float] = [],
        file_name: str | None = None,
        absolute_times: bool = True,
        additional_offset: float = 10.1,  # TODO Return to 5 when PLCs updated
        result_callback: Callable[[ResultCode, str], None] | None = None,
    ) -> tuple[ResultCode, str]:
        """
        Upload a track table to the PLC from lists OR a CSV file.

        Number of points for tai, azimuth, and elevation must be equal.
        Supports modes New and Append for lists, and New only for a CSV file.
        Although a PLC can only hold a maximum of 10,000 points at a time, the lists or
        CSV file can contain an arbritary number of points and this function will
        store them all locally and load them to the PLC when space is made avaiable by
        the PLC consuming loaded points when tracking has been started. However a
        subsequent call with mode 1: "New" will cancel any points waiting to be
        loaded.
        For tests that generate points just-in-time use lists. First call this method
        with mode 1: "New", then make all subsequent calls with mode 2: "Append".
        If absolute_times is false, the value of Time_cds.Status.TAIoffset will be
        retrieved from the PLC and added to the tai times supplied.

        :param int mode: Choose from 0 for Append, and 1 for New. Default is Append.
          # TODO add reset.
        :param list[float] tai: A list of times to make up the track table points.
        :param list[float] azi: A list of azimuths to make up the track table points.
        :param list[float] ele: A list of elevations to make up the track table points.
        :param str file_name: File name of the track table file including its path.
        :param bool absolute_times: Whether the time column is a real time or a relative
            time. Default True.
        :param float additional_offset: Add additional time to every point. Only has an
            effect when absolute_times is False. Default 10.1
        :param result_callback: Callback with result code and message when task finishes
        :return: The result of the command execution.
        """
        if not self._session_id:
            msg = "Need to take command authority before loading a track table."
            logger.error(msg)
            return ResultCode.NOT_EXECUTED, msg

        mode_int = (
            self.convert_enum_to_int("LoadModeType", mode)
            if isinstance(mode, str)
            else mode
        )
        if file_name is not None and mode_int == 0 and not absolute_times:
            msg = (
                "Cannot append a track table file with relative times. Please "
                "use mode 1: 'New'"
            )
            logger.error(msg)
            return ResultCode.NOT_EXECUTED, msg

        table_args: list[Any] = [absolute_times]
        if not absolute_times:
            table_args.append(additional_offset)
            table_args.append(
                self.attributes[
                    "Time_cds.Status.TAIoffset"
                ].value  # type: ignore[attr-defined]
            )
        track_table = TrackTable(*table_args)

        if len(tai) > 0:
            track_table.store_from_list(tai, azi, ele)
        elif file_name is not None:
            track_table.store_from_csv(file_name)
        else:
            msg = "Missing track table points."
            logger.error(msg)
            return ResultCode.NOT_EXECUTED, msg

        if mode_int == 1:  # New track table queue
            if self._stop_track_table_schedule_task_event is not None:
                self._stop_track_table_schedule_task_event.set()

            self._track_table_queue = None
            self._track_table_scheduled_task = None
            self._track_table = None

        if self._track_table_queue is None:
            self._track_table_queue = queue.Queue()

        self._track_table_queue.put(
            {
                "track_table": track_table,
                "mode": mode_int,
                "result_callback": result_callback,
            }
        )
        # Rely on scheduled track table task to send appended values if it is still
        # running (inadequate space on PLC to load all stored points).
        if (
            self._track_table_scheduled_task is None
            or self._track_table_scheduled_task.done()
        ):
            return self._load_track_table_to_plc()

        return ResultCode.EXECUTING, "Track table queued."

    def _load_track_table_to_plc(self) -> tuple[ResultCode, str]:
        first_load = threading.Event()
        stop_scheduling = threading.Event()
        self._track_table_scheduled_task = asyncio.run_coroutine_threadsafe(
            self._schedule_load_next_points(first_load, stop_scheduling),
            self.event_loop,
        )
        self._stop_track_table_schedule_task_event = stop_scheduling
        if not first_load.wait(3):
            stop_scheduling.set()
            table_info = ""
            if self._track_table:
                table_info = " " + self._track_table.get_details_string()

            msg = f"Timed out while loading first batch of track table{table_info}."
            logger.error(msg)
            return ResultCode.NOT_EXECUTED, msg

        return (
            ResultCode.EXECUTING,
            f"First batch of {len(self._track_table.tai)} track table points loaded "
            f"successfully. Continuing with loading...",
        )

    async def _schedule_load_next_points(
        self,
        first_load: threading.Event,
        stop_scheduling: threading.Event,
    ) -> None:
        table_kwargs = self._track_table_queue.get(block=False)
        self._track_table = table_kwargs["track_table"]
        mode = table_kwargs["mode"]
        result_callback = table_kwargs["result_callback"]
        # Create the call method here so it only happens once
        track_load_node = self.nodes[Command.TRACK_LOAD_TABLE.value][0]
        track_load_parent = await track_load_node.get_parent()

        async def track_load_method(args):
            return await track_load_parent.call_method(track_load_node, *args)

        # One call in case mode is "New"
        result_code, result_msg = await self._load_next_points(track_load_method, mode)
        first_load.set()
        # Then keep calling until local track table is empty or PLC is full
        while not stop_scheduling.is_set() and result_code != ResultCode.NOT_EXECUTED:
            if (
                result_code
                not in (
                    ResultCode.COMMAND_DONE,
                    ResultCode.COMMAND_ACTIVATED,
                    ResultCode.COMMAND_FAILED,
                    ResultCode.ENTIRE_TRACK_TABLE_LOADED,
                )
                or result_code == ResultCode.UNKNOWN
            ):
                msg = (
                    f"Failed to load all points to PLC. Reason: {result_msg}. "
                    f"Result code: {result_code}. "
                    f"{self._track_table.remaining_points()} remaining from "
                    f"{self._track_table.get_details_string()}."
                )
                logger.error(msg)
                result_callback(result_code, msg)
                break

            if result_code == ResultCode.COMMAND_FAILED:
                # CommandFailed is returned when there is not enough space left
                # in the PLC track table.
                # Allow time for the loaded points to be consumed. Minimum time between
                # points is 50ms, at that rate 1000 points would take 50 seconds. Check
                # more often than this to ensure PLC does not run out of points.
                sleep_length = 45
                if result_callback:
                    result_callback(
                        result_code,
                        "PLC track table full after loading "
                        f"{self._track_table.num_loaded_batches} batches. Waiting "
                        f"{sleep_length} seconds for loaded points to be consumed...",
                    )
                await asyncio.sleep(sleep_length)

            if result_code == ResultCode.ENTIRE_TRACK_TABLE_LOADED:
                if result_callback:
                    result_callback(result_code, result_msg)

                try:
                    table_kwargs = self._track_table_queue.get(block=False)
                except queue.Empty:
                    result_msg = (
                        "Finished loading all queued track table points to the PLC."
                    )
                    break

                self._track_table = table_kwargs["track_table"]
                result_callback = table_kwargs["result_callback"]

            result_code, result_msg = await self._load_next_points(track_load_method)
        logger.debug("Async track table loading: %s", result_msg)

    async def _load_next_points(
        self, track_load_method: Callable, mode: int = 0
    ) -> tuple[ResultCode, str]:
        num, tai, azi, ele = self._track_table.get_next_points(1000)
        logger.debug(f"_load_next_points got {num} points")

        if num == 0:
            return (
                ResultCode.ENTIRE_TRACK_TABLE_LOADED,
                f"Finished loading track table {self._track_table.get_details_string()}"
                f" to the PLC. {self._track_table.sent_index} points have been sent in "
                f"{self._track_table.num_loaded_batches} batches.",
            )

        # The TrackLoadTable node accepts arrays of length 1000 only.
        if num < 1000:
            padding = [0] * (1000 - num)
            tai.extend(padding)
            azi.extend(padding)
            ele.extend(padding)

        if not self._session_id:
            return (
                ResultCode.NOT_EXECUTED,
                "Lost authority while loading track table "
                f"{self._track_table.get_details_string()}.",
            )

        # Call TrackLoadTable command and validate result code
        load_args = [
            self._session_id,
            ua.UInt16(mode),
            ua.UInt16(num),
            tai,
            azi,
            ele,
        ]
        logger.info("Calling track load with the following args: %s", load_args)
        result_code = await track_load_method(load_args)
        result_code, result_msg = self._validate_command_result_code(result_code)
        if result_code not in (
            ResultCode.COMMAND_DONE,
            ResultCode.COMMAND_ACTIVATED,
        ):
            # Failed to send points so restore index.
            self._track_table.sent_index -= num
        else:
            self._track_table.num_loaded_batches += 1

        return result_code, result_msg

    def start_tracking(
        self,
        interpol: str | int = 0,
        now: bool = True,
        start_time: float | None = None,
    ) -> tuple[ResultCode, str]:
        """
        Start a loaded track table.

        :param int interpol: The desired interpolation method. Defaults to 0; Spline.
        :param bool now: Whether to start tracking immediately or not. Default True.
        :param float start_time: The track start time in seconds after SKAO Epoch (TAI
            Offset). Only applicable if now is False.
        :return CmdReturn: The response to TrackStart from the PLC.
        """
        interpol_int = (
            self.convert_enum_to_int("InterpolType", interpol)
            if isinstance(interpol, str)
            else interpol
        )
        if now:
            start_time = self.attributes[
                "Time_cds.Status.TAIoffset"
            ].value  # type: ignore[attr-defined]

        code, msg, _ = self.commands[Command.TRACK_START.value](
            ua.UInt16(interpol_int), start_time
        )
        return code, msg

    # ---------------------
    # Static pointing model
    # ---------------------
    def import_static_pointing_model(self, file_path: Path) -> str | None:
        """
        Import static pointing model parameters from a JSON file.

        The static pointing model is only imported to a variable of the SCU instance,
        and not written to a (possibly) connected DSC.

        :param file_path: Path to the JSON file to load.
        :return: The specified band the model is for, or `None` if the import failed.
        """
        sp_model = StaticPointingModel(SPM_SCHEMA_PATH)
        if sp_model.read_gpm_json(file_path):
            band = sp_model.get_band()
            self._static_pointing[band] = sp_model
            logger.debug(
                f"Successfully imported static pointing model from '{str(file_path)}' "
                f"for '{band}'."
            )
            return band
        return None

    def export_static_pointing_model(
        self,
        band: str,
        file_path: Path | None = None,
        antenna: str | None = None,
        overwrite: bool = False,
    ) -> None:
        """
        Export current static pointing model parameters of specified band to JSON file.

        :param band: Band name to export.
        :param file_path: Optional path and name of JSON file to write.
        :param antenna: Optional antenna name to store in static pointing model JSON.
        :param overwrite: Whether to overwrite an existing file. Default is False.
        """
        if band in self._static_pointing:
            if antenna is not None:
                self._static_pointing[band].set_antenna(antenna)
            if self._static_pointing[band].write_gpm_json(file_path, overwrite):
                logger.debug(f"Successfully exported '{band}' static pointing model.")
        else:
            logger.info(f"Static pointing model not setup for '{band}' in SCU client.")

    def set_static_pointing_value(self, band: str, name: str, value: float) -> None:
        """
        Set the named static pointing parameters value in the band's model.

        :param band: Band name.
        :param name: Name of the parameter to set.
        :param value: Float value to set.
        """
        if band in self._static_pointing:
            self._static_pointing[band].set_coefficient(name, value=value)
        else:
            logger.info(f"Static pointing model not setup for '{band}' in SCU client.")

    def get_static_pointing_value(self, band: str, name: str) -> float | None:
        """
        Get the named static pointing parameters value in the band's model.

        :param band: Band name.
        :param name: Name of the parameter to set.
        :returns:
            - Value of parameter if set.
            - Default 0.0 if not set.
            - NaN if invalid parameter name given.
            - None if band's model is not setup.
        """
        if band in self._static_pointing:
            return self._static_pointing[band].get_coefficient_value(name)
        logger.info(f"Static pointing model not setup for '{band}' in SCU client.")
        return None

    def read_static_pointing_model(
        self, band: str, antenna: str = "SKAxxx"
    ) -> dict[str, float]:
        """
        Read static pointing model parameters for a specified band from connected DSC.

        The read parameters is stored in SCU's static pointing model dict so changes can
        be made and setup and/or exported again.

        :param band: Band's parameters to read from DSC.
        :param antenna: Target antenna name to store in static pointing model JSON dict.
        :return: A dict of the read static pointing parameters.
        """
        if band not in self._static_pointing:
            logger.debug(f"Creating new StaticPointingModel instance for '{band}'.")
            self._static_pointing[band] = StaticPointingModel(SPM_SCHEMA_PATH)
            self._static_pointing[band].set_antenna(antenna)
            self._static_pointing[band].set_band(band)
        coeff_dict = {}
        band_int = self.convert_enum_to_int("BandType", band)
        for coeff in self._static_pointing[band].coefficients:
            value = self.attributes[
                f"Pointing.StaticPmParameters.StaticPmParameters[{band_int}].{coeff}"
            ].value
            coeff_dict[coeff] = value
            self._static_pointing[band].set_coefficient(coeff, value=value)
        return coeff_dict

    def setup_static_pointing_model(self, band: str) -> CmdReturn:
        """
        Setup static pointing model parameters for a specified band on connected DSC.

        :param band: Band's parameters to write to DSC.
        :return: The result of writing the static pointing model parameters.
        """
        if band in self._static_pointing:
            band_int = self.convert_enum_to_int("BandType", band)
            params = self._static_pointing[band].get_all_coefficient_values()
            logger.debug(f"Static PM setup for '{band}': {params}")
            return self.commands[Command.STATIC_PM_SETUP.value](band_int, *params)
        msg = f"Static pointing model not setup for '{band}' in SCU client."
        logger.info(msg)
        return ResultCode.NOT_EXECUTED, msg, None

    # ---------------------------
    # Convenience command methods
    # ---------------------------
    def interlock_acknowledge_dmc(self) -> CmdReturn:
        """
        Acknowledge the interlock status for the DMC.

        :return: The result of acknowledging the interlock status for the DMC.
        """
        logger.info("acknowledge dmc")
        return self.commands[Command.INTERLOCK_ACK.value]()

    def reset_dmc(self, axis: int) -> CmdReturn:
        """
        Reset the DMC  for a specific axis.

        :param axis: The axis to be reset (AxisSelectType).
        :return: The result of resetting the DMC for the specified axis.
        """
        logger.info("reset dmc")
        return self.commands[Command.RESET.value](ua.UInt16(axis))

    def activate_dmc(self, axis: int) -> CmdReturn:
        """
        Activate the DMC for a specific axis.

        :param axis: The axis to activate (AxisSelectType).
        :return: The result of activating the DMC for the specified axis.
        """
        logger.info("activate dmc")
        return self.commands[Command.ACTIVATE.value](ua.UInt16(axis))

    def deactivate_dmc(self, axis: int) -> CmdReturn:
        """
        Deactivate a specific axis of the DMC controller.

        :param axis: The axis to deactivate (AxisSelectType).
        :return: The result of desactivating the DMC for the specified axis.
        """
        logger.info("deactivate dmc")
        return self.commands[Command.DEACTIVATE.value](ua.UInt16(axis))

    def move_to_band(self, position: str | int) -> CmdReturn:
        """
        Move the system to a specified band.

        :param position: The band to move to, either as a string representation or a
            numerical value.
        :return: The result of moving to the specified band.
        :raises KeyError: If the specified band is not found in the bands dictionary.
        """
        logger.info("move to band: %s", position)
        if isinstance(position, int):
            return self.commands[Command.MOVE2BAND.value](ua.UInt16(position))
        band = self.convert_enum_to_int("BandType", position)
        return self.commands[Command.MOVE2BAND.value](band)

    def abs_azel(
        self,
        az_angle: float,
        el_angle: float,
        az_velocity: float = 1.0,
        el_velocity: float = 1.0,
    ) -> CmdReturn:
        """
        Calculate and move the telescope to an absolute azimuth and elevation position.

        :param az_angle: The absolute azimuth angle in degrees.
        :param el_angle: The absolute elevation angle in degrees.
        :param az_velocity: The azimuth velocity in degrees per second.
        :param el_velocity: The elevation velocity in degrees per second.
        """
        logger.info(
            f"abs az: {az_angle:.4f} el: {el_angle:.4f}, "
            f"az velocity: {az_velocity:.4f}, el velocity: {el_velocity:.4f}"
        )
        return self.commands[Command.SLEW2ABS_AZ_EL.value](
            az_angle, el_angle, az_velocity, el_velocity
        )

    def stow(self) -> CmdReturn:
        """
        Stow the dish.

        :return: The result of the stow command.
        """
        logger.info("stow")
        return self.commands[Command.STOW.value](True)

    def unstow(self) -> CmdReturn:
        """
        Unstow the dish.

        :return: The result of the unstow command.
        """
        logger.info("unstow")
        return self.commands[Command.STOW.value](False)

    def activate_az(self) -> CmdReturn:
        """
        Activate the azimuth axis.

        :return: The result of activating the azimuth functionality.
        """
        logger.info("activate azimuth")
        return self.activate_dmc(self.convert_enum_to_int("AxisSelectType", "Az"))

    def deactivate_az(self) -> CmdReturn:
        """
        Deactivate the azimuth axis.

        :return: The result of deactivating azimuth.
        """
        logger.info("deactivate azimuth")
        return self.deactivate_dmc(self.convert_enum_to_int("AxisSelectType", "Az"))

    def activate_el(self) -> CmdReturn:
        """
        Activate the elevation axis.

        :return: The result of activating elevation.
        """
        logger.info("activate elevation")
        return self.activate_dmc(self.convert_enum_to_int("AxisSelectType", "El"))

    def deactivate_el(self) -> CmdReturn:
        """
        Deactivate the elevation axis.

        :return: The result of deactivating elevation.
        """
        logger.info("deactivate elevation")
        return self.deactivate_dmc(self.convert_enum_to_int("AxisSelectType", "El"))

    def activate_fi(self) -> CmdReturn:
        """
        Activate the feed indexer.

        :return: The result of activating the feed indexer.
        """
        logger.info("activate feed indexer")
        return self.activate_dmc(self.convert_enum_to_int("AxisSelectType", "Fi"))

    def deactivate_fi(self) -> CmdReturn:
        """
        Deactivate the feed indexer.

        :return: The result of deactivating the feed indexer.
        """
        logger.info("deactivate feed indexer")
        return self.deactivate_dmc(self.convert_enum_to_int("AxisSelectType", "Fi"))

    def abs_azimuth(self, az_angle: float, az_vel: float) -> CmdReturn:
        """
        Move to the absolute azimuth angle with given velocity.

        :param az_angle: The azimuth angle value.
        :param az_vel: The azimuth velocity.
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs az: {az_angle:.4f} vel: {az_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            self.convert_enum_to_int("AxisSelectType", "Az"), az_angle, az_vel
        )

    def abs_elevation(self, el_angle: float, el_vel: float) -> CmdReturn:
        """
        Move to the absolute elevation angle with given velocity.

        :param el_angle: The absolute elevation angle.
        :param el_vel: The elevation velocity.
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs el: {el_angle:.4f} vel: {el_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            self.convert_enum_to_int("AxisSelectType", "El"), el_angle, el_vel
        )

    def abs_feed_indexer(self, fi_angle: float, fi_vel: float) -> CmdReturn:
        """
        Calculate the absolute feed indexer value.

        :param fi_angle: The angle value for the feed indexer.
        :param fi_vel: The velocity value for the feed indexer.
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs fi: {fi_angle:.4f} vel: {fi_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            self.convert_enum_to_int("AxisSelectType", "Fi"), fi_angle, fi_vel
        )

    def load_static_offset(self, az_offset: float, el_offset: float) -> CmdReturn:
        """
        Load static azimuth and elevation offsets for tracking.

        :param az_offset: The azimuth offset value.
        :param el_offset: The elevation offset value.
        :return: The result of loading the static offsets.
        """
        logger.info(f"offset az: {az_offset:.4f} el: {el_offset:.4f}")
        return self.commands[Command.TRACK_LOAD_STATIC_OFF.value](az_offset, el_offset)


# pylint: disable=too-many-arguments, invalid-name
def SCU(  # noqa: N802
    host: str = "127.0.0.1",
    port: int = 4840,
    endpoint: str = "",
    namespace: str = "",
    username: str | None = None,
    password: str | None = None,
    timeout: float = 10.0,
    use_nodes_cache: bool = False,
    nodes_cache_dir: Path = USER_CACHE_DIR,
    authority_name: str | None = None,
) -> SteeringControlUnit:
    """SCU object generator method.

    This method creates an SCU object with the provided parameters, connects it to the
    OPC-UA server and sets it up, ready to be used with the attributes and commands.

    :return: an instance of the SteeringControlUnit class.
    """
    scu = SteeringControlUnit(
        host=host,
        port=port,
        endpoint=endpoint,
        namespace=namespace,
        username=username,
        password=password,
        timeout=timeout,
        use_nodes_cache=use_nodes_cache,
        nodes_cache_dir=nodes_cache_dir,
    )
    scu.connect_and_setup()
    if authority_name is not None:
        scu.take_authority(authority_name)
    return scu
