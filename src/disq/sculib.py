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

    from disq import SCU
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

    from disq import SteeringControlUnit
    with SteeringControlUnit(host="localhost") as scu:
        scu.take_authority("LMC")

Finally, the third and most complete option to initialise and connect a
``SteeringControlUnit``, is to use the ``SCU_from_config()`` object generator method,
which will:

- Read the server address/port/namespace from the configuration file.
- Configure logging.
- Create (and return) the ``SteeringControlUnit`` object.
- Connect to the server.
- Take authority if requested.

.. code-block:: python

    from disq import SCU_from_config
    scu = SCU_from_config("cetc54_simulator", authority_name="LMC")
    # Do things with the scu instance..
    scu.disconnect_and_cleanup()

All nodes from and including the PLC_PRG node are stored in the nodes dictionary:
``scu.nodes``. The keys are the full node names, the values are the Node objects.
The full names of all nodes can be retrieved with:

.. code-block:: python

    scu.get_node_list()

Every value in ``scu.nodes`` exposes the full OPC UA functionality for a node.
Note: When accessing nodes directly, it is mandatory to await any calls:

.. code-block:: python

    node = scu.nodes["PLC_PRG"]
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

    from disq import Command
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

# pylint: disable=too-many-lines, broad-exception-caught
import asyncio
import datetime
import enum
import json
import logging
import logging.config
import queue
import socket
import threading
import time
from concurrent.futures import Future
from enum import Enum
from functools import cached_property
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Final, Type, TypedDict

from asyncua import Client, Node, ua
from asyncua.crypto.cert_gen import setup_self_signed_certificate
from asyncua.crypto.security_policies import SecurityPolicyBasic256
from cryptography.x509.oid import ExtendedKeyUsageOID
from packaging.version import InvalidVersion, Version

from disq import configuration
from disq.constants import (
    PACKAGE_VERSION,
    USER_CACHE_DIR,
    CmdReturn,
    Command,
    ResultCode,
)
from disq.track_table import TrackTable

logger = logging.getLogger("ska-mid-ds-scu")

COMPATIBLE_CETC_SIM_VER: Final = Version("3.2.3")


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
        logger.exception("*** Exception caught: %s [context: %s]", e, msg)


def create_rw_attribute(
    node: Node, event_loop: asyncio.AbstractEventLoop, node_name: str
) -> object:
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
) -> object:
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


class CachedNodesDict(TypedDict):
    """Cached nodes dictionary type."""

    node_ids: dict[str, tuple[str, int]]
    timestamp: str


# Type aliases
NodeDict = dict[str, tuple[Node, int]]
AttrDict = dict[str, object]
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
    :param gui_app: Whether the instance is for a GUI application. Default is False.
    :param use_nodes_cache: Whether to use any existing caches of node IDs.
        Default is False.
    :param nodes_cache_dir: Directory where to save the cache and load it from.
        Default is the user cache directory determined with platformdirs.
    :param app_name: application name for OPC UA client description.
        Default 'SKA-Mid-DS-SCU v{PACKAGE_VERSION}'
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
        gui_app: bool = False,
        use_nodes_cache: bool = False,
        nodes_cache_dir: Path = USER_CACHE_DIR,
        app_name: str = f"SKA-Mid-DS-SCU v{PACKAGE_VERSION}",
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
        self._gui_app = gui_app
        self._use_nodes_cache = use_nodes_cache
        self._nodes_cache_dir = nodes_cache_dir
        self._app_name = app_name
        # Other local variables
        self._client: Client | None = None
        self._event_loop_thread: threading.Thread | None = None
        self._subscription_handler = None
        self._subscriptions: dict = {}
        self._subscription_queue: queue.Queue = queue.Queue()
        self._user: int | None = None
        self._session_id: ua.UInt16 | None = None
        self._server_url: str
        self._server_str_id: str
        self._plc_prg: Node
        self._ns_idx: int
        self._nodes: NodeDict
        self._nodes_reversed: dict[tuple[Node, int], str]
        self._attributes: AttrDict
        self._commands: CmdDict
        self._plc_prg_nodes_timestamp: str
        self._parameter: Node
        self._parameter_ns_idx: int
        self._parameter_nodes: NodeDict
        self._parameter_attributes: AttrDict
        self._track_table_queue: queue.Queue | None = None
        self._stop_track_table_schedule_task_event: threading.Event | None = None
        self._track_table_scheduled_task: Future | None = None
        self._track_table: TrackTable | None = None
        self._opcua_enum_types: dict[str, Type[Enum]] = {}

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
                logger.exception(msg)
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
        result_code, _ = self.release_authority()
        if result_code != ResultCode.CONNECTION_CLOSED:
            self.unsubscribe_all()
            self.disconnect()
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
                "From now on it will be assumed that the CETC54 simulator is running."
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
                logger.exception(message)
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
        """Dictionary of the 'PLC_PRG' node's entire tree of `asyncua.Node` objects."""
        return self._nodes

    @property
    def attributes(self) -> AttrDict:
        """
        Dictionary containing the attributes in the 'PLC_PRG' node tree.

        The values are callables that return the current value.
        """
        return self._attributes

    @property
    def commands(self) -> CmdDict:
        """
        Dictionary containing command methods in the 'PLC_PRG' node tree.

        The values are callables which require the expected parameters.
        """
        return self._commands

    @property
    def plc_prg_nodes_timestamp(self) -> str:
        """
        Generation timestamp of the 'PLC_PRG' nodes.

        The timestamp is in 'yyyy-mm-dd hh:mm:ss' string format.
        """
        return self._plc_prg_nodes_timestamp

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
    def opcua_enum_types(self) -> dict[str, Type[Enum]]:
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
                    self._opcua_enum_types.update({type_name: new_enum})  # type: ignore
                    setattr(ua, type_name, new_enum)
                except (RuntimeError, ValueError):
                    missing_types.append(type_name)
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
            else user
        )
        if user_int == 2:  # HHP
            code = ResultCode.NOT_EXECUTED
            msg = "SCU cannot take authority as HHP user"
            logger.info("TakeAuth command not executed, as %s", msg)
        elif self._user is None or (self._user is not None and self._user < user_int):
            code, msg, vals = self.commands[Command.TAKE_AUTH.value](
                ua.UInt16(user_int)
            )
            if code == ResultCode.COMMAND_DONE:
                self._user = user_int
                self._session_id = ua.UInt16(vals[0])
            else:
                logger.error("TakeAuth command failed with message '%s'", msg)
        else:
            user_str = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
            code = ResultCode.NOT_EXECUTED
            msg = f"SCU already has command authority with user {user_str}"
            logger.info("TakeAuth command not executed, as %s", msg)
        return code, msg

    def release_authority(self) -> tuple[ResultCode, str]:
        """
        Release command authority.

        :return: The result of the command execution.
        """
        if self._user is not None and self._session_id is not None:
            code, msg, _ = self.commands[Command.RELEASE_AUTH.value](
                ua.UInt16(self._user)
            )
            if code == ResultCode.COMMAND_DONE:
                self._user, self._session_id = None, None
            elif code in [ResultCode.NO_CMD_AUTH, ResultCode.COMMAND_FAILED]:
                user = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
                logger.info(
                    "SCU has already lost command authority as user '%s' to "
                    "another client.",
                    user,
                )
                self._user, self._session_id = None, None
        else:
            code = ResultCode.NOT_EXECUTED
            msg = "SCU has no command authority to release."
            logger.info(msg)
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
            integer = value.value if isinstance(value, Enum) else value
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

    def _create_enum_from_node(self, name: str, node: Node) -> Enum:
        enum_values = asyncio.run_coroutine_threadsafe(
            node.get_value(), self.event_loop
        ).result()
        if not isinstance(enum_values, list):
            raise ValueError(f"Expected a list of EnumValueType for node '{name}'.")
        enum_dict = {}
        for value in enum_values:
            display_name = value.DisplayName.Text
            enum_dict[display_name] = value.Value
        return Enum(name, enum_dict)

    def _create_command_function(
        self,
        node: Node,
        event_loop: asyncio.AbstractEventLoop,
        node_name: str,
        need_authority: bool = True,
    ) -> Callable:
        """
        Create a command function to execute a method on a specified Node.

        :param node: The Node on which the method will be executed.
        :param event_loop: The asyncio event loop to run the coroutine on.
        :param node_name: The full name of the Node.
        :param need_authority: If the command needs user authority to be executed.
            Defaults to True.
        :return: A function that can be used to execute a method on the Node.
        """
        try:
            node_parent = asyncio.run_coroutine_threadsafe(
                node.get_parent(), event_loop
            ).result()
            node_call_method = node_parent.call_method
        except AttributeError as e:
            logger.error(
                "Caught an exception while trying to get the method for a command.\n"
                "Exception: %s\nNode name: %s\nParent object: %s",
                e,
                node_name,
                node_parent,
            )

            def empty_func(
                *args: Any,  # pylint: disable=unused-argument
            ) -> CmdReturn:
                return (
                    ResultCode.NOT_EXECUTED,
                    "No command method to call",
                    None,
                )

            return empty_func

        read_name = asyncio.run_coroutine_threadsafe(
            node.read_display_name(), event_loop
        )
        node_id = f"{node.nodeid.NamespaceIndex}:{read_name.result().Text}"

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
            if need_authority and self._session_id is None:
                return (
                    ResultCode.NOT_EXECUTED,
                    "SCU has no command authority to execute requested command",
                    result_output,
                )
            try:
                cmd_args = [self._session_id, *args] if need_authority else [*args]
                logger.debug(
                    "Calling command node '%s' with args list: %s",
                    node_id,
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
                        "SCU has lost command authority as user '%s' to "
                        "another client.",
                        user,
                    )
                    self._user, self._session_id = None, None
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
    def _load_json_file(self, file_path: Path) -> dict[str, CachedNodesDict]:
        """
        Load JSON file.

        :param file_path: of JSON file to load.
        :return: decoded JSON file contents as nested dictionary,
            or empty dict if the file does not exists.
        """
        if file_path.exists():
            with open(file_path, "r", encoding="UTF-8") as file:
                try:
                    return json.load(file)
                except json.JSONDecodeError:
                    logger.warning("The file %s is not valid JSON.", file_path)
        else:
            logger.debug("The file %s does not exist.", file_path)
        return {}

    def _cache_node_ids(self, file_path: Path, nodes: NodeDict) -> None:
        """
        Cache Node IDs.

        Create a dictionary of Node names with their unique Node ID and node class
        and write it to a new or existing JSON file with the OPC-UA server address
        as key.

        :param file_path: of JSON file to load.
        :param nodes: dictionary of Nodes to cache.
        """
        node_ids = {}
        for key, tup in nodes.items():
            try:
                node, node_class = tup
                if key != "":
                    node_ids[key] = (node.nodeid.to_string(), node_class)
            except TypeError as exc:
                logger.debug("TypeError with dict value %s: %s", tup, exc)
        cached_data = self._load_json_file(file_path)
        cached_data[self._server_str_id] = {
            "node_ids": node_ids,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w+", encoding="UTF-8") as file:
            json.dump(cached_data, file, indent=4, sort_keys=True)

    def _populate_node_dicts(self) -> None:
        """
        Populate dictionaries with Node objects.

        This method populates dictionaries with Node objects for different categories
        like nodes, attributes, and commands.

        nodes: Contains the entire asyncua.Node-tree from and including
        the 'PLC_PRG' node.

        attributes: Contains the attributes in the asyncua.Node-tree from
        the 'PLC_PRG' node on. The values are callables that return the
        current value.

        commands: Contains all callable methods in the asyncua.Node-tree
        from the 'PLC_PRG' node on. The values are callables which
        just require the expected parameters.

        This method may raise exceptions related to asyncio operations.
        """
        # Create node dicts of the PLC_PRG node's tree
        top_node_name = "PLC_PRG"
        self._plc_prg = asyncio.run_coroutine_threadsafe(
            self._client.nodes.objects.get_child(
                [
                    f"{self._ns_idx}:Logic",
                    f"{self._ns_idx}:Application",
                    f"{self._ns_idx}:{top_node_name}",
                ]
            ),
            self.event_loop,
        ).result()
        (
            self._nodes,
            self._attributes,
            self._commands,
            self._plc_prg_nodes_timestamp,
        ) = self._check_cache_and_generate_node_dicts(self._plc_prg, top_node_name)
        self._nodes_reversed = {val[0]: key for key, val in self.nodes.items()}

        # We also want the PLC's parameters for the drives and the PLC program.
        # But only if we are not connected to the simulator.
        top_node_name = "Parameter"
        if not self._gui_app and self._parameter_ns_idx is not None:
            self._parameter = asyncio.run_coroutine_threadsafe(
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
            ) = self._check_cache_and_generate_node_dicts(
                self._parameter, top_node_name
            )

    def _check_cache_and_generate_node_dicts(
        self,
        top_level_node: Node,
        top_level_node_name: str,
    ) -> tuple[NodeDict, AttrDict, CmdDict, str]:
        """
        Check for an existing nodes cache to use and generate the dicts accordingly.

        If the ``use_nodes_cache`` var is set, check if there is an existing cache file
        matching the top level node name, load the JSON and generate the node dicts from
        it. Otherwise generate the node dicts by scanning the node's tree on the server.

        :param top_level_node: The top-level node for which to generate dictionaries.
        :param top_level_node_name: Name of the top-level node.
        :return: A tuple containing dictionaries for nodes, attributes, and commands, as
            well as a string timestamp of when the dicts were generated.
        """
        cache_file_path = self._nodes_cache_dir / f"{top_level_node_name}.json"
        cache = self._load_json_file(cache_file_path) if self._use_nodes_cache else None
        cached_nodes = cache.get(self._server_str_id) if cache is not None else None
        # Check for existing Node IDs cache
        if cached_nodes:
            node_dicts = self._generate_node_dicts_from_cache(
                cached_nodes["node_ids"], top_level_node_name
            )
            timestamp = cached_nodes["timestamp"]
        else:
            node_dicts = self.generate_node_dicts_from_server(
                top_level_node, top_level_node_name
            )
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._cache_node_ids(cache_file_path, node_dicts[0])
        return *node_dicts, timestamp

    def _generate_node_dicts_from_cache(
        self, cache_dict: dict[str, tuple[str, int]], top_level_node_name: str
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Generate dicts for nodes, attributes, and commands from a cache.

        :param top_level_node_name: Name of the top-level node.
        :return: A tuple containing dictionaries for nodes, attributes and commands.
        """
        logger.info(
            "Generating dicts of '%s' node's tree from existing cache in %s",
            top_level_node_name,
            self._nodes_cache_dir,
        )
        nodes = {}
        attributes = {}
        commands = {}
        for node_name, tup in cache_dict.items():
            node_id, node_class = tup
            node = self._client.get_node(node_id)
            nodes[node_name] = (node, node_class)
            if node_class == 2:
                # An attribute. Add it to the attributes dict.
                attributes[node_name] = create_rw_attribute(
                    node, self.event_loop, node_name
                )
            elif node_class == 4:
                # A command. Add it to the commands dict.
                commands[node_name] = self._create_command_function(
                    node,
                    self.event_loop,
                    node_name,
                    node_name != Command.TAKE_AUTH.value,
                )
        return (
            nodes,
            attributes,
            commands,
        )

    def generate_node_dicts_from_server(
        self, top_level_node: Node, top_level_node_name: str
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Generate dicts for nodes, attributes, and commands for a given top-level node.

        This function generates dictionaries for nodes, attributes, and
        commands based on the structure of the top-level node and returns a tuple
        containing these dictionaries.

        The dictionaries contain mappings of keys to nodes, attributes, and commands.

        :param top_level_node: The top-level node for which to generate dictionaries.
        :param top_level_node_name: Name of the top-level node.
        :return: A tuple containing dictionaries for nodes, attributes, and commands.
        """
        logger.info(
            "Generating dicts of '%s' node's tree from server. It may take a while...",
            top_level_node_name,
        )
        nodes, attributes, commands = self._get_sub_nodes(top_level_node)
        nodes.update({top_level_node_name: (top_level_node, 1)})
        return (
            nodes,
            attributes,
            commands,
        )

    def generate_full_node_name(
        self,
        node: Node,
        parent_names: list[str] | None,
        node_name_separator: str = ".",
    ) -> tuple[str, list[str]]:
        """
        Generate the full name of a node by combining its name with its parent names.

        :param node: The node for which the full name is generated.
        :param parent_names: A list of parent node names. Defaults to None if the node
            has no parent.
        :param node_name_separator: The separator used to join the node and parent
            names. Default is '.'.
        :return: A tuple containing the full node name and a list of ancestor names.
        :raises: No specific exceptions raised.
        """
        try:
            name: str = node.nodeid.Identifier.split(node_name_separator)[-1]
        except AttributeError:
            name = (
                asyncio.run_coroutine_threadsafe(
                    node.read_browse_name(), self.event_loop
                )
                .result()
                .Name
            )

        ancestors = []
        if parent_names is not None:
            for p_name in parent_names:
                ancestors.append(p_name)

        if name != "PLC_PRG":
            ancestors.append(name)

        try:
            node_name = node_name_separator.join(ancestors)
        except Exception:
            logger.exception("Invalid node for: %s", ancestors)
            return ("", [""])

        return (node_name, ancestors)

    # pylint: disable=unused-argument
    def _get_sub_nodes(
        self,
        node: Node,
        node_name_separator: str = ".",
        parent_names: list[str] | None = None,
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Retrieve sub-nodes, attributes, and commands of a given node.

        :param node: The node to retrieve sub-nodes from.
        :param node_name_separator: Separator for node names (default is '.').
        :param parent_names: List of parent node names (default is None).
        :return: A tuple containing dictionaries of nodes, attributes, and commands.
        """
        nodes: NodeDict = {}
        attributes: AttrDict = {}
        commands: CmdDict = {}
        node_name, ancestors = self.generate_full_node_name(node, parent_names)
        # Do not add the InputArgument and OutputArgument nodes.
        if (
            node_name.endswith(".InputArguments", node_name.rfind(".")) is True
            or node_name.endswith(".OutputArguments", node_name.rfind(".")) is True
        ):
            return nodes, attributes, commands

        node_class = (
            asyncio.run_coroutine_threadsafe(node.read_node_class(), self.event_loop)
            .result()
            .value
        )
        nodes[node_name] = (node, node_class)
        # node_class = 1: Normal node with children
        # node_class = 2: Attribute
        # node_class = 4: Method
        if node_class == 1:
            children = asyncio.run_coroutine_threadsafe(
                node.get_children(), self.event_loop
            ).result()
            child_nodes: NodeDict = {}
            for child in children:
                child_nodes, child_attributes, child_commands = self._get_sub_nodes(
                    child, parent_names=ancestors
                )
                nodes.update(child_nodes)
                attributes.update(child_attributes)
                commands.update(child_commands)
        elif node_class == 2:
            # An attribute. Add it to the attributes dict.
            # attributes[node_name] = node.get_value
            #
            # Check if RO or RW and call the respective creator functions.
            # if node.figure_out_if_RW_or_RO is RW:
            attributes[node_name] = create_rw_attribute(
                node, self.event_loop, node_name
            )
            # else:
            # attributes[node_name] = create_ro_attribute(
            #     node, self.event_loop, node_name
            # )
        elif node_class == 4:
            # A command. Add it to the commands dict.
            commands[node_name] = self._create_command_function(
                node, self.event_loop, node_name, node_name != Command.TAKE_AUTH.value
            )
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

    def get_attribute_data_type(self, attribute: str | ua.uatypes.NodeId) -> str:
        """
        Get the data type for the given node.

        Returns string for the type or "Unknown" for a not yet known type.
        """
        dt_id = ""
        if isinstance(attribute, str):
            if attribute == "Pointing.Status.CurrentPointing":
                # Special case where the ICD type is Double but the node actually
                # returns a 7-element array.
                return attribute

            node, _ = self.nodes[attribute]
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
            return dt_name

        # load_data_type_definitions() called in connect() adds new classes to the
        # asyncua.ua module.
        if dt_name in dir(ua):
            if issubclass(getattr(ua, dt_name), enum.Enum):
                return "Enumeration"

        return "Unknown"

    def _enum_fields_out_of_order(
        self,
        index: int,
        field: ua.uaprotocol_auto.EnumField,
        enum_node: ua.uatypes.NodeId,
    ) -> str:
        """
        Check if the fields of an enumeration are out of order.

        :param index: The expected index of the field.
        :param field: The enumeration field to be checked.
        :param enum_node: The NodeId of the enumeration.
        :return: A string indicating the error if fields are out of order.
        """
        enum_name = (
            asyncio.run_coroutine_threadsafe(
                enum_node.read_browse_name(), self.event_loop
            )
            .result()
            .Name
        )
        logger.error(
            "Incorrect index for field %s of enumeration %s. Expected: %d, actual: %d",
            field.Name,
            enum_name,
            index,
            field.Value,
        )
        return (
            f"ERROR: incorrect index for {field.Name}; expected: {index} "
            f"actual: {field.Value}"
        )

    def get_enum_strings(self, enum_node: str | ua.uatypes.NodeId) -> list[str]:
        """
        Get list of enum strings where the index of the list matches the enum value.

        enum_node MUST be the name of an Enumeration type node (see
        get_attribute_data_type()).
        """
        dt_id = ""
        if isinstance(enum_node, str):
            node, _ = self.nodes[enum_node]
            dt_id = asyncio.run_coroutine_threadsafe(
                node.read_data_type(), self.event_loop
            ).result()
        elif isinstance(enum_node, ua.uatypes.NodeId):
            dt_id = enum_node

        dt_node = self._client.get_node(dt_id)
        dt_node_def = asyncio.run_coroutine_threadsafe(
            dt_node.read_data_type_definition(), self.event_loop
        ).result()

        return [
            (
                field.Name
                if field.Value == index
                else self._enum_fields_out_of_order(index, field, dt_node)
            )
            for index, field in enumerate(dt_node_def.Fields)
        ]

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
    # pylint: disable=dangerous-default-value
    def subscribe(
        self,
        attributes: str | list[str],
        period: int = 100,
        data_queue: queue.Queue | None = None,
        bad_shutdown_callback: Callable[[str], None] | None = None,
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
        """
        if data_queue is None:
            data_queue = self._subscription_queue
        subscription_handler = SubscriptionHandler(
            data_queue, self._nodes_reversed, bad_shutdown_callback
        )
        if not isinstance(attributes, list):
            attributes = [
                attributes,
            ]
        nodes: set[Node] = set()
        missing_nodes = []
        for attribute in attributes:
            if attribute != "":
                if attribute in self.nodes:
                    nodes.add(self.nodes[attribute][0])
                else:
                    missing_nodes.append(attribute)
        if len(missing_nodes) > 0:
            logger.warning(
                "The following OPC-UA attributes not found in nodes dict and not "
                "subscribed for event updates: %s",
                missing_nodes,
            )
        subscription = asyncio.run_coroutine_threadsafe(
            self._client.create_subscription(period, subscription_handler),
            self.event_loop,
        ).result()
        handles = []
        bad_nodes = set()
        for node in nodes:
            try:
                handle = asyncio.run_coroutine_threadsafe(
                    subscription.subscribe_data_change(node), self.event_loop
                ).result()
                handles.append(handle)
            except Exception as e:
                msg = f"Failed to subscribe to node '{node.nodeid.to_string()}'"
                asyncio.run_coroutine_threadsafe(
                    handle_exception(e, msg), self.event_loop
                )
                bad_nodes.add(node)
        uid = time.monotonic_ns()
        self._subscriptions[uid] = {
            "handles": handles,
            "nodes": nodes - bad_nodes,
            "subscription": subscription,
        }
        return uid, missing_nodes, list(bad_nodes)

    def unsubscribe(self, uid: int) -> None:
        """
        Unsubscribe a user from a subscription.

        :param uid: The ID of the user to unsubscribe.
        """
        subscription = self._subscriptions.pop(uid)
        _ = asyncio.run_coroutine_threadsafe(
            subscription["subscription"].delete(), self.event_loop
        ).result()

    def unsubscribe_all(self) -> None:
        """Unsubscribe all subscriptions."""
        while len(self._subscriptions) > 0:
            _, subscription = self._subscriptions.popitem()
            _ = asyncio.run_coroutine_threadsafe(
                subscription["subscription"].delete(), self.event_loop
            ).result()

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
        if self._session_id is None:
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
        # One call in case mode is "New"
        result_code, result_msg = await self._load_next_points(mode)
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

            result_code, result_msg = await self._load_next_points()
        logger.debug("Async track table loading: %s", result_msg)

    async def _load_next_points(self, mode: int = 0) -> tuple[ResultCode, str]:
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

        if self._session_id is None:
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
        track_load_node = self.nodes[Command.TRACK_LOAD_TABLE.value][0]
        result_code = await track_load_node.call_method(track_load_node, *load_args)
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


def SCU_from_config(  # noqa: N802
    server_name: str,
    ini_file: str = "disq.ini",
    use_nodes_cache: bool = True,
    authority_name: str | None = None,
) -> SteeringControlUnit | None:
    """SCU object generator method.

    This method creates an SCU object based on OPC-UA server_name connection details
    found in the ini_file configuration file. It then connects the SCU object to the
    OPC-UA server and sets it up, ready to be used with the attributes and commands.

    The method also configures logging based on the log configuration file:
    "disq_logging_config.yaml".

    :return: an initialised and connected instance of the SteeringControlUnit class or
        None if the connection to the OPC-UA server failed.
    """
    configuration.configure_logging()
    sculib_args: dict = configuration.get_config_sculib_args(
        ini_file, server_name=server_name
    )
    # TODO: figure out a neat way to handle conversion of config variables from string
    if "timeout" in sculib_args:
        sculib_args["timeout"] = float(sculib_args["timeout"].strip())
    if "port" in sculib_args:
        sculib_args["port"] = int(sculib_args["port"].strip())
    try:
        scu = SCU(
            **sculib_args,
            use_nodes_cache=use_nodes_cache,
            authority_name=authority_name,
        )
    except ConnectionRefusedError:
        logger.error(
            "Failed to connect to the OPC-UA server with connection parameters: %s",
            str(sculib_args),
        )
        return None
    return scu
