"""System Control Unit library for an SKA-Mid Dish OPC UA server."""

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
from enum import Enum
from functools import cached_property
from importlib import metadata, resources
from pathlib import Path
from typing import Any, Callable, Final, Type, TypedDict

import numpy
import yaml  # type: ignore
from asyncua import Client, Node, ua
from asyncua.crypto.cert_gen import setup_self_signed_certificate
from asyncua.crypto.security_policies import SecurityPolicyBasic256
from cryptography.x509.oid import ExtendedKeyUsageOID
from packaging.version import InvalidVersion, Version
from platformdirs import user_cache_dir

logger = logging.getLogger("sculib")

# Type aliases
NodeDict = dict[str, tuple[Node, int]]
AttrDict = dict[str, object]
CmdReturn = tuple[int, str, list[int | None] | None]
CmdDict = dict[str, Callable[..., CmdReturn]]

PACKAGE_VERSION: Final = metadata.version("DiSQ")
USER_CACHE_DIR: Final = Path(user_cache_dir(appauthor="SKAO", appname="DiSQ"))
COMPATIBLE_CETC_SIM_VER: Final = Version("3.2.3")


def configure_logging(default_log_level: int = logging.INFO) -> None:
    """
    Configure logging settings based on a YAML configuration file.

    :param default_log_level: The default logging level to use if no configuration file
        is found. Defaults to logging.INFO.
    :type default_log_level: int
    :raises ValueError: If an error occurs while configuring logging from the file.
    """
    disq_log_config_file = Path("disq_logging_config.yaml")
    if disq_log_config_file.exists() is False:
        disq_log_config_file = Path(
            resources.files(__package__) / "default_logging_config.yaml"  # type: ignore
        )
    config = None
    if disq_log_config_file.exists():
        with open(disq_log_config_file, "rt", encoding="UTF-8") as f:
            try:
                config = yaml.safe_load(f.read())
            except Exception as e:
                print(f"{type(e).__name__}: '{e}'")
                print(
                    "WARNING: Unable to read logging configuration file "
                    f"{disq_log_config_file}"
                )
        try:
            at_time = datetime.time.fromisoformat(
                config["handlers"]["file_handler"]["atTime"]
            )
            config["handlers"]["file_handler"]["atTime"] = at_time
        except KeyError as e:
            print(f"WARNING: {e} not found in logging configuration for file_handler")
    else:
        print(f"WARNING: Logging configuration file {disq_log_config_file} not found")

    if config is None:
        print("Reverting to basic logging config at level:{default_log_level}")
        logging.basicConfig(level=default_log_level)
    else:
        Path("logs").mkdir(parents=True, exist_ok=True)
        try:
            logging.config.dictConfig(config)
        except ValueError as e:
            print(f"{type(e).__name__}: '{e}'")
            print(
                "WARNING: Caught exception. Unable to configure logging from file "
                f"{disq_log_config_file}. Reverting to logging to the console "
                "(basicConfig)."
            )
            logging.basicConfig(level=default_log_level)


async def handle_exception(e: Exception, msg: str = "") -> None:
    """
    Handle and log an exception.

    :param e: The exception that was caught.
    :type e: Exception
    """
    logger.exception("*** Exception caught: %s [context: %s]", e, msg)


def create_rw_attribute(
    node: Node, event_loop: asyncio.AbstractEventLoop, node_name: str
) -> object:
    """
    Create a read-write attribute for an OPC UA node.

    :param node: The OPC UA node to create the attribute for.
    :type node: asyncua.Node

    :param event_loop: The asyncio event loop to use for running coroutines.
    :type event_loop: asyncio.AbstractEventLoop

    :param node_name: The name of the OPC UA node.
    :type node_name: str

    :return: An instance of a read-write attribute for the given OPC UA node.
    :rtype: opc_ua_rw_attribute

    The opc_ua_rw_attribute class has the following methods:

    - value: A property that gets the value of the OPC UA node.
    - value.setter: A setter method that sets the value of the OPC UA node.

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
    :type node: asyncua.Node
    :param event_loop: The asyncio event loop to use.
    :type event_loop: asyncio.AbstractEventLoop
    :param node_name: The name of the OPC UA node.
    :type node_name: str
    :return: An object with a read-only 'value' property.
    :rtype: opc_ua_ro_attribute
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
    :type subscription_queue: queue.Queue
    :param nodes: A dictionary mapping nodes to their names.
    :type nodes: dict
    """

    def __init__(self, subscription_queue: queue.Queue, nodes: dict[Node, str]) -> None:
        """
        Initialize the object with a subscription queue and nodes.

        :param subscription_queue: A queue for subscriptions.
        :type subscription_queue: queue.Queue
        :param nodes: A dictionary of nodes.
        :type nodes: dict
        """
        self.subscription_queue = subscription_queue
        self.nodes = nodes

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


class CachedNodesDict(TypedDict):
    """Cached nodes dictionary type."""

    node_ids: dict[str, tuple[str, int]]
    timestamp: str


class Command(Enum):
    """
    Commands of Dish Controller used in SCU methods.

    This needs to be kept up to date with the ICD.
    """

    TAKE_AUTH = "CommandArbiter.Commands.TakeAuth"
    RELEASE_AUTH = "CommandArbiter.Commands.ReleaseAuth"
    ACTIVATE = "Management.Commands.Activate"
    DEACTIVATE = "Management.Commands.DeActivate"
    MOVE2BAND = "Management.Commands.Move2Band"
    RESET = "Management.Commands.Reset"
    SLEW2ABS_AZ_EL = "Management.Commands.Slew2AbsAzEl"
    SLEW2ABS_SINGLE_AX = "Management.Commands.Slew2AbsSingleAx"
    STOP = "Management.Commands.Stop"
    STOW = "Management.Commands.Stow"
    AMBTEMP_CORR_SETUP = "Pointing.Commands.AmbTempCorrSetup"
    PM_CORR_ON_OFF = "Pointing.Commands.PmCorrOnOff"
    STATIC_PM_SETUP = "Pointing.Commands.StaticPmSetup"
    INTERLOCK_ACK = "Safety.Commands.InterlockAck"
    TRACK_LOAD_STATIC_OFF = "Tracking.Commands.TrackLoadStaticOff"
    TRACK_LOAD_TABLE = "Tracking.Commands.TrackLoadTable"
    TRACK_START = "Tracking.Commands.TrackStart"


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class SCU:
    """
    System Control Unit.

    Small library that eases the pain when connecting to an OPC UA server and calling
    methods on it, reading or writing attributes.

    How to:
    - Import the library:

    ```
    from sculib import SCU, Command
    ```
    - Instantiate an SCU object and connect to the server. Provided here are the
      defaults which can be overwritten by specifying the named parameter:

    ```
    scu = SCU(host="localhost", port=4840, endpoint="", namespace="", timeout=10.0)
    scu.connect_and_setup()
    # Do things
    scu.disconnect_and_cleanup()
    ```
    - Or altenatively SCU can be used as a context manager without calling the setup and
      teardown methods explicitly:

    ```
    with SCU(host="localhost") as scu:
        scu.take_authority("LMC")
    ```
    - All nodes from and including the PLC_PRG node are stored in the nodes dictionary:
      `scu.nodes`. The keys are the full node names, the values are the Node objects.
      The full names of all nodes can be retrieved with:

    ```
    scu.get_node_list()
    ```
    - Every value in `scu.nodes` exposes the full OPC UA functionality for a node.
      Note: When accessing nodes directly, it is mandatory to await any calls:

    ```
    node = scu.nodes['PLC_PRG']
    node_name = (await node.read_display_name()).Text
    ```
    - The methods that are below the PLC_PRG node's hierarchy can be accessed through
      the commands dictionary:

    ```
    scu.get_command_list()
    ```
    - When you want to call a command, please check the ICD for the parameters that the
      commands expects. Checking for the correctness of the parameters is not done here
      in sculib but in the PLC's OPC UA server. Once the parameters are in order,
      calling a command is really simple:

    ```
    result = scu.commands['COMMAND_NAME'](YOUR_PARAMETERS)
    ```
    - You can also use the Command enum, as well as helper method for converting types
      from the OPC UA server to the correct base integer type:

    ```
    axis = scu.convert_enum_to_int("AxisSelectType", "Az")
    result = scu.commands[Command.ACTIVATE.value](axis)
    ```
    - For instance, command the PLC to slew to a new position:

    ```
    az = 182.0; el = 21.8; az_v = 1.2; el_v = 2.1
    code, msg, _ = scu.commands[Command.SLEW2ABS_AZ_EL.value](az, el, az_v, el_v)
    ```
    - The OPC UA server also provides read-writeable and read-only variables, commonly
      called in OPC UA "attributes". An attribute's value can easily be read:

    ```
    scu.attributes['Azimuth.p_Set'].value
    ```
    - If an attribute is writable, then a simple assignment does the trick:

    ```
    scu.attributes['Azimuth.p_Set'].value = 1.2345
    ```
    - In case an attribute is not writeable, the OPC UA server will report an error:

    ```
    scu.attributes['Azimuth.p_Set'].value = 10
    *** Exception caught
    "User does not have permission to perform the requested operation."
    (BadUserAccessDenied)
    ```
    """  # noqa: RST201,RST203,RST214,RST301

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
        app_name: str = f"DiSQ.sculib v{PACKAGE_VERSION}",
    ) -> None:
        """
        Initialise SCU with the provided parameters.

        :param host: The hostname or IP address of the server. Default is 'localhost'.
        :type host: str
        :param port: The port number of the server. Default is 4840.
        :type port: int
        :param endpoint: The endpoint on the server. Default is ''.
        :type endpoint: str
        :param namespace: The namespace for the server. Default is ''.
        :type namespace: str
        :param username: The username for authentication. Default is None.
        :type username: str | None
        :param password: The password for authentication. Default is None.
        :type password: str | None
        :param timeout: The timeout value for connection. Default is 10.0.
        :type timeout: float
        :param eventloop: The asyncio event loop to use. Default is None.
        :type eventloop: asyncio.AbstractEventLoop | None
        :param gui_app: Whether the instance is for a GUI application. Default is False.
        :type gui_app: bool
        :param use_nodes_cache: Whether to use any existing caches of node IDs.
        :type use_nodes_cache: bool
        :param app_name: application name for OPC UA client description.
        :type app_name: str
        """
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
        self._app_name = app_name
        # Other local variables
        self.client: Client | None = None
        self.event_loop_thread: threading.Thread | None = None
        self.subscription_handler = None
        self.subscriptions: dict = {}
        self.subscription_queue: queue.Queue = queue.Queue()
        self._user: int | None = None
        self._session_id: ua.UInt16 | None = None
        self._server_url: str
        self._server_str_id: str
        self.plc_prg: Node
        self.ns_idx: int
        self.nodes: NodeDict
        self.nodes_reversed: dict[tuple[Node, int], str]
        self.attributes: AttrDict
        self.commands: CmdDict
        self._plc_prg_nodes_timestamp: str
        self.parameter: Node
        self.parameter_ns_idx: int
        self.parameter_nodes: NodeDict
        self.parameter_attributes: AttrDict
        self.parameter_commands: CmdDict
        self.server: Node
        self.server_nodes: NodeDict
        self.server_attributes: AttrDict
        self.server_commands: CmdDict

    def connect_and_setup(self) -> None:
        """
        Connect to the server and setup the SCU client.

        :raises Exception: If connection to OPC UA server fails.
        """
        try:
            self.client = self.connect()
        except Exception as e:
            msg = (
                "Cannot connect to the OPC UA server. Please "
                "check the connection parameters that were "
                "passed to instantiate the sculib!"
            )
            logger.error("%s - %s", msg, e)
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
                        f"DiSQ-SCU not compatible with v{self.server_version} of CETC "
                        f"simulator, only v{COMPATIBLE_CETC_SIM_VER} and up"
                    )
            except InvalidVersion:
                logger.warning(
                    "Server version (%s) does not conform to semantic versioning",
                    self.server_version,
                )
        self.populate_node_dicts(self._gui_app, self._use_nodes_cache)
        logger.info("Successfully connected to server and initialised SCU client")

    def __enter__(self) -> "SCU":
        """Connect to the server and setup the SCU client."""
        self.connect_and_setup()
        return self

    def disconnect_and_cleanup(self) -> None:
        """
        Disconnect from server and clean up client resources.

        This method unsubscribes from all subscriptions, disconnects from the server,
        and stops the event loop if it was started in a separate thread.
        """
        self.release_authority()
        self.unsubscribe_all()
        self.disconnect()
        if self.event_loop_thread is not None:
            # Signal the event loop thread to stop.
            self.event_loop.call_soon_threadsafe(self.event_loop.stop)
            # Join the event loop thread once it is done processing tasks.
            self.event_loop_thread.join()

    def __exit__(self, *args: Any) -> None:
        """Disconnect from server and clean up client resources."""
        self.disconnect_and_cleanup()

    @cached_property
    def server_version(self) -> str | None:
        """
        The software/firmware version of the server that SCU is connected to.

        :return: The version of the server, or None if the server version info could
            not be read successfully.
        :rtype: str
        """
        try:
            version_node = asyncio.run_coroutine_threadsafe(
                self.client.nodes.objects.get_child(
                    [
                        f"{self.ns_idx}:Logic",
                        f"{self.ns_idx}:Application",
                        f"{self.ns_idx}:PLC_PRG",
                        f"{self.ns_idx}:Management",
                        f"{self.ns_idx}:NamePlate",
                        f"{self.ns_idx}:DscSoftwareVersion",
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
    def plc_prg_nodes_timestamp(self) -> str:
        """
        Generation timestamp of the PLC_PRG Node tree.

        :return: timestamp in 'yyyy-mm-dd hh:mm:ss' string format.
        :rtype: str
        """
        return self._plc_prg_nodes_timestamp

    def run_event_loop(
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
        :type event_loop: asyncio.AbstractEventLoop
        :param thread_started_event: The threading event signaling that the thread has
            started.
        :type thread_started_event: threading.Event
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
        self.event_loop_thread = threading.Thread(
            target=self.run_event_loop,
            args=(event_loop, thread_started_event),
            name=f"asyncio event loop for sculib instance {self.__class__.__name__}",
            daemon=True,
        )
        self.event_loop_thread.start()
        thread_started_event.wait(5.0)  # Wait for the event loop thread to start

    def set_up_encryption(self, client: Client, user: str, pw: str) -> None:
        """
        Set up encryption for the client with the given user credentials.

        :param client: The client to set up encryption for.
        :type client: Client
        :param user: The username for the server.
        :type user: str
        :param pw: The password for the server.
        :type pw: str
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

    # pylint: disable=too-many-arguments
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
            self.set_up_encryption(client, self.username, self.password)
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
            self.parameter_ns_idx = asyncio.run_coroutine_threadsafe(
                client.get_namespace_index(
                    "http://boschrexroth.com/OpcUa/Parameter/Objects/"
                ),
                self.event_loop,
            ).result()
        except Exception:
            self.parameter_ns_idx = None
            message = (
                "*** Exception caught while trying to access the namespace "
                "'http://boschrexroth.com/OpcUa/Parameter/Objects/' for the parameter "
                "nodes on the OPC UA server. "
                "From now on it will be assumed that the CETC54 simulator is running."
            )
            logger.warning(message)

        try:
            if self.namespace != "" and self.endpoint != "":
                self.ns_idx = asyncio.run_coroutine_threadsafe(
                    client.get_namespace_index(self.namespace), self.event_loop
                ).result()
            else:
                # Force namespace index for first physical controller
                self.ns_idx = 2
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
            logger.error(message)
            # e.add_note(message)
            raise e
        return client

    def disconnect(self, client: Client | None = None) -> None:
        """
        Disconnect a client connection.

        :param client: The client to disconnect. If None, disconnect self.client.
        :type client: Client
        """
        if client is None and self.client is not None:
            client = self.client
            self.client = None
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
        :rtype: bool
        """
        return self.client is not None

    def take_authority(self, user: str | int) -> tuple[int, str]:
        """
        Take command authority.

        :param user: Authority user name - DscCmdAuthorityType enum.
        :type user: str | int (ua.DscCmdAuthorityType)
        :return: The result of the command execution.
        :rtype: tuple[int, str]
        """
        user_int = (
            self.convert_enum_to_int("DscCmdAuthorityType", user)
            if isinstance(user, str)
            else user
        )
        if user_int == 2:  # HHP
            code = -1
            msg = "DiSQ-SCU cannot take authority as HHP user"
            logger.info("TakeAuth command not executed, as %s", msg)
        elif self._user is None or (self._user is not None and self._user < user_int):
            code, msg, vals = self.commands[Command.TAKE_AUTH.value](
                ua.UInt16(user_int)
            )
            if code == 10:  # CommandDone
                self._user = user_int
                self._session_id = ua.UInt16(vals[0])
            else:
                logger.error("TakeAuth command failed with message '%s'", msg)
        else:
            user_str = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
            code = -1
            msg = f"DiSQ-SCU already has command authority with user {user_str}"
            logger.info("TakeAuth command not executed, as %s", msg)
        return code, msg

    def release_authority(self) -> tuple[int, str]:
        """
        Release command authority.

        :return: The result of the command execution.
        :rtype: tuple[int, str]
        """
        if self._user is not None and self._session_id is not None:
            code, msg, _ = self.commands[Command.RELEASE_AUTH.value](
                ua.UInt16(self._user)
            )
            if code == 10:  # CommandDone
                self._user, self._session_id = None, None
            elif code in [0, 4]:  # NoCmdAuth, CommandFailed
                user = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
                logger.info(
                    "DiSQ-SCU has already lost command authority as user '%s' to "
                    "another client.",
                    user,
                )
                self._user, self._session_id = None, None
        else:
            code = -1
            msg = "DiSQ-SCU has no command authority to release."
            logger.info(msg)
        return code, msg

    def convert_enum_to_int(self, enum_type: str, name: str) -> ua.UInt16 | None:
        """
        Convert the name (string) of the given enumeration type to an integer value.

        :param enum_type: the name of the enumeration type to use.
        :type enum_type: str
        :param name: of enum to convert.
        :type name: str
        :return: enum integer value, or None if the type does not exist.
        :rtype: int | None
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
        :type enum_type: str
        :param value: of enum to convert.
        :type value: int
        :return: enum name, or the original integer value if the type does not exist.
        :rtype: str | int
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

    @cached_property
    def opcua_enum_types(self) -> dict[str, Type[Enum]]:
        """
        Retrieve a dictionary of OPC-UA enum types.

        :return: A dictionary mapping OPC-UA enum type names to their corresponding
            value. The value being an enumerated type.
        :rtype: dict
        :raises AttributeError: If any of the required enum types are not found in the
            UA namespace.
        """
        result = {}
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
                result.update({type_name: getattr(ua, type_name)})
            except AttributeError:
                try:
                    enum_node = self.client.get_node(
                        f"ns={self.ns_idx};s=@{type_name}.EnumValues"
                    )
                    enum_dict = self._create_enum_from_node(type_name, enum_node)
                    result.update({type_name: enum_dict})
                    setattr(ua, type_name, enum_dict)
                except (RuntimeError, ValueError):
                    missing_types.append(type_name)
        if missing_types:
            logger.warning(
                "OPC-UA server does not implement the following Enumerated types "
                "as expected: %s",
                str(missing_types),
            )
        return result

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
    ) -> Callable:
        """
        Create a command function to execute a method on a specified Node.

        :param node: The Node on which the method will be executed.
        :type node: asyncua.Node
        :param event_loop: The asyncio event loop to run the coroutine on.
        :type event_loop: asyncio.AbstractEventLoop
        :param node_name: The full name of the Node.
        :type node_name: str
        :return: A function that can be used to execute a method on the Node.
        :rtype: function
        """
        try:
            node_parent = asyncio.run_coroutine_threadsafe(
                node.get_parent(), event_loop
            ).result()
            call = node_parent.call_method
        except AttributeError as e:
            logger.warning(
                "Caught an exception while trying to get the method for a command.\n"
                "Exception: %s\nNode name: %s\nParent object: %s",
                e,
                node_name,
                node_parent,
            )

            def empty_func(
                *args: Any,  # pylint: disable=unused-argument
            ) -> CmdReturn:
                logger.warning("Command node %s has no method to call.", uid)
                return -1, "No method", None

            return empty_func

        read_name = asyncio.run_coroutine_threadsafe(
            node.read_display_name(), event_loop
        )
        uid = f"{node.nodeid.NamespaceIndex}:{read_name.result().Text}"

        def fn(*args: Any) -> CmdReturn:
            """
            Execute function with arguments and return tuple with return code/message.

            :param args: Optional positional arguments to pass to the function.
            :type args: tuple

            :return: A tuple containing the return code (int), return message (str),
                and a list of other returned values (Any), if any, otherwise None.
            :rtype: tuple[int, str, list[Any] | None]

            :raises: Any exception raised during the execution of the function will be
                handled by the function and a tuple with return code -1 and exception
                message will be returned.

            Note: This function uses asyncio to run the coroutine in a separate thread.
            """
            try:
                cmd_args = (
                    [self._session_id, *args]
                    if self._session_id is not None
                    else [*args]
                )
                logger.debug(
                    "Calling command node '%s' with args list: %s",
                    uid,
                    cmd_args,
                )
                result: int | list[Any] = asyncio.run_coroutine_threadsafe(
                    call(uid, *cmd_args), event_loop
                ).result()
                if isinstance(result, list):
                    return_code: int = result.pop(0)
                    return_vals = result
                else:
                    return_code = result
                    return_vals = None
                if hasattr(ua, "CmdResponseType") and return_code is not None:
                    # The asyncua library has a CmdResponseType enum ONLY if the opcua
                    # server implements the type
                    # pylint: disable=no-member
                    return_msg = str(ua.CmdResponseType(return_code).name)
                else:
                    return_msg = str(return_code)
                if return_code == 0:  # NoCmdAuth
                    user = self.convert_int_to_enum("DscCmdAuthorityType", self._user)
                    logger.info(
                        "DiSQ-SCU has lost command authority as user '%s' to "
                        "another client.",
                        user,
                    )
                    self._user, self._session_id = None, None
                return (return_code, return_msg, return_vals)
            except Exception as e:
                # e.add_note(f'Command: {uid} args: {args}')
                asyncio.run_coroutine_threadsafe(
                    handle_exception(e, f"Command: {uid} ({node_name}), args: {args}"),
                    event_loop,
                )
                return -1, f"asyncua exception: {str(e)}", None

        return fn

    def _load_json_file(self, file_path: Path) -> dict[str, CachedNodesDict]:
        """
        Load JSON file.

        :param file_path: of JSON file to load.
        :type file_path: Path
        :return: decoded JSON file contents as nested dictionary,
            or empty dict if the file does not exists.
        :rtype: dict
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
        :type file_path: Path
        :param nodes: dictionary of Nodes to cache.
        :type nodes: NodeDict
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

    def populate_node_dicts(
        self, plc_only: bool = False, use_cache: bool = False
    ) -> None:
        """
        Populate dictionaries with Node objects.

        This method populates dictionaries with Node objects for different categories
        like nodes, attributes, and commands.

        nodes: Contains the entire uasync.Node-tree from and including
        the 'PLC_PRG' node.

        attributes: Contains the attributes in the uasync.Node-tree from
        the 'PLC_PRG' node on. The values are callables that return the
        current value.

        commands: Contains all callable methods in the uasync.Node-tree
        from the 'PLC_PRG' node on. The values are callables which
        just require the expected parameters.

        This method may raise exceptions related to asyncio operations.
        """
        top_node_name = "PLC_PRG"
        cache_file_path = USER_CACHE_DIR / f"{top_node_name}.json"
        cache = self._load_json_file(cache_file_path) if use_cache else None
        cached_nodes = cache.get(self._server_str_id) if cache is not None else None

        # Check for existing Nodes IDs cache
        if cached_nodes:
            (
                self.nodes,
                self.attributes,
                self.commands,
            ) = self.generate_node_dicts_from_cache(
                cached_nodes["node_ids"], top_node_name
            )
            self._plc_prg_nodes_timestamp = cached_nodes["timestamp"]
        else:
            plc_prg = asyncio.run_coroutine_threadsafe(
                self.client.nodes.objects.get_child(
                    [
                        f"{self.ns_idx}:Logic",
                        f"{self.ns_idx}:Application",
                        f"{self.ns_idx}:{top_node_name}",
                    ]
                ),
                self.event_loop,
            ).result()
            (
                self.nodes,
                self.attributes,
                self.commands,
            ) = self.generate_node_dicts_from_server(plc_prg, top_node_name)
            self.plc_prg = plc_prg
            self._cache_node_ids(cache_file_path, self.nodes)
            self._plc_prg_nodes_timestamp = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        self.nodes_reversed = {val[0]: key for key, val in self.nodes.items()}

        # We also want the PLC's parameters for the drives and the PLC program.
        # But only if we are not connected to the simulator.
        top_node_name = "Parameter"
        cache_file_path = USER_CACHE_DIR / f"{top_node_name}.json"
        if not plc_only and self.parameter_ns_idx is not None:
            cache = self._load_json_file(cache_file_path) if use_cache else None
            cached_nodes = cache.get(self._server_str_id) if cache is not None else None
            if cached_nodes:
                (
                    self.parameter_nodes,
                    self.parameter_attributes,
                    self.parameter_commands,
                ) = self.generate_node_dicts_from_cache(
                    cached_nodes["node_ids"], top_node_name
                )
            else:
                parameter = asyncio.run_coroutine_threadsafe(
                    self.client.nodes.objects.get_child(
                        [f"{self.parameter_ns_idx}:{top_node_name}"]
                    ),
                    self.event_loop,
                ).result()
                (
                    self.parameter_nodes,
                    self.parameter_attributes,
                    self.parameter_commands,
                ) = self.generate_node_dicts_from_server(parameter, top_node_name)
                self.parameter = parameter
                self._cache_node_ids(cache_file_path, self.parameter_nodes)

        # And now create dicts for all nodes of the OPC UA server. This is
        # intended to serve as the API for the Dish LMC.
        top_node_name = "Root"
        cache_file_path = USER_CACHE_DIR / f"{top_node_name}.json"
        if not plc_only:
            cache = self._load_json_file(cache_file_path) if use_cache else None
            cached_nodes = cache.get(self._server_str_id) if cache is not None else None
            if cached_nodes:
                (
                    self.server_nodes,
                    self.server_attributes,
                    self.server_commands,
                ) = self.generate_node_dicts_from_cache(
                    cached_nodes["node_ids"], top_node_name
                )
            else:
                server = self.client.get_root_node()
                (
                    self.server_nodes,
                    self.server_attributes,
                    self.server_commands,
                ) = self.generate_node_dicts_from_server(server, top_node_name)
                self.server = server
                self._cache_node_ids(cache_file_path, self.server_nodes)

    def generate_node_dicts_from_cache(
        self, cache_dict: dict[str, tuple[str, int]], top_level_node_name: str
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Generate dicts for nodes, attributes, and commands from a cache.

        :param top_level_node_name: Name of the top-level node.
        :type top_level_node_name: str
        :return: A tuple containing dictionaries for nodes, attributes and commands.
        :rtype: tuple
        """
        logger.info(
            "Generating dicts of '%s' node's tree from existing cache in %s",
            top_level_node_name,
            USER_CACHE_DIR,
        )
        nodes = {}
        attributes = {}
        commands = {}
        for node_name, tup in cache_dict.items():
            node_id, node_class = tup
            node = self.client.get_node(node_id)
            nodes[node_name] = (node, node_class)
            if node_class == 2:
                # An attribute. Add it to the attributes dict.
                attributes[node_name] = create_rw_attribute(
                    node, self.event_loop, node_name
                )
            elif node_class == 4:
                # A command. Add it to the commands dict.
                commands[node_name] = self._create_command_function(
                    node, self.event_loop, node_name
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

        This function is part of a class and takes the top-level node and an optional
        name for it as input. It then generates dictionaries for nodes, attributes, and
        commands based on the structure of the top-level node and returns a tuple
        containing these dictionaries.

        The dictionaries contain mappings of keys to nodes, attributes, and commands.

        :param top_level_node: The top-level node for which to generate dictionaries.
        :type top_level_node: Node
        :param top_level_node_name: Name of the top-level node.
        :type top_level_node_name: str
        :return: A tuple containing dictionaries for nodes, attributes, and commands.
        :rtype: tuple
        """
        logger.info(
            "Generating dicts of '%s' node's tree from server. It may take a while...",
            top_level_node_name,
        )
        nodes, attributes, commands = self.get_sub_nodes(top_level_node)
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
        :type node: asyncua.Node
        :param parent_names: A list of parent node names. Defaults to None if the node
            has no parent.
        :type parent_names: list[str] | None
        :param node_name_separator: The separator used to join the node and parent
            names. Default is '.'.
        :type node_name_separator: str
        :return: A tuple containing the full node name and a list of ancestor names.
        :rtype: tuple
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
    def get_sub_nodes(
        self,
        node: Node,
        node_name_separator: str = ".",
        parent_names: list[str] | None = None,
    ) -> tuple[NodeDict, AttrDict, CmdDict]:
        """
        Retrieve sub-nodes, attributes, and commands of a given node.

        :param node: The node to retrieve sub-nodes from.
        :type node: asyncua.Node
        :param node_name_separator: Separator for node names (default is '.').
        :type node_name_separator: str
        :param parent_names: List of parent node names (default is None).
        :type parent_names: list[str] | None
        :return: A tuple containing dictionaries of nodes, attributes, and commands.
        :rtype: tuple
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
                child_nodes, child_attributes, child_commands = self.get_sub_nodes(
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
                node, self.event_loop, node_name
            )
        return nodes, attributes, commands

    def get_node_list(self) -> list[str]:
        """
        Get a list of node names.

        :return: A list of node names.
        :rtype: list[str]
        """
        return self.__get_node_list(self.nodes)

    def get_command_list(self) -> list[str]:
        """
        Get the list of commands associated with this object.

        :return: A list of strings representing the commands.
        :rtype: list[str]
        """
        return self.__get_node_list(self.commands)

    def get_attribute_list(self) -> list[str]:
        """
        Get the list of attributes.

        :return: A list of attribute names.
        :rtype: list[str]
        """
        return self.__get_node_list(self.attributes)

    def get_parameter_node_list(self) -> list[str]:
        """
        Get a list of parameter nodes.

        :return: A list of parameter nodes.
        :rtype: list[str]
        """
        return self.__get_node_list(self.parameter_nodes)

    def get_parameter_command_list(self) -> list[str]:
        """
        Get a list of parameter commands.

        :return: A list of parameter commands.
        :rtype: list[str]
        """
        return self.__get_node_list(self.parameter_commands)

    def get_parameter_attribute_list(self) -> list[str]:
        """
        Return a list of parameter attributes.

        :return: A list of parameter attribute names.
        :rtype: list[str]
        """
        return self.__get_node_list(self.parameter_attributes)

    def get_server_node_list(self) -> list[str]:
        """
        Get the list of server nodes.

        :return: A list of server node names.
        :rtype: list[str]
        """
        return self.__get_node_list(self.server_nodes)

    def get_server_command_list(self) -> list[str]:
        """
        Get the list of server commands.

        :return: A list of server command strings.
        :rtype: list[str]
        """
        return self.__get_node_list(self.server_commands)

    def get_server_attribute_list(self) -> list[str]:
        """
        Get a list of server attributes.

        :return: A list of server attributes.
        :rtype: list[str]
        """
        return self.__get_node_list(self.server_attributes)

    def __get_node_list(self, nodes: dict) -> list[str]:
        """
        Get a list of node keys from a dictionary of nodes.

        :param nodes: A dictionary of nodes where keys are node identifiers.
        :type nodes: dict
        :return: A list of node keys.
        :rtype: list
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
            node, _ = self.nodes[attribute]
            dt_id = asyncio.run_coroutine_threadsafe(
                node.read_data_type(), self.event_loop
            ).result()
        elif isinstance(attribute, ua.uatypes.NodeId):
            dt_id = attribute

        dt_node = self.client.get_node(dt_id)
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
        :type index: int
        :param field: The enumeration field to be checked.
        :type field: asyncua.ua.uaprotocol_auto.EnumField
        :param enum_node: The NodeId of the enumeration.
        :type enum_node: asyncua.ua.uatypes.NodeId
        :return: A string indicating the error if fields are out of order.
        :rtype: str
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

        dt_node = self.client.get_node(dt_id)
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
        :type node_list: list[str]
        :return: A list of tuples containing the node names and their descriptions.
        :rtype: tuple(str, str|None)
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

    # pylint: disable=dangerous-default-value
    def subscribe(
        self,
        attributes: str | list[str],
        period: int = 100,
        data_queue: queue.Queue | None = None,
    ) -> tuple[int, list, list]:
        """
        Subscribe to OPC-UA attributes for event updates.

        :param attributes: A single OPC-UA attribute or a list of attributes to
            subscribe to.
        :type attributes: str or list[str]
        :param period: The period in milliseconds for checking attribute updates.
        :type period: int
        :param data_queue: A queue to store the subscribed attribute data. If None, uses
            the default subscription queue.
        :type data_queue: queue.Queue
        :return: unique identifier for the subscription and lists of missing/bad nodes.
        :rtype: tuple[int, list, list]
        """
        if data_queue is None:
            data_queue = self.subscription_queue
        subscription_handler = SubscriptionHandler(data_queue, self.nodes_reversed)
        if not isinstance(attributes, list):
            attributes = [
                attributes,
            ]
        nodes: set[Node] = set()
        missing_nodes = []
        for attribute in attributes:
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
            self.client.create_subscription(period, subscription_handler),
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
        self.subscriptions[uid] = {
            "handles": handles,
            "nodes": nodes - bad_nodes,
            "subscription": subscription,
        }
        return uid, missing_nodes, list(bad_nodes)

    def unsubscribe(self, uid: int) -> None:
        """
        Unsubscribe a user from a subscription.

        :param uid: The ID of the user to unsubscribe.
        :type uid: int
        """
        subscription = self.subscriptions.pop(uid)
        _ = asyncio.run_coroutine_threadsafe(
            subscription["subscription"].delete(), self.event_loop
        ).result()

    def unsubscribe_all(self) -> None:
        """Unsubscribe all subscriptions."""
        while len(self.subscriptions) > 0:
            _, subscription = self.subscriptions.popitem()
            _ = asyncio.run_coroutine_threadsafe(
                subscription["subscription"].delete(), self.event_loop
            ).result()

    def get_subscription_values(self) -> list[dict]:
        """
        Get the values from the subscription queue.

        :return: A list of dictionaries containing the values from the subscription
            queue.
        :rtype: list[dict]
        """
        values = []
        while not self.subscription_queue.empty():
            values.append(self.subscription_queue.get(block=False, timeout=0.1))
        return values

    def load_track_table_file(self, file_name: str) -> numpy.ndarray:
        """
        Load a track table file to upload with the load_program_track command.

        - positions = self.load_track_table_file(
            os.getenv('HOME')+'/Downloads/radial.csv')
        - await scu.load_program_track(asyncua.uaLoadEnumTypes.New,
            len(positions),
            positions[:, 0],
            positions[:, 1],
            positions[:, 2])

        :param str file_name: File name of the track table file including its path.
        :return: 3d numpy array that contains [time offset, az position, el position]
        :rtype: numpy.array
        """
        try:
            lines = []
            # Load the track table file.
            with open(file_name, "r", encoding="utf-8") as f:
                lines = f.readlines()
            # Remove the header line because it does not contain a position.
            lines.pop(0)
            # Remove a trailing '\n' and split the cleaned line at every ','.
            cleaned_lines = [line.rstrip("\n").split(",") for line in lines]
            # Return an array that contains the time offsets and positions.
            return numpy.array(cleaned_lines, dtype=float)
        except Exception as e:
            logger.error(
                "Could not load or convert the track table file '%s': %s",
                file_name,
                e,
            )
            raise e

    def track_table_reset_and_upload_from_file(self, file_name: str) -> None:
        """
        Direct upload a track table to the dish structure's OPC UA server.

        :param file_name: File name of the track table file including its path.
        :type file_name: str
        """
        # pylint: disable=no-member
        positions = self.load_track_table_file(file_name)
        # Reset the currently loaded track table.
        zero = numpy.zeros(1)
        self.load_program_track(ua.LoadModeType.Reset, 0, zero, zero, zero)
        # Submit the new track table.
        self.load_program_track(
            ua.LoadModeType.New,
            len(positions),
            positions[:, 0],
            positions[:, 1],
            positions[:, 2],
        )

    # commands to DMC state - dish management controller
    def interlock_acknowledge_dmc(self) -> CmdReturn:
        """
        Acknowledge the interlock status for the DMC.

        :return: The result of acknowledging the interlock status for the DMC.
        """
        logger.info("acknowledge dmc")
        return self.commands[Command.INTERLOCK_ACK.value]()

    def reset_dmc(self, axis: int = None) -> CmdReturn:
        """
        Reset the DMC (Device Motion Controller) for a specific axis.

        :param axis: The axis for which the DMC should be reset. Valid values are 0 for
            AZ (Azimuth), 1 for EL (Elevation), 2 for FI (Focus), and 3 for both AZ and
            EL. Defaults to None.
        :type axis: int
        :return: The result of resetting the DMC for the specified axis.
        :raises ValueError: If the axis parameter is not provided.
        """
        logger.info("reset dmc")
        if axis is None:
            logger.error(
                "reset_dmc requires an axis as parameter! Try one "
                "of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
            raise ValueError
        return self.commands[Command.RESET.value](ua.UInt16(axis))

    def activate_dmc(self, axis: int = None) -> CmdReturn:
        """
        Activate the DMC (Digital Motion Controller) for a specific axis.

        :param axis: The axis for which to activate the DMC (0=AZ, 1=EL, 2=FI, 3=AZ&EL).
        :type axis: int
        :return: The result of activating the DMC for the specified axis.
        :raises ValueError: If the axis parameter is None.
        """
        logger.info("activate dmc")
        if axis is None:
            logger.error(
                "activate_dmc requires an axis as parameter! "
                "Try one of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
            raise ValueError
        return self.commands[Command.ACTIVATE.value](ua.UInt16(axis))

    def deactivate_dmc(self, axis: int = None) -> CmdReturn:
        """
        Deactivate a specific axis of the DMC controller.

        :param axis: The axis to deactivate (0=AZ, 1=EL, 2=FI, 3=AZ&EL).
        :type axis: int, optional
        :raises ValueError: If the axis parameter is None.
        """
        logger.info("deactivate dmc")
        if axis is None:
            logger.error(
                "deactivate_dmc requires an axis as parameter! "
                "Try one of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
            raise ValueError
        return self.commands[Command.DEACTIVATE.value](ua.UInt16(axis))

    def move_to_band(self, position: str | int) -> CmdReturn:
        """
        Move the system to a specified band.

        :param position: The band to move to, either as a string representation or a
            numerical value.
        :type position: str or int
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
        :type az_angle: float
        :param el_angle: The absolute elevation angle in degrees.
        :type el_angle: float
        :param az_velocity: The azimuth velocity in degrees per second.
        :type az_velocity: float
        :param el_velocity: The elevation velocity in degrees per second.
        :type el_velocity: float
        """
        logger.info(
            f"abs az: {az_angle:.4f} el: {el_angle:.4f}, "
            f"az velocity: {az_velocity:.4f}, el velocity: {el_velocity:.4f}"
        )
        return self.commands[Command.SLEW2ABS_AZ_EL.value](
            az_angle, el_angle, az_velocity, el_velocity
        )

    # commands to ACU
    def activate_az(self) -> CmdReturn:
        """
        Activate the azimuth functionality.

        This method activates azimuth by calling the activate_dmc method with a
        parameter of 0.

        :return: The result of activating the azimuth functionality.
        """
        logger.info("activate azimuth")
        return self.activate_dmc(0)

    def deactivate_az(self) -> CmdReturn:
        """
        Deactivate azimuth functionality.

        :return: The result of deactivating azimuth.
        :raises Exception: May raise exceptions if there are errors during the
            deactivation process.
        """
        logger.info("deactivate azimuth")
        return self.deactivate_dmc(0)

    def activate_el(self) -> CmdReturn:
        """
        Activate the elevation.

        :return: The result of activating elevation.
        """
        logger.info("activate elevation")
        return self.activate_dmc(1)

    def deactivate_el(self) -> CmdReturn:
        """
        Deactivate elevation.

        This method deactivates elevation.

        :return: The result of deactivating elevation.
        """
        logger.info("deactivate elevation")
        return self.deactivate_dmc(1)

    def activate_fi(self) -> CmdReturn:
        """
        Activate the feed indexer.

        :return: The result of activating the feed indexer.
        """
        logger.info("activate feed indexer")
        return self.activate_dmc(2)

    def deactivate_fi(self) -> CmdReturn:
        """
        Deactivate the feed indexer.

        :return: The result of deactivating the feed indexer.
        """
        logger.info("deactivate feed indexer")
        return self.deactivate_dmc(2)

    def abs_azimuth(self, az_angle: float, az_vel: float) -> CmdReturn:
        """
        Calculate the absolute azimuth based on azimuth angle and azimuth velocity.

        :param az_angle: The azimuth angle value.
        :type az_angle: float
        :param az_vel: The azimuth velocity value.
        :type az_vel: float
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs az: {az_angle:.4f} vel: {az_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            ua.UInt16(0), az_angle, az_vel
        )

    def abs_elevation(self, el_angle: float, el_vel: float) -> CmdReturn:
        """
        Calculate the absolute elevation angle and velocity.

        :param el_angle: The absolute elevation angle.
        :type el_angle: float
        :param el_vel: The elevation velocity.
        :type el_vel: float
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs el: {el_angle:.4f} vel: {el_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            ua.UInt16(1), el_angle, el_vel
        )

    def abs_feed_indexer(self, fi_angle: float, fi_vel: float) -> CmdReturn:
        """
        Calculate the absolute feed indexer value.

        :param fi_angle: The angle value for the feed indexer.
        :type fi_angle: float
        :param fi_vel: The velocity value for the feed indexer.
        :type fi_vel: float
        :return: The result of the Slew2AbsSingleAx command.
        """
        logger.info(f"abs fi: {fi_angle:.4f} vel: {fi_vel:.4f}")
        return self.commands[Command.SLEW2ABS_SINGLE_AX.value](
            ua.UInt16(2), fi_angle, fi_vel
        )

    def load_static_offset(self, az_offset: float, el_offset: float) -> CmdReturn:
        """
        Load static azimuth and elevation offsets for tracking.

        :param az_offset: The azimuth offset value.
        :type az_offset: float
        :param el_offset: The elevation offset value.
        :type el_offset: float
        :return: The result of loading the static offsets.
        """
        logger.info(f"offset az: {az_offset:.4f} el: {el_offset:.4f}")
        return self.commands[Command.TRACK_LOAD_STATIC_OFF.value](az_offset, el_offset)

    def load_program_track(
        self,
        load_type: str,
        entries: int,
        t: numpy.ndarray,
        az: numpy.ndarray,
        el: numpy.ndarray,
    ) -> CmdReturn:
        """
        Load a program track with provided entries, time offsets, azimuth and elevation.

        :param load_type: Type of track data being loaded.
        :type load_type: str
        :param entries: Number of entries in the track table.
        :type entries: int
        :param t: List of time offset values.
        :type t: numpy.ndarray
        :param az: List of azimuth values.
        :type az: numpy.ndarray
        :param el: List of elevation values.
        :type el: numpy.ndarray
        :raises IndexError: If the provided track table contents are not aligned.
        """
        logger.info("%s", load_type)

        # unused
        # LOAD_TYPES = {
        #     'LOAD_NEW' : 0,
        #     'LOAD_ADD' : 1,
        #     'LOAD_RESET' : 2}

        # unused
        # table selector - to tidy for future use
        # ptrackA = 11
        # TABLE_SELECTOR =  {
        #     'pTrackA' : 11,
        #     'pTrackB' : 12,
        #     'oTrackA' : 21,
        #     'oTrackB' : 22}

        # unused
        # funny thing is SCU wants 50 entries, even for LOAD RESET! or if you send less
        # then you have to pad the table
        # if entries != 50:
        #     padding = 50 - entries
        #     t  += [0] * padding
        #     az += [0] * padding
        #     el += [0] * padding

        # pylint: disable=too-many-boolean-expressions
        if (entries > 0) and (
            (entries != len(t))
            or (entries != len(az))
            or (entries != len(el))
            or (len(t) != len(az))
            or (len(az) != len(el))
            or (len(az) != len(el))
        ):
            e = IndexError()
            msg = (
                "The provided track table contents are not usable because the contents "
                "are not aligned. The given number of track table entries and the size "
                "of each of the three arrays (t, az and el) need to match: Specified "
                "number of entries = %d, number of elements in time offset array = %d, "
                "number of elements in azimuth array = %d, number of elements in "
                "elevation array = %d."
            )
            logger.error(msg, entries, len(t), len(az), len(el))
            raise e

        # Format the track table in the new way that preceeds every row with
        # its row number.
        table = ""
        for index in range(len(t)):  # pylint: disable=consider-using-enumerate
            row = f"{index:03d}:{t[index]},{az[index]},{el[index]};"
            table += row
        logger.debug(f"Track table that will be sent to DS: {table}")
        byte_string = table.encode()
        try:
            return self.commands[Command.TRACK_LOAD_TABLE.value](
                load_type,
                ua.UInt16(entries),
                ua.ByteString(byte_string),
            )
        except Exception:
            logger.warning(
                "Tried to upload a track table in the new format but this failed. "
                "Will now try to uplad the track table in the old format...",
                exc_info=True,
            )

        # If I get here, then the OPC UA server likely did not support the new
        # format. Try again with the old format.
        table = ""
        table = f"{entries:03d}:"
        for index in range(0, entries):
            row = f"{t[index]},{az[index]},{el[index]};"
            table += row
        logger.debug(f"Track table that will be sent to DS: {table}")
        byte_string = table.encode()
        try:
            return self.commands[Command.TRACK_LOAD_TABLE.value](
                load_type,
                ua.UInt16(entries),
                ua.ByteString(byte_string),
            )
        except Exception as e:
            logger.exception(
                "Uploading of a track table in the old format failed too. "
                "Please check that your track table is correctly formatted, "
                "not empty and contains valid entries."
            )
            raise e

    def start_program_track(self, interpol_mode: int) -> CmdReturn:
        """
        Start tracking a program.

        :return: The result of starting the program track.
        """
        # TODO
        return self.commands[Command.TRACK_START.value](ua.UInt16(interpol_mode))

    def acu_ska_track(self) -> CmdReturn:
        """ACU SKA track."""
        # TODO
        logger.info("acu ska track")
        return self.commands[Command.TRACK_LOAD_TABLE.value](ua.UInt16(0))

    def _format_tt_line(
        self,
        t: float,
        az: float,
        el: float,
        capture_flag: int = 1,
        parallactic_angle: float = 0.0,
    ) -> str:
        """
        Something will provide a time, az, and el as minimum.

        Time must already be absolute time desired in MJD format. Assumption is capture
        flag and parallactic angle will not be used.
        """
        f_str = (
            f"{t:.12f} {az:.6f} {el:.6f} {capture_flag:.0f} "
            "{parallactic_angle:.6f}\n"
        )
        return f_str

    def _format_body(self, t: list[float], az: list[float], el: list[float]) -> str:
        """
        Format the body of a message with timestamp, azimuth, and elevation values.

        :param t: List of timestamps.
        :type t: list
        :param az: List of azimuth values.
        :type az: list
        :param el: List of elevation values.
        :type el: list
        :return: Formatted body of the message.
        :rtype: str
        """
        body = ""
        for i in range(len(t)):  # pylint: disable=consider-using-enumerate
            body += self._format_tt_line(t[i], az[i], el[i])
        return body
