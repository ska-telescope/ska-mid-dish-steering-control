"""System Control Unit library for an OPC UA server."""

# Feed Indexer tests [316-000000-043]
# Author: P.P.A. Kotze
# Date: 1/9/2020
# Version:
# 0.1 Initial
# 0.2 Update after feedback and correction from HN email dated 1/8/2020
# 0.3 Rework scu_get and scu_put to simplify
# 0.4 attempt more generic scu_put with either jason payload or simple params, remove
#   payload from feedback function
# 0.5 create scu_lib
# 0.6 1/10/2020 added load track tables and start table tracking also as debug added
#   'field' command for old scu
# HN: 13/05/2021 Changed the way file name is defined by defining a start time
# 07/10/2021 Added new "save_session14" where file time is no longer added to the file
#   name in this library, but expected to be passed as part of "filename" string
#   argument from calling script.
# 2023-08-31, Thomas Juerges Refactored the basic access mechanics for an OPC UA server.

import asyncio
import datetime
import enum
import logging
import logging.config
import os
import queue
import threading
import time
from importlib import resources
from pathlib import Path
from typing import Any, Union

import asyncua
import numpy
import yaml

logger = logging.getLogger("sculib")


def configure_logging(default_log_level: int = logging.INFO) -> None:
    """
    Configure logging settings based on a YAML configuration file.

    :param default_log_level: The default logging level to use if no configuration file
        is found. Defaults to logging.INFO.
    :type default_log_level: int
    :raises ValueError: If an error occurs while configuring logging from the file.
    """
    if os.path.exists("disq_logging_config.yaml"):
        disq_log_config_file: resources.Traversable | str = "disq_logging_config.yaml"
    else:
        disq_log_config_file = (
            resources.files(__package__) / "default_logging_config.yaml"
        )
    config = None
    if os.path.exists(disq_log_config_file):
        with open(disq_log_config_file, "rt") as f:
            try:
                config = yaml.safe_load(f.read())
                at_time = datetime.time.fromisoformat(
                    config["handlers"]["file_handler"]["atTime"]
                )
                config["handlers"]["file_handler"]["atTime"] = at_time
            except Exception as e:
                print(e)
                print(
                    "WARNING: Unable to read logging configuration file "
                    f"{disq_log_config_file}"
                )
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
            print(e)
            print(
                "WARNING: Caught exception. Unable to configure logging from file "
                f"{disq_log_config_file}. Reverting to logging to the console "
                "(basicConfig)."
            )
            logging.basicConfig(level=default_log_level)


# define some preselected sensors for recording into a logfile
hn_feed_indexer_sensors = [
    "acu.time.act_time_source",
    "acu.time.internal_time",
    "acu.time.external_ptp",
    "acu.general_management_and_controller.state",
    "acu.general_management_and_controller.feed_indexer_pos",
    "acu.azimuth.state",
    "acu.azimuth.p_set",
    "acu.azimuth.p_act",
    "acu.azimuth.v_act",
    "acu.elevation.state",
    "acu.elevation.p_set",
    "acu.elevation.p_act",
    "acu.elevation.v_act",
    "acu.feed_indexer.state",
    "acu.feed_indexer.p_set",
    "acu.feed_indexer.p_shape",
    "acu.feed_indexer.p_act",
    "acu.feed_indexer.v_shape",
    "acu.feed_indexer.v_act",
    "acu.feed_indexer.motor_1.actual_velocity",
    "acu.feed_indexer.motor_2.actual_velocity",
    "acu.feed_indexer.motor_1.actual_torque",
    "acu.feed_indexer.motor_2.actual_torque",
    "acu.general_management_and_controller.act_power_consum",
    "acu.general_management_and_controller.power_factor",
    "acu.general_management_and_controller.voltage_phase_1",
    "acu.general_management_and_controller.voltage_phase_2",
    "acu.general_management_and_controller.voltage_phase_3",
    "acu.general_management_and_controller.current_phase_1",
    "acu.general_management_and_controller.current_phase_2",
    "acu.general_management_and_controller.current_phase_3",
]

# OPC UA equivalent for the above listed hh_feed_indexer_sensors
# NOTE: These attributes/sensros are currently missing:
# *** general_management_and_controller.state
# *** time.act_time_source
# *** time.internal_time
# *** time.external_ptp
# Helper code that I used:
# lower = {}
# for i in scu.attributes.keys():
#     lower = i.tolower()
#     lower[lower] = i
# s = "hn_opcua_feed_indexer_sensors = ["
# for i in sculib.feed_indexer_sensors:
#     name = i.replace('acu.', '')
#     name = name.replace('feed_indexer', 'feedindexer')
#     a = None
#     try:
#         u = lower[name]
#         s += f"\n'{u}',"
#     except KeyError as e:
#         print(f'*** {name}')
# s += f"\n]"
# print(f'{s}')
# s = "hn_opcua_tilt_sensors = ["
# for i in sculib.hn_tilt_sensors:
#     name = i.replace('acu.', '')
#     name = name.replace('feed_indexer', 'feedindexer')
#     a = None
#     try:
#         u = lower[name]
#         s += f"\n'{u}',"
#     except KeyError as e:
#         print(f'*** {name}')
# s += f"\n]"
# print(f'{s}')
hn_opcua_feed_indexer_sensors = [
    "Azimuth.AxisState",
    "Azimuth.p_Set",
    "Azimuth.p_Act",
    "Azimuth.v_Act",
    "Elevation.AxisState",
    "Elevation.p_Set",
    "Elevation.p_Act",
    "Elevation.v_Act",
    "FeedIndexer.AxisState",
    "FeedIndexer.MotorOne.mActTorq",
    "FeedIndexer.MotorOne.mActVelocity",
    "FeedIndexer.MotorTwo.mActTorq",
    "FeedIndexer.MotorTwo.mActVelocity",
    "FeedIndexer.p_Set",
    "FeedIndexer.p_Shape",
    "FeedIndexer.p_Act",
    "FeedIndexer.v_Shape",
    "FeedIndexer.v_Act",
    "Management.ManagementStatus.FiPos",
    "Management.ManagementStatus.PowerStatus.ActPwrCnsm",
    "Management.ManagementStatus.PowerStatus.CurrentPh1",
    "Management.ManagementStatus.PowerStatus.CurrentPh2",
    "Management.ManagementStatus.PowerStatus.CurrentPh3",
    "Management.ManagementStatus.PowerStatus.PowerFactor",
    "Management.ManagementStatus.PowerStatus.VoltagePh1",
    "Management.ManagementStatus.PowerStatus.VoltagePh2",
    "Management.ManagementStatus.PowerStatus.VoltagePh3",
    "Time.DscTime",
]

# hn_tilt_sensors is equivalent to "Servo performance"
hn_tilt_sensors = [
    "acu.time.act_time_source",
    "acu.time.internal_time",
    "acu.time.external_ptp",
    "acu.general_management_and_controller.state",
    "acu.general_management_and_controller.feed_indexer_pos",
    "acu.azimuth.state",
    "acu.azimuth.p_set",
    "acu.azimuth.p_act",
    "acu.azimuth.v_act",
    "acu.elevation.state",
    "acu.elevation.p_set",
    "acu.elevation.p_act",
    "acu.elevation.v_act",
    "acu.general_management_and_controller.act_power_consum",
    "acu.general_management_and_controller.power_factor",
    "acu.general_management_and_controller.voltage_phase_1",
    "acu.general_management_and_controller.voltage_phase_2",
    "acu.general_management_and_controller.voltage_phase_3",
    "acu.general_management_and_controller.current_phase_1",
    "acu.general_management_and_controller.current_phase_2",
    "acu.general_management_and_controller.current_phase_3",
    "acu.pointing.act_amb_temp_1",
    "acu.pointing.act_amb_temp_2",
    "acu.pointing.act_amb_temp_3",
    "acu.general_management_and_controller.temp_air_inlet_psc",
    "acu.general_management_and_controller.temp_air_outlet_psc",
    "acu.pointing.incl_signal_x_raw",
    "acu.pointing.incl_signal_x_deg",
    "acu.pointing.incl_signal_x_filtered",
    "acu.pointing.incl_signal_x_corrected",
    "acu.pointing.incl_signal_y_raw",
    "acu.pointing.incl_signal_y_deg",
    "acu.pointing.incl_signal_y_filtered",
    "acu.pointing.incl_signal_y_corrected",
    "acu.pointing.incl_temp",
    "acu.pointing.incl_corr_val_az",
    "acu.pointing.incl_corr_val_el",
]

hn_opcua_tilt_sensors = [
    "Azimuth.AxisState",
    "Azimuth.p_Set",
    "Azimuth.p_Act",
    "Azimuth.v_Act",
    "Elevation.AxisState",
    "Elevation.p_Set",
    "Elevation.p_Act",
    "Elevation.v_Act",
    "Management.ManagementStatus.FiPos",
    "Management.ManagementStatus.PowerStatus.ActPwrCnsm",
    "Management.ManagementStatus.PowerStatus.CurrentPh1",
    "Management.ManagementStatus.PowerStatus.CurrentPh2",
    "Management.ManagementStatus.PowerStatus.CurrentPh3",
    "Management.ManagementStatus.PowerStatus.PowerFactor",
    "Management.ManagementStatus.PowerStatus.VoltagePh1",
    "Management.ManagementStatus.PowerStatus.VoltagePh2",
    "Management.ManagementStatus.PowerStatus.VoltagePh3",
    "Management.ManagementStatus.TempHumidStatus.TempPSC_Inlet",
    "Management.ManagementStatus.TempHumidStatus.TempPSC_Outlet",
    "Pointing.ActAmbTemp_East",
    "Pointing.ActAmbTemp_South",
    "Pointing.ActAmbTemp_West",
    "Pointing.TiltCorrVal_Az",
    "Pointing.TiltCorrVal_El",
    "Pointing.TiltTemp_One",
    "Pointing.TiltXArcsec_One",
    "Pointing.TiltXArcsec_Two",
    "Pointing.TiltXFilt_One",
    "Pointing.TiltXFilt_Two",
    "Pointing.TiltXRaw_One",
    "Pointing.TiltXRaw_Two",
    "Pointing.TiltXTemp_Two",
    "Pointing.TiltYArcsec_One",
    "Pointing.TiltYArcsec_Two",
    "Pointing.TiltYFilt_One",
    "Pointing.TiltYFilt_Two",
    "Pointing.TiltYRaw_One",
    "Pointing.TiltYRaw_Two",
    "Time.DscTime",
]


async def handle_exception(e: Exception) -> None:
    """
    Handle and log an exception.

    :param e: The exception that was caught.
    :type e: Exception
    """
    logger.exception("*** Exception caught: %s", e)


def create_command_function(node: asyncua.Node, event_loop: asyncio.AbstractEventLoop):
    """
    Create a command function to execute a method on a specified Node.

    :param node: The Node on which the method will be executed.
    :type node: asyncua.Node
    :param event_loop: The asyncio event loop to run the coroutine on.
    :type event_loop: asyncio.AbstractEventLoop
    :return: A function that can be used to execute a method on the Node.
    :rtype: function
    """
    call = (
        asyncio.run_coroutine_threadsafe(node.get_parent(), event_loop)
        .result()
        .call_method
    )
    read_name = asyncio.run_coroutine_threadsafe(node.read_display_name(), event_loop)
    uid = f"{node.nodeid.NamespaceIndex}:{read_name.result().Text}"

    def fn(*args) -> Any:
        """
        Execute function with arguments and return tuple with return code and message.

        :param args: Optional positional arguments to pass to the function.
        :type args: Tuple

        :return: A tuple containing the return code (int) and return message (str).
        :rtype: Tuple[int, str]

        :raises: Any exception raised during the execution of the function will be
            handled by the function and a tuple with return code -1 and exception
            message will be returned.

        Note: This function uses asyncio to run the coroutine in a separate thread.
        """
        try:
            return_code = asyncio.run_coroutine_threadsafe(
                call(uid, *args), event_loop
            ).result()
            return_msg: str = ""
            if hasattr(asyncua.ua, "CmdResponseType") and return_code is not None:
                # The asyncua library has a CmdResponseType enum ONLY if the opcua
                # server implements the type
                return_msg = asyncua.ua.CmdResponseType(return_code).name
            else:
                return_msg = str(return_code)
        except Exception as e:
            # e.add_note(f'Command: {uid} args: {args}')
            asyncio.run_coroutine_threadsafe(handle_exception(e), event_loop)
            return_code = -1
            return_msg = f"Command: {uid}, args: {args}, exception: {e}"
        return return_code, return_msg

    return fn


def create_rw_attribute(node: asyncua.Node, event_loop: asyncio.AbstractEventLoop):
    """
    Create a read-write attribute for an OPC UA node.

    :param node: The OPC UA node to create the attribute for.
    :type node: asyncua.Node

    :param event_loop: The asyncio event loop to use for running coroutines.
    :type event_loop: asyncio.AbstractEventLoop

    :return: An instance of a read-write attribute for the given OPC UA node.
    :rtype: opc_ua_rw_attribute

    The opc_ua_rw_attribute class has the following methods:

    - value: A property that gets the value of the OPC UA node.
    - value.setter: A setter method that sets the value of the OPC UA node.

    :raises Exception: If an exception occurs during getting or setting the value.
    """

    class opc_ua_rw_attribute:  # noqa: N801
        @property
        def value(self) -> Any:
            try:
                return asyncio.run_coroutine_threadsafe(
                    node.get_value(), event_loop
                ).result()
            except Exception as e:
                asyncio.run_coroutine_threadsafe(handle_exception(e), event_loop)

        @value.setter
        def value(self, _value: Any) -> None:
            try:
                asyncio.run_coroutine_threadsafe(
                    node.set_value(_value), event_loop
                ).result()
            except Exception as e:
                asyncio.run_coroutine_threadsafe(handle_exception(e), event_loop)

    return opc_ua_rw_attribute()


def create_ro_attribute(node: asyncua.Node, event_loop: asyncio.AbstractEventLoop):
    """
    Create a read-only attribute for an OPC UA Node.

    :param node: The OPC UA Node from which to read the attribute.
    :type node: asyncua.Node
    :param event_loop: The asyncio event loop to use.
    :type event_loop: asyncio.AbstractEventLoop
    :return: An object with a read-only 'value' property.
    :rtype: opc_ua_ro_attribute
    :raises Exception: If an error occurs while getting the value from the OPC UA Node.
    """

    class opc_ua_ro_attribute:  # noqa: N801
        @property
        def value(self) -> Any:
            try:
                return asyncio.run_coroutine_threadsafe(
                    node.get_value(), event_loop
                ).result()
            except Exception as e:
                asyncio.run_coroutine_threadsafe(handle_exception(e), event_loop)

    return opc_ua_ro_attribute()


class SubscriptionHandler:
    """
    A class representing a Subscription Handler.

    :param subscription_queue: The queue to store subscription notifications.
    :type subscription_queue: queue.Queue
    :param nodes: A dictionary mapping nodes to their names.
    :type nodes: dict
    """

    def __init__(self, subscription_queue: queue.Queue, nodes: dict) -> None:
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
        self, node: asyncua.Node, value: Any, data: asyncua.ua.DataChangeNotification
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


class SCU:
    """
    System Control Unit.

    Small library that eases the pain when connecting to an OPC UA server and calling
    methods on it, reading or writing attributes.

    How to:
    - Import the library:
    ```
    import sculib
    ```
    - Instantiate an SCU object. I provide here the defaults which can be overwritten by
      specifying the named parameter:
    ```
    scu = sculib.SCU(host = 'localhost', port = 4840, endpoint = '',
      namespace = 'http://skao.int/DS_ICD/', timeout = 10.0)
    ```
    - Done.
    - All nodes from and including the PLC_PRG node are stored in the nodes dictionary:
    ```
    scu.get_node_list()
    ```
    - Every value in the dictionary exposes the full OPC UA functionality for a node.
      Note: When accessing nodes directly, it is mandatory to await any calls:
    ```
    node_name = (await (node.read_display_name()).Text
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
    - For instance, command the PLC to slew to a new position:
    ```
    az = 182.0; el = 21.8; az_v = 1.2; el_v = 2.1
    result_code,result_msg = scu.commands['Management.Slew2AbsAzEl'](az, el, az_v, el_v)
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
    ```
    ```
    *** Exception caught
    "User does not have permission to perform the requested operation."
    (BadUserAccessDenied)
    ```
    """  # noqa: RST201,RST203,RST214,RST301

    def __init__(
        self,
        host: str = "localhost",
        port: int = 4840,
        endpoint: str = "/OPCUA/SimpleServer",
        namespace: str = "CETC54",
        username: str | None = None,
        password: str | None = None,
        timeout: float = 10.0,
        eventloop: asyncio.AbstractEventLoop | None = None,
        debug: bool = False,
    ) -> None:
        """
        Initializes the sculib with the provided parameters.

        :param host: The hostname or IP address of the server. Default is 'localhost'.
        :type host: str
        :param port: The port number of the server. Default is 4840.
        :type port: int
        :param endpoint: The endpoint on the server. Default is '/OPCUA/SimpleServer'.
        :type endpoint: str
        :param namespace: The namespace for the server. Default is 'CETC54'.
        :type namespace: str
        :param username: The username for authentication. Default is None.
        :type username: str | None
        :param password: The password for authentication. Default is None.
        :type password: str | None
        :param timeout: The timeout value for connection. Default is 10.0.
        :type timeout: float
        :param eventloop: The asyncio event loop to use. Default is None.
        :type eventloop: asyncio.AbstractEventLoop | None
        :param debug: Whether debug mode is enabled. Default is False.
        :type debug: bool
        :raises Exception: If connection to OPC UA server fails.
        """
        logger.info("Initialising sculib")
        self.init_called = False
        self.host = host
        self.port = port
        self.endpoint = endpoint
        self.namespace = namespace
        self.timeout = timeout
        self.event_loop_thread = None
        self.subscription_handler = None
        self.subscriptions = {}
        self.subscription_queue = queue.Queue()
        if eventloop is None:
            self.create_and_start_asyncio_event_loop()
        else:
            self.event_loop = eventloop
        logger.info(f"Event loop: {self.event_loop}")
        try:
            self.connection = self.connect(
                self.host, self.port, self.endpoint, self.timeout, encryption=False
            )
        except Exception:
            try:
                # TODO: why these user/pw?
                # These appear to NOT be default ones for the CETC54 simulator...
                user = "lmc"
                pw = "lmclmclmc"
                if username is not None:
                    user = username
                if password is not None:
                    pw = password
                self.connection = self.connect(
                    self.host,
                    self.port,
                    self.endpoint,
                    self.timeout,
                    encryption=True,
                    user=user,
                    pw=pw,
                )
            except Exception as e:
                # e.add_note('Cannot connect to the OPC UA server. Please '
                msg = (
                    "Cannot connect to the OPC UA server. Please "
                    "check the connection parameters that were "
                    "passed to instantiate the sculib!"
                )
                logger.error("%s - %s", msg, e)
                raise e
        logger.info("Populating nodes dicts from server. This will take about 1s...")
        self.populate_node_dicts()
        self.debug = debug
        self.init_called = True
        logger.info("Initialising sculib done.")

    def __del__(self) -> None:
        """
        Clean up resources and disconnect from all subscriptions.

        This method unsubscribes from all subscriptions, disconnects from the server,
        and stops the event loop if it was started in a separate thread.
        """
        self.unsubscribe_all()
        self.disconnect()
        if self.event_loop_thread is not None:
            # Signal the event loop thread to stop.
            self.event_loop.call_soon_threadsafe(self.event_loop.stop)
            # Join the event loop thread once it is done processing tasks.
            self.event_loop_thread.join()

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

    def create_and_start_asyncio_event_loop(self) -> None:
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

    def set_up_encryption(self, connection, user: str, pw: str) -> None:
        # this is generated if it does not exist
        """
        Set up encryption for the connection with the given user credentials.

        :param connection: The connection object to set up encryption for.
        :type connection: object
        :param user: The username for the connection.
        :type user: str
        :param pw: The password for the connection.
        :type pw: str
        """
        opcua_client_key = Path(resources.files(__package__) / "certs/key.pem")
        # this is generated if it does not exist
        opcua_client_cert = Path(resources.files(__package__) / "certs/cert.der")
        # get from simulator/PKI/private/SimpleServer_2048.der in tarball
        opcua_server_cert = Path(
            resources.files(__package__) / "certs/SimpleServer_2048.der"
        )
        connection.set_user(user)
        connection.set_password(pw)
        from asyncua.crypto.cert_gen import setup_self_signed_certificate
        from asyncua.crypto.security_policies import SecurityPolicyBasic256
        from asyncua.ua import MessageSecurityMode
        from cryptography.x509.oid import ExtendedKeyUsageOID

        client_app_uri = "urn:freeopcua:client"
        _ = asyncio.run_coroutine_threadsafe(
            setup_self_signed_certificate(
                key_file=opcua_client_key,
                cert_file=opcua_client_cert,
                app_uri=client_app_uri,
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
            connection.set_security(
                SecurityPolicyBasic256,
                certificate=str(opcua_client_cert),
                private_key=str(opcua_client_key),
                server_certificate=str(opcua_server_cert),
                mode=MessageSecurityMode.Sign,
            ),
            self.event_loop,
        ).result()

    def connect(
        self,
        host: str,
        port: int,
        endpoint: str,
        timeout: float,
        encryption: bool = True,
        user: str = None,
        pw: str = None,
    ) -> None:
        """
        Connect to an OPC UA server.

        :param host: The host IP address or hostname of the OPC UA server.
        :type host: str
        :param port: The port number of the OPC UA server.
        :type port: int
        :param endpoint: The endpoint path of the OPC UA server.
        :type endpoint: str
        :param timeout: The timeout for the connection in seconds.
        :type timeout: float
        :param encryption: Flag indicating whether encryption is enabled (default is
            True).
        :type encryption: bool
        :param user: Optional username for authentication.
        :type user: str
        :param pw: Optional password for authentication.
        :type pw: str
        :raises: Exception if an error occurs during the connection process.
        """
        opc_ua_server = f"opc.tcp://{host}:{port}{endpoint}"
        logger.info(f"Connecting to: {opc_ua_server}")
        connection = asyncua.Client(opc_ua_server, timeout)
        if encryption:
            self.set_up_encryption(connection, user, pw)
        _ = asyncio.run_coroutine_threadsafe(
            connection.connect(), self.event_loop
        ).result()
        self.opc_ua_server = opc_ua_server
        try:
            _ = asyncio.run_coroutine_threadsafe(
                connection.load_data_type_definitions(), self.event_loop
            ).result()
        except Exception:
            # The CETC simulator V1 returns a faulty DscCmdAuthorityEnumType,
            # where the entry for 3 has no name.
            pass
        # Get the namespace index for the PLC's Parameter node
        try:
            self.parameter_ns_idx = asyncio.run_coroutine_threadsafe(
                connection.get_namespace_index(
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
            if self.namespace != "" and endpoint != "":
                self.ns_idx = asyncio.run_coroutine_threadsafe(
                    connection.get_namespace_index(self.namespace), self.event_loop
                ).result()
            else:
                # Force namespace index for first physical controller
                self.ns_idx = 2
        except ValueError as e:
            namespaces = None
            try:
                namespaces = asyncio.run_coroutine_threadsafe(
                    connection.get_namespace_array(), self.event_loop
                ).result()
            except Exception:
                pass
            try:
                self.disconnect(connection)
            except Exception:
                pass
            message = (
                "*** Exception caught while trying to access the requested "
                f"namespace '{self.namespace}' on the OPC UA server. Will NOT continue "
                "with the normal operation but list the available namespaces here for "
                f"future reference:\n{namespaces}"
            )
            logger.error(message)
            # e.add_note(message)
            raise e
        return connection

    def disconnect(self, connection=None) -> None:
        """
        Disconnect from a connection.

        :param connection: The connection to disconnect from. If None, disconnect from
            self.connection.
        :type connection: object
        """
        if connection is None:
            connection = self.connection
            self.connection = None
        if connection is not None:
            _ = asyncio.run_coroutine_threadsafe(
                connection.disconnect(), self.event_loop
            ).result()

    def connection_reset(self) -> None:
        """Reset the connection by disconnecting and reconnecting."""
        self.disconnect()
        self.connect()

    def populate_node_dicts(self) -> None:
        # Create three dicts:
        # nodes, attributes, commands
        # nodes: Contains the entire uasync.Node-tree from and including
        #   the 'PLC_PRG' node.
        # attributes: Contains the attributes in the uasync.Node-tree from
        #   the 'PLC_PRG' node on. The values are callables that return the
        #   current value.
        # {Key = Name as in the node hierarchy, value = the uasync.Node}
        # commands: Contains all callable methods in the uasync.Node-tree
        #   from the 'PLC_PRG' node on. The values are callables which
        #   just require the expected parameters.
        """
        Populate dictionaries with Node objects.

        This method populates dictionaries with Node objects for different categories
        like Nodes, Attributes, and Commands.

        This method does not accept any parameters and does not return anything.

        This method may raise exceptions related to asyncio operations.
        """
        plc_prg = asyncio.run_coroutine_threadsafe(
            self.connection.nodes.objects.get_child(
                [
                    f"{self.ns_idx}:Logic",
                    f"{self.ns_idx}:Application",
                    f"{self.ns_idx}:PLC_PRG",
                ]
            ),
            self.event_loop,
        ).result()
        (
            self.nodes,
            self.nodes_reversed,
            self.attributes,
            self.attributes_reversed,
            self.commands,
            self.commands_reversed,
        ) = self.generate_node_dicts(plc_prg, "PLC_PRG")
        self.plc_prg = plc_prg
        # We also want the PLC's parameters for the drives and the PLC program.
        # But only if we are not connected to the simulator.
        if self.parameter_ns_idx is not None:
            parameter = asyncio.run_coroutine_threadsafe(
                self.connection.nodes.objects.get_child(
                    [f"{self.parameter_ns_idx}:Parameter"]
                ),
                self.event_loop,
            ).result()
            (
                self.parameter_nodes,
                self.parameter_nodes_reversed,
                self.parameter_attributes,
                self.parameter_attributes_reversed,
                self.parameter_commands,
                self.parameter_commands_reversed,
            ) = self.generate_node_dicts(parameter, "Parameter")
            self.parameter = parameter
        # And now create dicts for all nodes of the OPC UA server. This is
        # intended to serve as the API for the Dish LMC.
        server = self.connection.get_root_node()
        (
            self.server_nodes,
            self.server_nodes_reversed,
            self.server_attributes,
            self.server_attributes_reversed,
            self.server_commands,
            self.server_commands_reversed,
        ) = self.generate_node_dicts(server, "Root")
        self.server = server

    def generate_node_dicts(self, top_level_node, top_level_node_name: str = None):
        """
        Generate dicts for nodes, attributes, and commands for a given top-level node.

        This function is part of a class and takes the top-level node and an optional
        name for it as input. It then generates dictionaries for nodes, attributes, and
        commands based on the structure of the top-level node and returns a tuple
        containing these dictionaries.

        The dictionaries contain mappings of keys to values and values to keys for
        nodes, attributes, and commands.

        Example:
            nodes = {'node1': {'attribute1': 'value1'}, 'node2':
            {'attribute2': 'value2'}}
            nodes_reversed = {'{'attribute1': 'value1'}: 'node1',
            {'attribute2': 'value2'}: 'node2'}
            attributes = {'attribute1': 'value1', 'attribute2': 'value2'}
            attributes_reversed = {'value1': 'attribute1', 'value2': 'attribute2'}
            commands = {'command1': 'node1', 'command2': 'node2'}
            commands_reversed = {'node1': 'command1', 'node2': 'command2'}

        :param top_level_node: The top-level node for which to generate dictionaries.
        :type top_level_node: Any

        :param top_level_node_name: Optional name for the top-level node. Default None.
        :type top_level_node_name: str

        :return: A tuple containing dictionaries for nodes, nodes_reversed, attributes,
            attributes_reversed, commands, and commands_reversed.
        :rtype: tuple
        """
        nodes, attributes, commands = self.get_sub_nodes(top_level_node)
        nodes.update({top_level_node_name: top_level_node})
        nodes_reversed = {v: k for k, v in nodes.items()}
        attributes_reversed = {v: k for k, v in attributes.items()}
        commands_reversed = {v: k for k, v in commands.items()}
        return (
            nodes,
            nodes_reversed,
            attributes,
            attributes_reversed,
            commands,
            commands_reversed,
        )

    def generate_full_node_name(
        self,
        node: asyncua.Node,
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
        name = (
            asyncio.run_coroutine_threadsafe(node.read_browse_name(), self.event_loop)
            .result()
            .Name
        )
        ancestors = []
        if parent_names is not None:
            for p_name in parent_names:
                ancestors.append(p_name)

        if name != "PLC_PRG":
            ancestors.append(name)

        node_name = node_name_separator.join(ancestors)

        return (node_name, ancestors)

    def get_sub_nodes(
        self,
        node: asyncua.Node,
        node_name_separator: str = ".",
        parent_names: list[str] | None = None,
    ) -> (dict, dict, dict):
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
        nodes = {}
        attributes = {}
        commands = {}
        node_name, ancestors = self.generate_full_node_name(node, parent_names)
        # Do not add the InputArgument and OutputArgument nodes.
        if (
            node_name.endswith(".InputArguments", node_name.rfind(".")) is True
            or node_name.endswith(".OutputArguments", node_name.rfind(".")) is True
        ):
            return nodes, attributes, commands
        else:
            nodes[node_name] = node
        node_class = (
            asyncio.run_coroutine_threadsafe(node.read_node_class(), self.event_loop)
            .result()
            .value
        )
        # node_class = 1: Normal node with children
        # node_class = 2: Attribute
        # node_class = 4: Method
        if node_class == 1:
            children = asyncio.run_coroutine_threadsafe(
                node.get_children(), self.event_loop
            ).result()
            child_nodes = {}
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
            attributes[node_name] = create_rw_attribute(node, self.event_loop)
            # else:
            # attributes[node_name] = create_ro_attribute(node, self.event_loop)
        elif node_class == 4:
            # A command. Add it to the commands dict.
            commands[node_name] = create_command_function(node, self.event_loop)
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

    def __get_node_list(self, nodes) -> None:
        """
        Get a list of node keys from a dictionary of nodes.

        :param nodes: A dictionary of nodes where keys are node identifiers.
        :type nodes: dict
        :return: A list of node keys.
        :rtype: list
        """
        info = ""
        for key in nodes.keys():
            info += f"{key}\n"
        logger.debug(info)
        return list(nodes.keys())

    def get_attribute_data_type(
        self, attribute: str | asyncua.ua.uatypes.NodeId
    ) -> str:
        """
        Get the data type for the given node.

        Returns string for the type or "Unknown" for a not yet known type.
        """
        if isinstance(attribute, str):
            node = self.nodes[attribute]
            dt_id = asyncio.run_coroutine_threadsafe(
                node.read_data_type(), self.event_loop
            ).result()
        elif isinstance(attribute, asyncua.ua.uatypes.NodeId):
            dt_id = attribute

        dt_node = self.connection.get_node(dt_id)
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
        if dt_name in dir(asyncua.ua):
            if issubclass(getattr(asyncua.ua, dt_name), enum.Enum):
                return "Enumeration"

        return "Unknown"

    def _enum_fields_out_of_order(
        self,
        index: int,
        field: asyncua.ua.uaprotocol_auto.EnumField,
        enum_node: asyncua.ua.uatypes.NodeId,
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
            f"Incorrect index for field {field.Name} of enumeration {enum_name}. "
            f"Expected: {index}, actual: {field.Value}"
        )
        return (
            f"ERROR: incorrect index for {field.Name}; expected: {index} "
            f"actual: {field.Value}"
        )

    def get_enum_strings(self, enum_node: str | asyncua.ua.uatypes.NodeId) -> list[str]:
        """
        Get list of enum strings where the index of the list matches the enum value.

        enum_node MUST be the name of an Enumeration type node (see
        get_attribute_data_type()).
        """
        if isinstance(enum_node, str):
            node = self.nodes[enum_node]
            dt_id = asyncio.run_coroutine_threadsafe(
                node.read_data_type(), self.event_loop
            ).result()
        elif isinstance(enum_node, asyncua.ua.uatypes.NodeId):
            dt_id = enum_node

        dt_node = self.connection.get_node(dt_id)
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

    def subscribe(
        self,
        attributes: Union[str, list[str]] = hn_opcua_tilt_sensors,
        period: int = 100,
        data_queue: queue.Queue = None,
    ) -> int:
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
        :return: A unique identifier for the subscription.
        :rtype: int
        """
        if data_queue is None:
            data_queue = self.subscription_queue
        subscription_handler = SubscriptionHandler(data_queue, self.nodes_reversed)
        if not isinstance(attributes, list):
            attributes = [
                attributes,
            ]
        nodes = []
        invalid_attributes = []
        for attribute in attributes:
            if attribute in self.nodes:
                nodes.append(self.nodes[attribute])
            else:
                invalid_attributes.append(attribute)
        if len(invalid_attributes) > 0:
            logger.warning(
                "The following OPC-UA attributes not found in nodes dict and not "
                "subscribed for event updates: %s",
                invalid_attributes,
            )
        subscription = asyncio.run_coroutine_threadsafe(
            self.connection.create_subscription(period, subscription_handler),
            self.event_loop,
        ).result()
        handle = asyncio.run_coroutine_threadsafe(
            subscription.subscribe_data_change(nodes), self.event_loop
        ).result()
        uid = time.monotonic_ns()
        self.subscriptions[uid] = {
            "handle": handle,
            "nodes": nodes,
            "subscription": subscription,
        }
        return uid

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
            uid, subscription = self.subscriptions.popitem()
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

    def load_track_table_file(self, file_name: str) -> numpy.array:
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
                "Could not load or convert the track table file '%s': %s", file_name, e
            )
            raise e

    def track_table_reset_and_upload_from_file(self, file_name: str) -> None:
        """
        Direct upload a track table to the dish structure's OPC UA server.

        :param file_name: File name of the track table file including its path.
        :type file_name: str
        """
        positions = self.load_track_table_file(file_name)
        # Reset the currently loaded track table.
        self.load_program_track(asyncua.ua.LoadEnumType.Reset, 0, [0.0], [0.0], [0.0])
        # Submit the new track table.
        self.load_program_track(
            asyncua.ua.LoadEnumType.New,
            len(positions),
            positions[:, 0],
            positions[:, 1],
            positions[:, 2],
        )

    # Direct SCU webapi functions based on urllib PUT/GET
    def feedback(self, r):
        """
        This function logs feedback information and returns.

        This function logs the request URL, request body, reason, status code, text
        returned if debug mode is enabled or if the status code is not 200.

        :param r: The response object.
        :type r: Response object
        """
        logger.error("Not implemented because this function is not needed.")
        return
        # if self.debug == True:
        #     logger.info("***Feedback:", r.request.url, r.request.body)
        #     logger.info(r.reason, r.status_code)
        #     logger.info("***Text returned:")
        #     logger.info(r.text)
        # elif r.status_code != 200:
        #     logger.info("***Feedback:", r.request.url, r.request.body)
        #     logger.info(r.reason, r.status_code)
        #     logger.info("***Text returned:")
        #     logger.info(r.text)
        #     # logger.info(r.reason, r.status_code)
        #     # logger.info()

    # 	def scu_get(device, params = {}, r_ip = self.ip, r_port = port):
    def scu_get(self, device, params={}):
        """
        Perform a GET request to a specified device on the SCU.

        :param device: The device to send the GET request to.
        :type device: str
        :param params: Parameters to include in the GET request (default is an empty
            dictionary).
        :type params: dict
        :return: The response from the GET request.
        :rtype: requests.Response object
        """
        logger.error("Not implemented because this function is not needed.")
        return
        # """This is a generic GET command into http: scu port + folder
        # with params=payload"""
        # URL = "http://" + self.ip + ":" + self.port + device
        # r = requests.get(url=URL, params=params)
        # self.feedback(r)
        # return r

    def scu_put(self, device, payload={}, params={}, data=""):
        """
        Perform a PUT request to a specific device on the SCU.

        :param device: The device endpoint to send the PUT request.
        :type device: str
        :param payload: The JSON data to send in the PUT request.
        :type payload: dict
        :param params: The parameters to include in the PUT request.
        :type params: dict
        :param data: Additional data to include in the PUT request.
        :type data: str
        :return: Response object from the PUT request.
        :rtype: requests.Response
        """
        logger.error("Not implemented because this function is not needed.")
        return
        # """This is a generic PUT command into http: scu port + folder
        # with json=payload"""
        # URL = "http://" + self.ip + ":" + self.port + device
        # r = requests.put(url=URL, json=payload, params=params, data=data)
        # self.feedback(r)
        # return r

    def scu_delete(self, device, payload={}, params={}):
        """
        Send a DELETE request to a specific device.

        :param device: The device to send the DELETE request to.
        :type device: str
        :param payload: The payload data to send in the request.
        :type payload: dict
        :param params: The parameters to include in the request.
        :type params: dict
        :return: The response object from the DELETE request.
        :rtype: requests.models.Response
        """
        logger.error("Not implemented because this function is not needed.")
        return
        # """This is a generic DELETE command into http: scu port + folder
        # with params=payload"""
        # URL = "http://" + self.ip + ":" + self.port + device
        # r = requests.delete(url=URL, json=payload, params=params)
        # self.feedback(r)
        # return r

    # SIMPLE PUTS

    def command_authority(self, action: bool = None, username: str = ""):
        """
        Check and execute a command based on the specified action and username.

        :param action: A boolean value representing the action to be performed.
        :type action: bool
        :param username: The username of the user requesting the action.
        :type username: str
        :return: The result of the command execution.
        :rtype: result type
        :raises KeyError: If the action provided is not valid.
        """
        if action not in authority:  # noqa: F821 TODO real problem
            logger.error("command_authority requires the action to be Get or Release!")
            return
        if len(username) <= 0:
            logger.error(
                "command_authority command requires a user as second parameter!"
            )
            return
        # 1 get #2 release
        logger.info("command authority: ", action)
        authority = {"Get": True, "Release": False}
        return self.commands["CommandArbiter.TakeReleaseAuth"](
            authority[action], username
        )

    # commands to DMC state - dish management controller
    def interlock_acknowledge_dmc(self):
        """
        Acknowledge the interlock status for the DMC.

        :return: The result of acknowledging the interlock status for the DMC.
        :rtype: unknown
        """
        logger.info("acknowledge dmc")
        return self.commands["Safety.InterlockAck"]()

    def reset_dmc(self, axis: int = None):
        """
        Reset the DMC (Device Motion Controller) for a specific axis.

        :param axis: The axis for which the DMC should be reset. Valid values are 0 for
            AZ (Azimuth), 1 for EL (Elevation), 2 for FI (Focus), and 3 for both AZ and
            EL. Defaults to None.
        :type axis: int
        :return: The result of resetting the DMC for the specified axis.
        :rtype: str
        :raises ValueError: If the axis parameter is not provided.
        """
        logger.info("reset dmc")
        if axis is None:
            logger.error(
                "reset_dmc requires an axis as parameter! Try one "
                "of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
        return self.commands["Management.Reset"](axis)

    def activate_dmc(self, axis: int = None):
        """
        Activate the DMC (Digital Motion Controller) for a specific axis.

        :param axis: The axis for which to activate the DMC (0=AZ, 1=EL, 2=FI, 3=AZ&EL).
        :type axis: int
        :return: The result of activating the DMC for the specified axis.
        :rtype: unknown (depends on the implementation of
            self.commands['Management.Activate'])
        :raises ValueError: If the axis parameter is None.
        """
        logger.info("activate dmc")
        if axis is None:
            logger.error(
                "activate_dmc requires now an axis as parameter! "
                "Try one of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
            return
        return self.commands["Management.Activate"](axis)

    def deactivate_dmc(self, axis: int = None):
        """
        Deactivate a specific axis of the DMC controller.

        :param axis: The axis to deactivate (0=AZ, 1=EL, 2=FI, 3=AZ&EL).
        :type axis: int, optional
        """
        logger.info("deactivate dmc")
        if axis is None:
            logger.error(
                "deactivate_dmc requires now an axis as parameter! "
                "Try one of these values: 0=AZ, 1=EL, 2=FI, 3=AZ&EL"
            )
            return
        return self.commands["Management.DeActivate"](axis)

    def move_to_band(self, position):
        """
        Move the system to a specified band.

        :param position: The band to move to, either as a string representation or a
            numerical value.
        :type position: str or int
        :return: The result of moving to the specified band.
        :rtype: Depends on the specific implementation.
        :raises KeyError: If the specified band is not found in the bands dictionary.
        """
        bands = {
            "Band 1": 1,
            "Band 2": 2,
            "Band 3": 3,
            "Band 4": 4,
            "Band 5a": 5,
            "Band 5b": 6,
            "Band 5c": 7,
        }
        logger.info("move to band:", position)
        if not (isinstance(position, str)):
            return self.commands["Management.Move2Band"](position)
        else:
            return self.commands["Management.Move2Band"](bands[position])

    def abs_azel(
        self, az_angle, el_angle, az_velocity: float = None, el_velocity: float = None
    ):
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
        :raises ValueError: If az_velocity or el_velocity are not provided.
        """
        if az_velocity is None or el_velocity is None:
            logger.error(
                "abs_azel requires now the velocities for az and el "
                "as third and fourth parameters!"
            )
            return
        logger.info(
            f"abs az: {az_angle:.4f} el: {el_angle:.4f}, "
            f"az velocity: {az_velocity:.4f}, el velocity: {el_velocity:.4f}"
        )
        return self.commands["Management.Slew2AbsAzEl"](
            az_angle, el_angle, az_velocity, el_velocity
        )

    # commands to ACU
    def activate_az(self):
        """
        Activate the azimuth functionality.

        This method activates azimuth by calling the activate_dmc method with a
        parameter of 0.

        :return: The result of activating the azimuth functionality.
        :rtype: unknown
        """
        logger.info("activate azimuth")
        return self.activate_dmc(0)

    def deactivate_az(self):
        """
        Deactivate azimuth functionality.

        :return: The result of deactivating azimuth.
        :raises Exception: May raise exceptions if there are errors during the
            deactivation process.
        """
        logger.info("deactivate azimuth")
        return self.deactivate_dmc(0)

    def activate_el(self):
        """
        Activate the elevation.

        :return: The result of activating elevation.
        :rtype: int
        """
        logger.info("activate elevation")
        return self.activate_dmc(1)

    def deactivate_el(self):
        """
        Deactivate elevation.

        This method deactivates elevation.

        :return: The result of deactivating elevation.
        :rtype: int
        """
        logger.info("deactivate elevation")
        return self.deactivate_dmc(1)

    def activate_fi(self):
        """
        Activate the feed indexer.

        :return: The result of activating the feed indexer.
        :rtype: Whatever type the activate_dmc method returns.
        """
        logger.info("activate feed indexer")
        return self.activate_dmc(2)

    def deactivate_fi(self):
        """
        Deactivate the feed indexer.

        :return: The result of deactivating the feed indexer.
        :rtype: unknown
        """
        logger.info("deactivate feed indexer")
        return self.deactivate_dmc(2)

    def abs_azimuth(self, az_angle, az_vel):
        """
        Calculate the absolute azimuth based on azimuth angle and azimuth velocity.

        :param az_angle: The azimuth angle value.
        :type az_angle: float
        :param az_vel: The azimuth velocity value.
        :type az_vel: float
        :return: The absolute azimuth calculated.
        :rtype: float
        """
        logger.info(f"abs az: {az_angle:.4f} vel: {az_vel:.4f}")
        return self.commands["Management.Slew2AbsSingleAx"](0, az_angle, az_vel)

    def abs_elevation(self, el_angle, el_vel):
        """
        Calculate the absolute elevation angle and velocity.

        :param el_angle: The absolute elevation angle.
        :type el_angle: float
        :param el_vel: The elevation velocity.
        :type el_vel: float
        :return: The result of the Management.Slew2AbsSingleAx command.
        :rtype: unknown
        """
        logger.info(f"abs el: {el_angle:.4f} vel: {el_vel:.4f}")
        return self.commands["Management.Slew2AbsSingleAx"](1, el_angle, el_vel)

    def abs_feed_indexer(self, fi_angle, fi_vel):
        """
        Calculate the absolute feed indexer value.

        :param fi_angle: The angle value for the feed indexer.
        :type fi_angle: float
        :param fi_vel: The velocity value for the feed indexer.
        :type fi_vel: float
        :return: The result of the feed indexer operation.
        :rtype: float
        """
        logger.info(f"abs fi: {fi_angle:.4f} vel: {fi_vel:.4f}")
        return self.commands["Management.Slew2AbsSingleAx"](2, fi_angle, fi_vel)

    def load_static_offset(self, az_offset, el_offset):
        """
        Load static azimuth and elevation offsets for tracking.

        :param az_offset: The azimuth offset value.
        :type az_offset: float

        :param el_offset: The elevation offset value.
        :type el_offset: float

        :return: The result of loading the static offsets.
        :rtype: type returned by self.commands['Tracking.TrackLoadStaticOff']

        This function logs the provided azimuth and elevation offsets before loading
        them.

        Example usage:

        .. code-block:: python

            load_static_offset(0.1, 0.2)
        """
        logger.info(f"offset az: {az_offset:.4f} el: {el_offset:.4f}")
        return self.commands["Tracking.TrackLoadStaticOff"](az_offset, el_offset)

    def load_program_track(self, load_type, entries, t, az, el) -> None:
        """
        Load a program track with provided entries, time offsets, azimuth and elevation.

        :param load_type: Type of track data being loaded.
        :type load_type: str
        :param entries: Number of entries in the track table.
        :type entries: int
        :param t: List of time offset values.
        :type t: list
        :param az: List of azimuth values.
        :type az: list
        :param el: List of elevation values.
        :type el: list
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
        for index in range(len(t)):
            row = f"{index:03d}:{t[index]},{az[index]},{el[index]};"
            table += row
        logger.debug(f"Track table that will be sent to DS:{table}")
        byte_string = table.encode()
        try:
            return self.commands["Tracking.TrackLoadTable"](
                load_type,
                asyncua.ua.UInt16(entries),
                asyncua.ua.ByteString(byte_string),
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
        logger.debug(f"Track table that will be sent to DS:{table}")
        byte_string = table.encode()
        try:
            return self.commands["Tracking.TrackLoadTable"](
                load_type,
                asyncua.ua.UInt16(entries),
                asyncua.ua.ByteString(byte_string),
            )
        except Exception as e:
            logger.exception(
                "Uploading of a track table in the old format failed too. "
                "Please check that your track table is correctly formatted, "
                "not empty and contains valid entries."
            )
            raise e

    def start_program_track(self, start_time, start_restart_or_stop: bool = True):
        # unused
        # ptrackA = 11
        # interpol_modes
        # NEWTON = 0
        # SPLINE = 1
        # start_track_modes
        # AZ_EL = 1
        # RA_DEC = 2
        # RA_DEC_SC = 3  #shortcut
        """
        Start tracking a program.

        :param start_time: The time at which tracking should start.
        :type start_time: datetime
        :param start_restart_or_stop: A boolean indicating whether to start, restart, or
            stop tracking. Defaults to True (start).
        :type start_restart_or_stop: bool
        :return: The result of starting the program track.
        :rtype: Any
        """
        return self.commands["Tracking.TrackStart"](
            1, start_time, start_restart_or_stop
        )

    def acu_ska_track(self, number_of_entries: int = None, body: bytes = None):
        """
        ACU SKA track.

        :param number_of_entries: Number of entries.
        :type number_of_entries: int
        :param body: Body of bytes.
        :type body: bytes
        """
        logger.info("acu ska track")
        if number_of_entries is None:
            logger.error(
                "acu_ska_track requires now as first parameter the "
                "number of entries!"
            )
            return
        self.commands["Tracking.TrackLoadTable"](0, number_of_entries, body)

    def acu_ska_track_stoploadingtable(self):
        """
        Stop loading table track.

        This function is not implemented because the function "stopLoadingTable" is not
        supported by the OPC UA server.
        """
        logger.error(
            "Not implemented because the function "
            '"stopLoadingTable" is not supported by the OPC UA '
            "server."
        )
        return
        # self.scu_put('/acuska/stopLoadingTable')

    def format_tt_line(
        self, t, az, el, capture_flag: int = 1, parallactic_angle: float = 0.0
    ):
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

    def format_body(self, t, az, el):
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
        for i in range(len(t)):
            body += self.format_tt_line(t[i], az[i], el[i])
        return body

    # status get functions goes here

    def status_Value(self, sensor):  # noqa: N802
        """
        Get the value of a specific sensor from the attributes dictionary.

        :param self: The object instance.
        :param sensor: The key identifying the specific sensor.
        :type sensor: str
        :return: The value of the specified sensor.
        :rtype: any
        """
        return self.attributes[sensor].value

    def status_finalValue(self, sensor):  # noqa: N802
        """
        Return the final value status of a sensor.

        :param sensor: The sensor for which to get the final value status.
        :type sensor: str
        """
        logger.error(
            "Not implemented because the function "
            '"finalValue" is not supported by the OPC UA '
            "server."
        )
        return
        # return self.status_Value(sensor)
        # r = self.scu_get('/devices/statusValue',
        #       {'path': sensor})
        # data = r.json()['finalValue']
        # logger.info('finalValue: ', data)
        # return data

    def commandMessageFields(self, commandPath):  # noqa: N802,N803
        """
        Generate message fields for a specific command path.

        :param commandPath: The specific path for the command.
        :type commandPath: str
        """
        logger.error(
            "Not implemented because the function "
            '"commandMessageFields" is not supported by the OPC UA '
            "server."
        )
        return
        # r = self.scu_get('/devices/commandMessageFields',
        #       {'path': commandPath})
        # return r

    def statusMessageField(self, statusPath):  # noqa: N802,N803
        """
        Retrieve the status message field.

        :param statusPath: The path to the status message field.
        :type statusPath: str
        """
        logger.error(
            "Not implemented because the function "
            '"statusMessageFields" is not supported by the OPC UA '
            "server."
        )
        return
        # r = self.scu_get('/devices/statusMessageFields',
        #       {'deviceName': statusPath})
        # return r

    # ppak added 1/10/2020 as debug for onsite SCU version
    # but only info about sensor, value itself is murky?
    def field(self, sensor):
        """
        Return data for a specific sensor field.

        :param sensor: The name of the sensor field to retrieve data for.
        :type sensor: str
        :return: Data for the specified sensor field.
        :rtype: dict
        """
        logger.error(
            'Not implemented because the function "field" is not '
            "supported by the OPC UA server."
        )
        return
        # old field method still used on site
        r = self.scu_get("/devices/field", {"path": sensor})
        # data = r.json()['value']
        data = r.json()
        return data

    # logger functions goes here

    def create_logger(self, config_name, sensor_list):
        """
        PUT create a config for logging.

        Usage:
        create_logger('HN_INDEX_TEST', hn_feed_indexer_sensors)
        or
        create_logger('HN_TILT_TEST', hn_tilt_sensors)
        """
        logger.info("create logger")
        r = self.scu_put(
            "/datalogging/config", {"name": config_name, "paths": sensor_list}
        )
        return r

    """unusual does not take json but params"""

    def start_logger(self, filename):
        """
        Start logging data to a specified file.

        :param filename: The name of the file to log data to.
        :type filename: str
        :return: Response from the server.
        :rtype: str
        """
        logger.info("start logger: ", filename)
        r = self.scu_put("/datalogging/start", params="configName=" + filename)
        return r

    def stop_logger(self):
        """
        Stop the data logging process.

        :return: The response from stopping the data logging.
        :rtype: Depends on the response from the SCU.
        """
        logger.info("stop logger")
        r = self.scu_put("/datalogging/stop")
        return r

    def logger_state(self):
        #        logger.info('logger state ')
        """
        Get the current state of the logger.

        :return: The current state of the logger.
        :rtype: str
        """
        r = self.scu_get("/datalogging/currentState")
        # logger.info(r.json()['state'])
        return r.json()["state"]

    def logger_configs(self):
        """
        Get logger configurations.

        :return: The logger configurations.
        :rtype: dict
        """
        logger.info("logger configs ")
        r = self.scu_get("/datalogging/configs")
        return r

    def last_session(self):
        """GET last session."""
        logger.info("Last sessions ")
        r = self.scu_get("/datalogging/lastSession")
        session = r.json()["uuid"]
        return session

    def logger_sessions(self):
        """GET all sessions."""
        logger.info("logger sessions ")
        r = self.scu_get("/datalogging/sessions")
        return r

    def session_query(self, uid):
        """
        GET specific session only - specified by uid number.

        Usage:
            session_query('16')
        """
        logger.info("logger sessioN query uid ")
        r = self.scu_get("/datalogging/session", {"uid": uid})
        return r

    def session_delete(self, uid):
        """
        DELETE specific session only - specified by uid number.

        Not working - returns response 500
        Usage:
        session_delete('16')
        """
        logger.info("delete session ")
        r = self.scu_delete("/datalogging/session", params="uid=" + uid)
        return r

    def session_rename(self, uid, new_name):
        """
        RENAME specific session only - specified by uid number and new session name.

        Not working
        Works in browser display only, reverts when browser refreshed!
        Usage:
        session_rename('16','koos')
        """
        logger.info("rename session ")
        r = self.scu_put("/datalogging/session", params={"uid": uid, "name": new_name})
        return r

    def export_session(self, uid, interval_ms=1000):
        """
        EXPORT specific session.

        By uid and with interval output r.text could be directed to be saved to file.

        Usage:
        export_session('16',1000)
        or export_session('16',1000).text
        """
        logger.info("export session ")
        r = self.scu_get(
            "/datalogging/exportSession",
            params={"uid": uid, "interval_ms": interval_ms},
        )
        return r

    # sorted_sessions not working yet

    def sorted_sessions(
        self,
        is_descending="True",
        start_value="1",
        end_value="25",
        sort_by="Name",
        filter_type="indexSpan",
    ):
        """
        Retrieve a sorted list of sessions from the data logging endpoint.

        :param is_descending: Flag to specify whether the sorting should be descending
            or ascending. Default is descending.
        :type is_descending: str
        :param start_value: Starting value for the sorting range. Default is 1.
        :type start_value: str
        :param end_value: Ending value for the sorting range. Default is 25.
        :type end_value: str
        :param sort_by: Field to sort by. Default is 'Name'.
        :type sort_by: str
        :param filter_type: Type of filtering to apply. Default is 'indexSpan'.
        :type filter_type: str
        :return: A sorted list of sessions based on the specified parameters.
        :rtype: dict
        """
        logger.info("sorted sessions")
        r = self.scu_get(
            "/datalogging/sortedSessions",
            {
                "isDescending": is_descending,
                "startValue": start_value,
                "endValue": end_value,
                "filterType": filter_type,  # STRING - indexSpan|timeSpan,
                "sortBy": sort_by,
            },
        )
        return r

    # get latest session
    def save_session(self, filename, interval_ms=1000, session="last"):
        """
        Save session data as CSV after EXPORTing it.

        Default interval is 1s. Default is last recorded session if specified no error
        checking to see if it exists.

        Usage:
            export_session('16',1000)
            or export_session('16',1000).text
        """
        from pathlib import Path

        logger.info(
            "Attempt export and save of session: %s at rate %d ms", session, interval_ms
        )
        if session == "last":
            # get all logger sessions, may be many
            # r = self.logger_sessions()
            # [-1] for end of list, and ['uuid'] to get uid of last session in list
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        logger.info("Session id: %s", session)
        file_time = str(int(time.time()))
        file_name = str(filename + "_" + file_time + ".csv")
        file_path = Path.cwd() / "output" / file_name
        logger.info("Log file location:", file_path)
        f = open(file_path, "a+")
        f.write(file_txt)
        f.close()

    # get latest session ADDED BY HENK FOR BETTER FILE NAMING FOR THE OPTICAL TESTS
    # (USING "START" AS TIMESTAMP)
    def save_session13(self, filename, start, interval_ms=1000, session="last"):
        """
        Save session data as CSV after EXPORTing it.

        Default interval is 1s. Default is last recorded session if specified no error
        checking to see if it exists.

        Usage:
            export_session('16',1000)
            or export_session('16',1000).text
        """
        from pathlib import Path

        logger.info(
            "Attempt export and save of session: %s at rate %d ms", session, interval_ms
        )
        if session == "last":
            # get all logger sessions, may be many
            # r = self.logger_sessions()
            # [-1] for end of list, and ['uuid'] to get uid of last session in list
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        logger.info("Session id: %s", session)
        #        file_time = str(int(time.time()))
        file_time = str(int(start))
        file_name = str(filename + "_" + file_time + ".csv")
        file_path = Path.cwd() / "output" / file_name
        logger.info("Log file location:", file_path)
        f = open(file_path, "a+")
        f.write(file_txt)
        f.close()

    def save_session14(self, filename, interval_ms=1000, session="last"):
        """
        Save session data as CSV after EXPORTing it.

        Default interval is 1s. Default is last recorded session if specified no error
        checking to see if it exists.

        Usage:
            export_session('16',1000)
            or export_session('16',1000).text
        """
        from pathlib import Path

        logger.info(
            "Attempt export and save of session: %s at rate %d ms", session, interval_ms
        )
        if session == "last":
            # get all logger sessions, may be many
            # r = self.logger_sessions()
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        logger.info("Session id: %s", session)
        file_name = str(filename + ".csv")
        file_path = Path.cwd() / "output" / file_name
        logger.info("Log file location:", file_path)
        f = open(file_path, "a+")
        f.write(file_txt)
        f.close()

    # Simplified one line commands particular to test section being peformed

    # wait seconds, wait value, wait finalValue
    def wait_duration(self, seconds):
        """
        Wait for a specified duration in seconds.

        :param seconds: The duration to wait in seconds.
        :type seconds: float
        """
        logger.info(f"  wait for {seconds:.1f}s", end="")
        time.sleep(seconds)
        logger.info(" done *")

    def wait_value(self, sensor, value):
        """
        Wait until a sensor reaches a specific value.

        :param sensor: The sensor to monitor.
        :type sensor: str
        :param value: The value to wait for.
        :type value: int
        """
        logger.info(f"wait until sensor: {sensor} == value {value}")
        while self.attributes[sensor].value != value:
            time.sleep(1)
        logger.info(f" {sensor} done *")

    def wait_finalValue(self, sensor, value):  # noqa: N802
        """
        Wait until the sensor reaches a specified value.

        :param sensor: The sensor to monitor.
        :type sensor: str
        :param value: The target value for the sensor.
        :type value: int
        :raises: If the sensor does not reach the target value within a reasonable time.
        """
        logger.info(f"wait until sensor: {sensor} == value {value}")
        while self.status_finalValue(sensor) != value:
            time.sleep(1)
        logger.info(f" {sensor} done *")

    # Simplified track table functions


if __name__ == "__main__":
    logger.info("main")
