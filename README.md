# SKA-Mid Dish Structure Steering Control Unit

Steering Control Unit (SCU) for a SKA-Mid Dish Structure Controller OPC UA server.

This module contains an OPC UA client class that simplifies connecting to a server and
calling methods on it, reading or writing attributes.

## How to use SCU

The simplest way to initialise a ``SteeringControlUnit``, is to use the ``SteeringControlUnit()``
object generator method. It creates an instance, connects to the server, and can also
take command authority immediately. Provided here are some of the defaults which can be
overwritten by specifying the named parameter:

    from ska_mid_dish_steering_control import SteeringControlUnit
    scu = SteeringControlUnit(
        host="localhost",
        port=4840,
        endpoint="",
        namespace="",
        timeout=10.0,
    )
    scu.connect_and_setup()
    scu.take_authority("LMC") # Use the authority of your choice
    # Do things with the scu instance..
    scu.disconnect_and_cleanup()

All nodes from and including the PLC_PRG node are stored in the nodes dictionary:
``scu.nodes``. The keys are the full node names, the values are the Node objects.
The full names of all nodes can be retrieved with:

    scu.get_node_list()

Every value in ``scu.nodes`` exposes the full OPC UA functionality for a node.
Note: When accessing nodes directly, it is mandatory to await any calls:

    node = scu.nodes["PLC_PRG"]
    node_name = (await node.read_display_name()).Text

The command methods that are below the PLC_PRG node's hierarchy can be accessed through
the commands dictionary:

    scu.get_command_list()

When you want to call a command, please check the ICD for the parameters that the
commands expects. Checking for the correctness of the parameters is not done here
in the SCU class, but in the PLC's OPC UA server. Once the parameters are in order,
calling a command is really simple:

    result = scu.commands["COMMAND_NAME"](YOUR_PARAMETERS)

You can also use the ``Command`` enum, as well as the helper method for converting types
from the OPC UA server to the correct base integer type:

    from ska_mid_dish_steering_control.constants import Command
    axis = scu.convert_enum_to_int("AxisSelectType", "Az")
    result = scu.commands[Command.ACTIVATE.value](axis)

For instance, command the PLC to slew to a new position:

    az = 182.0; el = 21.8; az_v = 1.2; el_v = 2.1
    code, msg, _ = scu.commands[Command.SLEW2ABS_AZ_EL.value](az, el, az_v, el_v)

The OPC UA server also provides read-writeable and read-only variables, commonly
called in OPC UA "attributes". An attribute's value can easily be read:

    scu.attributes["Azimuth.p_Set"].value

If an attribute is writable, then a simple assignment does the trick:

    scu.attributes["Azimuth.p_Set"].value = 1.2345

In case an attribute is not writeable, the OPC UA server will report an error:

`*** Exception caught: User does not have permission to perform the requested operation.
(BadUserAccessDenied)`

You can subscribe to a single OPC UA node using the subscribe() method and use a 
separate thread to monitor events being added to a queue:

    import queue
    attr_monit_queue = queue.Queue()
    scu.subscribe("Management.Status.DscState", 100, attr_monit_queue)

    # Example of monitoring events added to the queue
    try:
        while True:
            data = attr_monit_queue.get(timeout=30)  # Wait for data for 30 seconds
            print(f"Received data: {data}")
    except queue.Empty:
        print("No data received within 30 seconds.")

To subscribe to multiple OPC UA nodes, Pass a list instead of string. eg

    scu.subscribe(["Management.Status.DscState", "Safety.Status.StowPinStatus"],100,attr_monit_queue)

## Tests

The unit tests of the ``SteeringControlUnit`` must be run with an instance connected to the CETC simulator or PLC. The CETC simulator is used by default locally and in the CI python-test job. 

In order to run the tests locally, you need the latest CETC simulator image (named 'simulator' and tagged with a version number) already built in your local docker image registry from [ska-te-dish-structure-simulator](https://gitlab.com/ska-telescope/ska-te-dish-structure-simulator). The ``make python-test`` target should always be used, as it executes a pre- and post-target to start and stop the container. If you want to run individual tests, you need to use:

    make python-test PYTHON_TEST_FILE=tests/test_steering_control_unit.py::...

To run the tests (only some will be) against the PLC at the Mid-ITF, use the defined custom pytest option:

    make python-test PYTHON_VARS_AFTER_PYTEST=--with-plc
