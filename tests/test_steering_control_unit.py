"""Tests for the SteeringControlUnit."""

from datetime import datetime
from time import sleep

import pytest
from conftest import PLC_IP
from packaging.version import Version

# from asyncua.ua import UaError
from pytest import LogCaptureFixture

from ska_mid_dish_steering_control import SCU, SteeringControlUnit, sculib

CETC_SIM_VERSION = "4.4"
PLC_VERSION = "0.0.4"


@pytest.fixture(name="scu")
def scu_fixture(request: pytest.FixtureRequest) -> SteeringControlUnit:  # type: ignore
    """Fixture to select which connected SCU to use."""
    # Switch between two fixtures defined in conftest.py
    with_plc = request.config.getoption("--with-plc")
    if with_plc:
        scu: SteeringControlUnit = request.getfixturevalue("scu_mid_itf_plc")
    else:
        scu = request.getfixturevalue("scu_cetc_simulator")
    # Setup simulator/PLC before running test
    # scu.take_authority("LMC")
    # The following setup is only needed if running tests individually for debugging
    # scu.stow(False)
    # scu.activate_dmc("AzEl")
    # scu.activate_dmc("Fi")
    yield scu
    # Stop any running slews and release authority after test (also done if test failed)
    # The following setup is only needed if running tests individually for debugging
    # scu.stop("AzEl")
    # scu.stop("Fi")
    sleep(0.5)


# pylint: disable=protected-access,unused-argument
class TestSetupTeardown:
    """Test SCU setup and teardown methods with mock simulator."""

    def test_disconnect_and_cleanup(
        self: "TestSetupTeardown",
        scu: SteeringControlUnit,
    ) -> None:
        """Test SCU's cleanup method."""
        scu.disconnect_and_cleanup()
        assert scu._subscriptions == {}
        assert scu._client is None
        assert scu._event_loop_thread is None

    def test_connect_and_setup(
        self: "TestSetupTeardown",
        scu: SteeringControlUnit,
        caplog: LogCaptureFixture,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test the client-server connection setup."""
        scu.connect_and_setup()
        if request.config.getoption("--with-plc"):
            expected_log = [
                f"Connecting to: opc.tcp://{PLC_IP}:4840",
                "Successfully connected to server and initialised SCU client",
            ]
        else:
            expected_log = [
                "Connecting to: opc.tcp://127.0.0.1:4840/OPCUA/SimpleServer",
                "Successfully connected to server and initialised SCU client",
            ]
        for message in expected_log:
            assert message in caplog.messages

    def test_connection_reset(
        self: "TestSetupTeardown",
        scu: SteeringControlUnit,
        caplog: LogCaptureFixture,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test the client-server connection."""
        assert scu.is_connected()
        scu.connection_reset()
        if request.config.getoption("--with-plc"):
            expected_log = [
                f"Connecting to: opc.tcp://{PLC_IP}:4840",
            ]
        else:
            expected_log = [
                "Connecting to: opc.tcp://127.0.0.1:4840/OPCUA/SimpleServer",
            ]
        for message in expected_log:
            assert message in caplog.messages

    def test_as_context_manager(self: "TestSetupTeardown") -> None:
        """Test using SCU as a context manager."""
        with SteeringControlUnit(
            port=4840,
            endpoint="/OPCUA/SimpleServer",
            namespace="CETC54",
            timeout=25,
        ) as scu:
            assert scu.server_version == CETC_SIM_VERSION

    def test_connection_failure(
        self: "TestSetupTeardown",
        caplog: LogCaptureFixture,
    ) -> None:
        """Test result of failed connection."""
        with pytest.raises(Exception):
            SCU(port=4841)
            expected_log = [
                "Connecting to: opc.tcp://127.0.0.1:4841",
                "Cannot connect to the OPC UA server. Please "
                "check the connection parameters that were "
                "passed to instantiate the sculib!",
            ]
            for message in expected_log:
                assert message in caplog.messages

    def test_incompatible_cetc_sim_version(
        self: "TestSetupTeardown",
        caplog: LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test connecting to incompatible CETC simulator version."""
        monkeypatch.setattr(sculib, "COMPATIBLE_CETC_SIM_VER", Version("100.0.0"))
        with pytest.raises(
            RuntimeError,
            match=f"SCU not compatible with v{CETC_SIM_VERSION} of CETC "
            "simulator, only v100.0.0 and up",
        ):
            scu = SCU(
                port=4840,
                endpoint="/OPCUA/SimpleServer",
                namespace="CETC54",
                timeout=25,
            )
            expected_log = [
                "Connecting to: opc.tcp://127.0.0.1:4840/OPCUA/SimpleServer",
                f"SCU not compatible with v{scu.server_version} of CETC "
                "simulator, only v100.0.0 and up",
            ]
            for message in expected_log:
                assert message in caplog.messages


class TestGeneral:
    """Test SCU properties and client-server related methods with mock simulator."""

    def test_properties(
        self: "TestGeneral", scu: SteeringControlUnit, request: pytest.FixtureRequest
    ) -> None:
        """Test the properties."""
        with_plc = request.config.getoption("--with-plc")
        if with_plc:
            assert scu.server_version == PLC_VERSION
        else:
            assert scu.server_version == CETC_SIM_VERSION
        assert scu.nodes != {}
        assert scu.attributes != {}
        assert scu.commands != {}
        assert isinstance(datetime.fromisoformat(scu.plc_prg_nodes_timestamp), datetime)
        if with_plc:
            assert scu.parameter_nodes != {}
            assert scu.parameter_attributes != {}
        else:
            assert scu.parameter_nodes == {}
            assert scu.parameter_attributes == {}
        assert scu.opcua_enum_types != {}

    def test_authority_commands(
        self: "TestGeneral",
        scu: SteeringControlUnit,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test the commands."""
        scu.take_authority("EGUI")
        scu.release_authority()
        expected_log = [
            "Calling command node '2:TakeAuth' with args list: [3]",
            # "Calling command node '2:ReleaseAuth' with args list: [0, 0]",
        ]
        for message in expected_log:
            assert message in caplog.messages

    def test_get_node_list(self: "TestGeneral", scu: SteeringControlUnit) -> None:
        """Test get_node_list method."""
        node_list = scu.get_node_list()
        assert isinstance(node_list, list)
        assert all(isinstance(node, str) for node in node_list)

    def test_get_command_list(self: "TestGeneral", scu: SteeringControlUnit) -> None:
        """Test get_command_list method."""
        command_list = scu.get_command_list()
        assert isinstance(command_list, list)
        assert all(isinstance(command, str) for command in command_list)

    def test_get_attribute_list(self: "TestGeneral", scu: SteeringControlUnit) -> None:
        """Test get_attribute_list method."""
        attribute_list = scu.get_attribute_list()
        assert isinstance(attribute_list, list)
        assert all(isinstance(attribute, str) for attribute in attribute_list)

    def test_get_attribute_data_type(
        self: "TestGeneral", scu: SteeringControlUnit
    ) -> None:
        """Test get_attribute_data_type method."""
        # Test with a known attribute
        data_type = scu.get_attribute_data_type("Pointing.Status.CurrentPointing")
        assert isinstance(data_type, list)
        assert data_type == ["Pointing.Status.CurrentPointing"]

        # Test with an unknown attribute
        # data_type = scu.get_attribute_data_type("UnknownAttribute")
        # assert isinstance(data_type, list)
        # assert data_type == ["Unknown"]
