"""Tests for the SteeringControlUnit."""

import os
from datetime import datetime
from pathlib import Path
from time import sleep

import pytest
from conftest import PLC_IP
from packaging.version import Version
from pytest import LogCaptureFixture

from ska_mid_dish_steering_control import (
    SCU,
    Command,
    ResultCode,
    SteeringControlUnit,
    sculib,
)

CETC_SIM_VERSION = "4.6"
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
    yield scu
    sleep(1)


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
            use_nodes_cache=True,
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
        session_id = scu._session_id
        scu.release_authority()
        expected_log = [
            "Calling command node 'CommandArbiter.Commands.TakeAuth' "
            "with args list: [3]",
            "Calling command node 'CommandArbiter.Commands.ReleaseAuth' "
            f"with args list: [{session_id}, 3]",
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

    def test_get_command_arguments(
        self: "TestGeneral", scu: SteeringControlUnit
    ) -> None:
        """Test get_command_arguments method."""
        command_args = scu.get_command_arguments(scu._client.get_node("i=123456789"))
        assert command_args is None
        command_args = scu.get_command_arguments(scu.nodes[Command.STOW.value][0])
        assert isinstance(command_args, list)
        assert command_args[0] == ("SessionID", "UInt16")
        assert command_args[1] == ("StowAction", "Boolean")

    def test_get_attribute_list(self: "TestGeneral", scu: SteeringControlUnit) -> None:
        """Test get_attribute_list method."""
        attribute_list = scu.get_attribute_list()
        assert isinstance(attribute_list, list)
        assert all(isinstance(attribute, str) for attribute in attribute_list)

    def test_get_attribute_data_type(
        self: "TestGeneral", scu: SteeringControlUnit
    ) -> None:
        """Test get_attribute_data_type method."""
        data_type = scu.get_attribute_data_type("Pointing.Status.CurrentPointing")
        assert isinstance(data_type, list)
        assert data_type == ["Pointing.Status.CurrentPointing"]
        data_type = scu.get_attribute_data_type("NonExistentAttribute")
        assert isinstance(data_type, list)
        assert data_type == ["Unknown"]

    def test_axis_commands(self: "TestGeneral", scu: SteeringControlUnit) -> None:
        """Test the axis commands methods."""
        assert scu.take_authority("EGUI") == (ResultCode.COMMAND_DONE, "CommandDone")
        done = (ResultCode.COMMAND_DONE, "CommandDone", None)
        activated = (ResultCode.COMMAND_ACTIVATED, "CommandActivated", None)
        assert scu.activate_az() == done
        assert scu.activate_el() == done
        assert scu.activate_fi() == done
        sleep(0.5)
        assert scu.abs_azimuth(20.0, 1.0) == activated
        assert scu.abs_elevation(30.0, 1.0) == activated
        assert scu.abs_feed_indexer(30.0, 2.0) == activated
        sleep(0.5)
        azim = scu.convert_enum_to_int("AxisSelectType", "Az")
        assert scu.commands[Command.STOP.value](azim) == done
        elev = scu.convert_enum_to_int("AxisSelectType", "El")
        assert scu.commands[Command.STOP.value](elev) == done
        feed = scu.convert_enum_to_int("AxisSelectType", "Fi")
        assert scu.commands[Command.STOP.value](feed) == done
        sleep(0.5)
        assert scu.abs_azel(20.0, 30.0, 1.0, 1.0) == activated
        sleep(0.5)
        azel = scu.convert_enum_to_int("AxisSelectType", "AzEl")
        assert scu.commands[Command.STOP.value](azel) == done
        sleep(0.5)
        assert scu.deactivate_az() == done
        assert scu.deactivate_el() == done
        assert scu.deactivate_fi() == done
        sleep(0.5)
        assert scu.move_to_band("Band_1") == activated
        assert scu.interlock_acknowledge_dmc() == (
            ResultCode.COMMAND_REJECTED,
            "CommandRejected",
            None,
        )
        assert scu.reset_dmc(azim) == (
            ResultCode.AXIS_NOT_ACTIVATED,
            "AxisNotActivated",
            None,
        )

    def test_static_pointing_methods(
        self: "TestGeneral",
        scu: SteeringControlUnit,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test the static pointing model methods."""
        scu.import_static_pointing_model(Path("nonexistent.json"))
        assert (
            "Caught exception trying to read file 'nonexistent.json': "
            "[Errno 2] No such file or directory: 'nonexistent.json'" in caplog.messages
        )
        scu.export_static_pointing_model("Band_3")
        assert (
            "Static pointing model not setup for 'Band_3' in SCU client."
            in caplog.messages
        )
        scu.setup_static_pointing_model("Band_3")
        assert (
            "Static pointing model not setup for 'Band_3' in SCU client."
            in caplog.messages
        )
        assert scu.read_static_pointing_model("Band_2") != {}
        assert (
            "Creating new StaticPointingModel instance for 'Band_2'." in caplog.messages
        )
        scu.import_static_pointing_model(Path("tests/resources/gpm-SKA063-Band_2.json"))
        assert (
            "Successfully imported static pointing model from "
            "'tests/resources/gpm-SKA063-Band_2.json' for 'Band_2'." in caplog.messages
        )
        scu.set_static_pointing_value("Band_2", "IA", 10.0)
        assert scu.get_static_pointing_value("Band_2", "IA") == 10.0
        assert scu.setup_static_pointing_model("Band_2") == (
            ResultCode.COMMAND_DONE,
            "CommandDone",
            None,
        )
        scu.export_static_pointing_model("Band_2")
        assert (
            "Successfully exported 'Band_2' static pointing model." in caplog.messages
        )
        assert os.path.exists("gpm-SKA063-Band_2.json")
        os.remove("gpm-SKA063-Band_2.json")
        scu.export_static_pointing_model("Band_2", antenna="SKA100")
        assert os.path.exists("gpm-SKA100-Band_2.json")
        os.remove("gpm-SKA100-Band_2.json")
        scu.export_static_pointing_model("Band_2", Path("Band_2.json"))
        assert os.path.exists("Band_2.json")
        os.remove("Band_2.json")
