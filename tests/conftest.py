"""Tests configuration."""

import logging

import pytest

from ska_mid_dish_steering_control import SCU, SteeringControlUnit

PLC_IP = "10.165.3.43"


@pytest.fixture(autouse=True)
def configure_logging():
    """Configure default logging levels for modules."""
    logging.getLogger("asyncua").setLevel(logging.ERROR)
    logging.getLogger("ska-mid-ds-scu").setLevel(logging.DEBUG)


@pytest.fixture(name="scu_cetc_simulator", scope="class")
def scu_cetc_simulator_fixture() -> SteeringControlUnit:  # type: ignore
    """Steering Control Unit connected to running CETC simulator."""
    scu = SCU(endpoint="/OPCUA/SimpleServer", namespace="CETC54", authority_name="LMC")
    yield scu
    if scu.is_connected():
        scu.disconnect_and_cleanup()
    else:
        scu.cleanup_resources()


@pytest.fixture(name="scu_mid_itf_plc", scope="class")
def scu_mid_itf_plc_fixture() -> SteeringControlUnit:  # type: ignore
    """Steering Control Unit connected to the PLC at the MID-ITF."""
    scu = SCU(host=PLC_IP, use_nodes_cache=True, authority_name="LMC")
    yield scu
    if scu.is_connected():
        scu.disconnect_and_cleanup()
    else:
        scu.cleanup_resources()


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest options for test setup."""
    parser.addoption(
        "--with-plc",
        action="store_true",
        help="The PLC at the MID-ITF is available over VPN to run the tests against.",
    )
