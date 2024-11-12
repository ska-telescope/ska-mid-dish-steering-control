"""Tests configuration."""

import logging
import subprocess
import time
from queue import Queue
from typing import Callable, Generator

import pytest

from ska_mid_dish_steering_control import SCU, SteeringControlUnit


@pytest.fixture(autouse=True)
def configure_logging():
    """Configure default logging levels for modules."""
    logging.getLogger("asyncua").setLevel(logging.ERROR)
    logging.getLogger("ska-mid-ds-scu").setLevel(logging.DEBUG)


class StubSCU(SteeringControlUnit):
    """SteeringControlUnit stub class (no real subscriptions)."""

    subscriptions: dict = {}

    def __init__(self) -> None:
        """Init."""
        super().__init__(
            host="127.0.0.1",
            port=4841,
            endpoint="/dish-structure/server/",
            namespace="http://skao.int/DS_ICD/",
            timeout=25,
        )
        super().connect_and_setup()

    def subscribe(
        self,
        attributes: str | list[str] | None = None,
        period: int | None = None,
        data_queue: Queue | None = None,
        bad_shutdown_callback: Callable[[str], None] | None = None,
    ) -> tuple[int, list, list]:
        """
        Subscribe to a data source.

        :param attributes: Optional. Attributes related to the subscription.
        :type attributes: dict
        :param period: Optional. Period of the subscription.
        :type period: int
        :param data_queue: Optional. Queue to store incoming data.
        :type data_queue: list
        :param bad_shutdown_callback: will be called if a BadShutdown subscription
            status notification is received, defaults to None.
        :return: unique identifier for the subscription and lists of missing/bad nodes.
        :rtype: tuple[int, list, list]
        """
        uid = time.monotonic_ns()
        self.subscriptions[uid] = {}
        return uid, [], []

    def unsubscribe(self, uid: int) -> None:
        """
        Unsubscribe a user from the subscription.

        :param uid: The unique identifier of the user to unsubscribe.
        :type uid: int
        :raises IndexError: If the user ID is invalid.
        """
        _ = self.subscriptions.pop(uid)


@pytest.fixture(scope="class", name="ds_simulator_opcua_server_mock")
def ds_simulator_opcua_server_mock_fixture() -> Generator:
    """Start DSSimulatorOPCUAServer as separate process."""
    simulator_process = subprocess.Popen(  # pylint: disable=consider-using-with
        ["python", "tests/resources/ds_opcua_server_mock.py"]
    )
    # Wait for some time to ensure the simulator is fully started
    time.sleep(5)
    yield simulator_process
    simulator_process.terminate()
    simulator_process.wait()  # Wait for the process to terminate completely


# pylint: disable=unused-argument
@pytest.fixture(scope="class", name="scu_mock_server")
def scu_mock_server_fixture(ds_simulator_opcua_server_mock: Generator) -> StubSCU:
    """Steering Control Unit with active connection to mock DS server fixture."""
    scu_mock_server = StubSCU()
    return scu_mock_server


@pytest.fixture(name="scu_cetc_simulator", scope="class")
def scu_cetc_simulator_fixture() -> SteeringControlUnit:  # type: ignore
    """Steering Control Unit connected to running CETC simulator."""
    scu = SCU(endpoint="/OPCUA/SimpleServer", namespace="CETC54", authority_name="LMC")
    yield scu
    scu.disconnect_and_cleanup()


@pytest.fixture(name="scu_mid_itf_plc", scope="class")
def scu_mid_itf_plc_fixture() -> SteeringControlUnit:  # type: ignore
    """Steering Control Unit connected to the PLC at the MID-ITF."""
    scu = SCU(host="10.165.3.43", use_nodes_cache=True, authority_name="LMC")
    yield scu
    scu.disconnect_and_cleanup()


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom pytest options for test setup."""
    parser.addoption(
        "--with-cetc-sim",
        action="store_true",
        help="A running CETC54 simulator is available to run the tests against.",
    )
    parser.addoption(
        "--with-plc",
        action="store_true",
        help="The PLC at the MID-ITF is available over VPN to run the tests against.",
    )
