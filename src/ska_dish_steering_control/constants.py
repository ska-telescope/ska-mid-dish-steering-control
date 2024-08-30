"""Common DiSQ enumerated types and other constants used in the package."""

from enum import Enum, IntEnum
from importlib import metadata
from typing import Final

# Constants
PACKAGE_VERSION: Final = metadata.version("ska-dish-steering-control")
SUBSCRIPTION_RATE_MS: Final = 100


# Enumerations
class NodesStatus(Enum):
    """Nodes status."""

    NOT_CONNECTED = "Not connected to server"
    VALID = "Nodes valid"
    ATTR_NOT_FOUND = "Client is missing attribute(s). Check log!"
    NODE_INVALID = "Client has invalid attribute(s). Check log!"
    NOT_FOUND_INVALID = "Client is missing and has invalid attribute(s). Check log!"


class Command(Enum):
    """
    Commands of Dish Structure Controller (DSC) used in SCU methods.

    It needs to be kept up to date with the ICD.
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


class NamePlate(Enum):
    """
    Nodes used for PLC lifetime and identification.

    This needs to be kept up to date with the ICD.
    """

    DISH_ID = "Management.NamePlate.DishId"
    DISH_STRUCTURE_SERIAL_NO = "Management.NamePlate.DishStructureSerialNo"
    DSC_SOFTWARE_VERSION = "Management.NamePlate.DscSoftwareVersion"
    ICD_VERSION = "Management.NamePlate.IcdVersion"
    RUN_HOURS = "Management.NamePlate.RunHours"
    TOTAL_DIST_AZ = "Management.NamePlate.TotalDist_Az"
    TOTAL_DIST_EL_DEG = "Management.NamePlate.TotalDist_El_deg"
    TOTAL_DIST_EL_M = "Management.NamePlate.TotalDist_El_m"
    TOTAL_DIST_FI = "Management.NamePlate.TotalDist_Fi"


class ResultCode(IntEnum):
    """
    Result codes of commands.

    This enum extends the DSC's 'CmdResponseType', which starts from 0 and goes upwards,
    with extra codes below 0 for internal use by SCU and the DiSQ GUI.

    It needs to be kept up to date with the ICD.
    """

    UNKNOWN = -10
    """An unknown result code was returned from the server."""
    EXECUTING = -5
    """SCU is busy executing a batch of commands asynchronously."""
    ENTIRE_TRACK_TABLE_LOADED = -4
    """The entire queued track table was succesfully loaded."""
    CONNECTION_CLOSED = -3
    """A ConnectionError exception was raised by asyncua."""
    UA_BASE_EXCEPTION = -2
    """An unexpected asyncua exception was caught."""
    NOT_EXECUTED = -1
    """Command was not executed by SCU for some reason (never sent to the server)."""
    NO_CMD_AUTH = 0
    """The user does not have command authority."""
    DISH_LOCKED = 1
    """Some interlock is active or E-stop is pressed."""
    COMMAND_REJECTED = 2
    """Command rejected because it cannot be executed in the current dish state."""
    COMMAND_TIMEOUT = 3
    """The command execution exceeded expected duration and was aborted."""
    COMMAND_FAILED = 4
    """Command failed during execution."""
    AXIS_NOT_ACTIVATED = 5
    """The axis is not activated."""
    STOWED = 6
    """The stow pin is deployed."""
    NO_OF_PARAMETERS_ERROR = 7
    """Either too few or too many parameters for the command."""
    PARAMETER_OUT_OF_RANGE = 8
    """One or more of the parameters are outside the expected range."""
    COMMAND_ACTIVATED = 9
    """Command accepted and execution ongoing. Typically for movement commands that take
    an indeterminate amount of time to complete."""
    COMMAND_DONE = 10
    """Command accepted and execution finished. Typically for instant commands that are
    completed in a few milliseconds."""
    NOT_IMPLEMENTED = 11
    """The command has not been implemented."""


# Type aliases
CmdReturn = tuple[ResultCode, str, list[int | None] | None]
