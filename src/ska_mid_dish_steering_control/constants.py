"""Common enumerated types and other constants used in the SCU package."""

from enum import Enum, IntEnum
from importlib.metadata import version
from pathlib import Path
from typing import Final

from platformdirs import user_cache_dir

__version__: Final = version("ska-mid-dish-steering-control")

# Constants
USER_CACHE_DIR: Final = Path(
    user_cache_dir(appauthor="SKAO", appname="ska-mid-dish-steering-control")
)
SUBSCRIPTION_RATE_MS: Final = 100


class Command(Enum):
    """
    Commands of Dish Structure Controller (DSC) used in SCU methods.

    It needs to be kept up to date with the ICD.
    """

    # CommandArbiter
    TAKE_AUTH = "CommandArbiter.Commands.TakeAuth"
    RELEASE_AUTH = "CommandArbiter.Commands.ReleaseAuth"
    # Management
    ACTIVATE = "Management.Commands.Activate"
    DEACTIVATE = "Management.Commands.DeActivate"
    MOVE2BAND = "Management.Commands.Move2Band"
    RESET = "Management.Commands.Reset"
    SET_ON_SOURCE_THRESHOLD = "Management.Commands.SetOnSourceThreshold"
    SET_POWER_MODE = "Management.Commands.SetPowerMode"
    SLEW2ABS_AZ_EL = "Management.Commands.Slew2AbsAzEl"
    SLEW2ABS_SINGLE_AX = "Management.Commands.Slew2AbsSingleAx"
    SLEW2REL_AZ_EL = "Management.Commands.Slew2RelAzEl"
    SLEW2REL_SINGLE_AX = "Management.Commands.Slew2RelSingleAx"
    STOP = "Management.Commands.Stop"
    STOW = "Management.Commands.Stow"
    TRACK_START_DM = "Management.Commands.TrackStartDM"
    TRACK_START_POLY_DM = "Management.Commands.TrackStartPolyDM"
    # Pointing
    AMBTEMP_CORR_SETUP = "Pointing.Commands.AmbTempCorrSetup"
    PM_CORR_ON_OFF = "Pointing.Commands.PmCorrOnOff"
    STATIC_PM_SETUP = "Pointing.Commands.StaticPmSetup"
    TILT_CAL_SETUP = "Pointing.Commands.TiltCalSetup"
    # Safety
    INTERLOCK_ACK = "Safety.Commands.InterlockAck"
    MOVE_STOW_PIN = "Safety.Commands.MoveStowPin"
    # Time source
    SET_TIME_SOURCE = "Time_cds.Commands.SetTimeSource"
    # Tracking
    TRACK_LOAD_STATIC_OFF = "Tracking.Commands.TrackLoadStaticOff"
    TRACK_LOAD_TABLE = "Tracking.Commands.TrackLoadTable"
    TRACK_START = "Tracking.Commands.TrackStart"
    TRACK_START_POLY = "Tracking.Commands.TrackStartPoly"


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
JSONData = (  # Type hint for any JSON-encodable data
    None
    | bool
    | int
    | float
    | str
    | list["JSONData"]  # A list can contain more JSON-encodable data
    | dict[str, "JSONData"]  # A dict must have str keys and JSON-encodable data
    | tuple["JSONData", ...]  # A tuple can contain more JSON-encodable data
)
