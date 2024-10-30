"""Module for import/export of a global static pointing model from/to a JSON file."""
# Original author: Justin Jonas @skao.int

import json
import logging
import re
from pathlib import Path

from jsonschema import ValidationError, validate

from . import utils
from .constants import JSONData

logger = logging.getLogger("ska-mid-ds-scu")


# mypy: ignore-errors
class StaticPointingModel:
    """
    Class for the communication of pointing model coefficients to TMC via a JSON file.

    The JSON structure contains more information than just the raw
    coefficient values required by the Dish Structure controller.
    It is expected that this associated metadata (attrs) might be used
    for validation and archived in the Telescope Model.
    The JSON structure is quite flat becasue the coefficients
    are generated on a per dish/band basis, so individual
    files are generated for each dish/band permutation.
    Refer to Table 5.4.1.4 in the Dish Structure / DishLMC ICD
    (301-000000-082) for the definition of pointing model
    coefficients and receiver bands.

    NOTE: These coefficients are subject to change over the
    course of AA0.5.  The ICD is under configuration control
    and an ECP, not an ADR, will be raised when changes are
    to be implemented.

    Versioning of JSON schema as per ADR-22
    Dish naming convention as per ADR-32
    JSON structure and key names as per ADR-35
    Assumes ADR-57 decission on functional allocation of pointing model
    Ref: SDR-1249 & SS-131

    Future consideration:
    - Methods to get dish, band, rms and attrs.
    - More robust error checking and exception handling.
    """

    INTERFACE_PREFIX = "https://schema.skao.int/ska-mid-dish-gpm/"
    VERSION = "1.2"
    ANTENNA_RE_PATTERN = "(^SKA[0-9]{3}$)|(^MKT[0-9]{3}$)"
    BAND_LIST = [
        "Optical",
        "Band_1",
        "Band_2",
        "Band_3",
        "Band_4",
        "Band_5a",
        "Band_5b",
        "Band_6",
    ]
    ATTRS_DEF_DICT = {
        "obs_date_times": [],
        "eb_ids": [],
        "analysis_script": "",
        "analysis_date_time": "",
        "comment": "",
    }
    # The order of the coefficients in the dict below must match the argument order of
    # the 'StaticPmSetup' command as specified in the ICD.
    DSC_COEFFICIENTS_DICT = {
        "IA": "arcsec",
        "CA": "arcsec",
        "NPAE": "arcsec",
        "AN": "arcsec",
        "AN0": "arcsec",
        "AW": "arcsec",
        "AW0": "arcsec",
        "ACEC": "arcsec",
        "ACES": "arcsec",
        "ABA": "arcsec",
        "ABphi": "deg",
        "IE": "arcsec",
        "ECEC": "arcsec",
        "ECES": "arcsec",
        "HECE4": "arcsec",
        "HESE4": "arcsec",
        "HECE8": "arcsec",
        "HESE8": "arcsec",
    }
    RMS_LIST = ["xel_rms", "el_rms", "sky_rms"]
    RMS_DEF_DICT = {"value": None, "units": "arcsec"}
    COEFF_DEF_DICT = {
        "value": 0.0,
        "units": None,
        "stderr": None,
        "used": False,
    }
    MIN_COEFF = -2000.0
    MAX_COEFF = +2000.0

    def __init__(self, schema_file_path: Path) -> None:
        """
        Set up lists of pointing model coefficient (TPOINT convention) and band names.

        The coefficient
        names are limited to those supported by the dish structure
        controller (DSC).
        Create a dictionary that desribes the static pointing
        model for a specific antenna and band combination with
        nulls and zeros as default values where appropriate.
        Also includes metadata items (attrs and fitting errors)
        that are not required by the Dish Structure controller
        (and probably Dish LMC).
        Note: The coefficient names follow the TPOINT convention
        except for ABA and ABphi.
        Except for ABA and ABphi, there is a trivial mapping,
        modulo a sign convention, with katpoint and VLBI Field
        System Pnn coefficients.

        :param schema_file_path: Path to the schema used for JSON validation.
        """
        # The global pointing model dict's minimal structure needed to pass validation
        self._gpm_dict: dict[str, JSONData] = {
            "interface": self.INTERFACE_PREFIX + self.VERSION,
            "antenna": "SKAxxx",
            "band": None,
            "coefficients": {},
        }
        for coeff in self.DSC_COEFFICIENTS_DICT:
            self._gpm_dict["coefficients"].update({coeff: {}})
        # Create schema
        self._schema = utils.load_json_file(schema_file_path)

    @property
    def coefficients(self) -> list[str]:
        """
        List of loaded coefficients' names.

        :return: List of loaded coefficients' names.
        """
        return list(self._gpm_dict["coefficients"].keys())

    def get_all_coefficient_values(self) -> list[float]:
        """
        Return a list of all the available coefficients' values.

        :returns:
            - The actual value if present.
            - 0.0 if not present.
        """
        values = []
        for coeff_name in self.DSC_COEFFICIENTS_DICT:
            if coeff_name in self._gpm_dict["coefficients"]:
                values.append(self._gpm_dict["coefficients"][coeff_name]["value"])
            else:
                values.append(0.0)
        return values

    def get_coefficient_value(self, coeff_name: str) -> float:
        """
        Return the value of named pointing coefficient from the structure.

        :param coeff_name: Must be a name in the coefficient list.
        :returns:
            - The actual value if present.
            - 0.0 if not present.
            - NaN if invalid coefficient name.
        """
        if coeff_name in self.DSC_COEFFICIENTS_DICT:
            if coeff_name in self._gpm_dict["coefficients"]:
                return self._gpm_dict["coefficients"][coeff_name]["value"]
            return 0.0
        return float("NaN")

    def set_coefficient(self, coeff_name: str, **kwargs: float | str) -> None:
        """
        Set the named pointing coefficient in the structure.

        Together with its units and standard error returned by SVD.
        Only "value" is mandatory for the DS controller,
        "units" and "stderr" are included for archival.
        DS controller requires coefficient values in units of arcsec.

        :param coeff_name: Must be a name in the coefficient list.
        :keyword value: Coefficient value (mandatory).
        :keyword units: Units of value.
        :keyword stderr: Standard error of value.
        """
        if coeff_name in self.DSC_COEFFICIENTS_DICT:
            for key, val in kwargs.items():
                if key in self.COEFF_DEF_DICT:
                    if key == "value":
                        if self.MIN_COEFF <= val <= self.MAX_COEFF:
                            self._gpm_dict["coefficients"][coeff_name][
                                key
                            ] = utils.check_float(val)
                    elif key == "stderr":
                        self._gpm_dict["coefficients"][coeff_name][
                            key
                        ] = utils.check_float(val)
                    else:
                        self._gpm_dict["coefficients"][coeff_name][key] = val

    def set_rms(self, rms_name: str, **kwargs: float | str) -> None:
        """
        Set the RMS errors associated with the fit in the structure.

        Not used by the DS controller.

        :param rms_name: Must be a name in the RMS list.
        :keyword value: RMS value.
        :keyword units: Units of value.
        """
        if rms_name in self.RMS_LIST:
            for key, val in kwargs.items():
                if key in self.RMS_DEF_DICT:
                    self._gpm_dict["rms_fits"][rms_name][key] = utils.check_float(val)

    def set_antenna(self, ant_name: str) -> bool:
        """
        Set the antenna name in the structure.

        :param ant_name: Name of the antenna.
        :returns: True if ant_name has correct format, else False.
        """
        if re.search(self.ANTENNA_RE_PATTERN, ant_name) is None:
            return False
        self._gpm_dict.update({"antenna": ant_name})
        return True

    def get_band(self) -> str:
        """
        Get the band name.

        :returns: Band name.
        """
        return self._gpm_dict["band"]

    def set_band(self, band_name: str) -> bool:
        """
        Set the band name.

        :param band_name: Mandatory. Tested against the list of valid feed names.
        :returns: True if a valid band_name is provided, False if not.
        """
        if band_name in self.BAND_LIST:
            self._gpm_dict.update({"band": band_name})
            return True
        return False

    def set_attr(self, **kwargs: str) -> bool:
        """
        Set named Attr values in the structure.

        The Attrs are not used by the DS controller.

        :keyword obs_date_times: UTC of pointing observation.
        :keyword eb_ids: IDs of execution block in ODA.
        :keyword analysis_date_time: UTC of parameter fit analysis.
        :keyword analysis_script: Script of parameter fit analysis.
        :keyword comment: Operator comment.
        :returns: True if valid attr keywords are provided, False if not.
        """
        valid_keywords = True
        for key, val in kwargs.items():
            if key in self.ATTRS_DEF_DICT:
                self._gpm_dict["attrs"][key] = val
            else:
                valid_keywords = False
        return valid_keywords

    def write_gpm_json(
        self, file_path: Path | None = None, overwrite: bool = False
    ) -> bool:
        """
        Export the global pointing model JSON object to a file.

        The file will have the antenna and band identification encoded into its name.
        Validate against the schema prior to writing.

        :param file_path: Optional path and name of JSON file to write.
        :param overwrite: Whether to overwrite an existing file. Default is False.
        :returns: True if successful, False if not.
        """
        try:
            validate(self._gpm_dict, self._schema)
        except ValidationError as e:
            logger.error(
                f"Current pointing model does not match the JSON schema: {e.message}",
            )
            return False
        if (
            self._gpm_dict["antenna"][0:3] in ["SKA", "MKT"]
            and self._gpm_dict["band"] in self.BAND_LIST
        ):
            file_name = (
                f"gpm-{self._gpm_dict['antenna']}-{self._gpm_dict['band']}.json"
                if file_path is None
                else file_path
            )
            try:
                with open(
                    file_name, "w" if overwrite else "x", encoding="utf-8"
                ) as file:
                    try:
                        json.dump(self._gpm_dict, file, indent=2)
                        return True
                    except (
                        TypeError,
                        OverflowError,
                        RecursionError,
                        UnicodeEncodeError,
                        OSError,
                    ) as e:
                        logger.error(f"Error while dumping JSON file: {e}")
            except (FileExistsError, PermissionError, OSError) as e:
                logger.error(
                    f"Caught exception trying to write file '{file_name}': {e}"
                )
        return False

    def read_gpm_json(self, file_path: Path) -> bool:
        """
        Import a global pointing model JSON object from a file.

        :param file_path: Path of JSON file containing parameters. Must conform to
            the schema.
        :returns: True if successful read and schema validation, False if not.
        """
        self._gpm_dict = utils.load_json_file(file_path)
        if self._gpm_dict is not None:
            try:
                validate(self._gpm_dict, self._schema)
                return True
            except ValidationError as e:
                logger.error(
                    f"'{file_path}' does not match the JSON schema: {e.message}"
                )
        return False
