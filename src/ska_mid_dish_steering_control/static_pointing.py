#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Original author: Justin Jonas @skao.int
"""Module for import/export of a global static pointing model from/to a JSON file."""

import json
import logging
import re
from pathlib import Path
from typing import Any

from jsonschema import ValidationError, validate

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

    _INTERFACE_PREFIX = "https://schema.skao.int/ska-mid-dish-gpm/"
    _VERSION = "1.2"
    _INTERFACE_PATTERN = _INTERFACE_PREFIX + "[0-9]{1,2}\\.[0-9]{1,2}"
    _ANTENNA_RE_PATTERN = "(^SKA[0-9]{3}$)|(^MKT[0-9]{3}$)"
    _BAND_LIST = [
        "Optical",
        "Band_1",
        "Band_2",
        "Band_3",
        "Band_4",
        "Band_5a",
        "Band_5b",
        "Band_6",
    ]
    _ATTRS_DEF_DICT = {
        "obs_date_times": [],
        "eb_ids": [],
        "analysis_script": "",
        "analysis_date_time": "",
        "comment": "",
    }
    _EB_ID_RE_PATTERN = "^eb-[a-z0-9]+-2[0-9]{3}[01][0-9][0-3][0-9]-[a-z0-9]+$"
    _ISO_UTC_RE_PATTERN = (
        "^2[0-9]{3}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$"
    )
    _DSC_COEFFICIENTS_DICT = {
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
    _RMS_LIST = ["xel_rms", "el_rms", "sky_rms"]
    _COEF_DEF_DICT = {
        "value": 0.0,
        "units": None,
        "stderr": None,
        "used": False,
    }
    _MIN_COEF = -2000.0
    _MAX_COEF = +2000.0
    _RMS_DEF_DICT = {"value": None, "units": "arcsec"}

    def __init__(self, schema_file_path: Path | None = None) -> None:
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

        :param schema_file_path: Optional Path to an existing JSON schema. If not
            provided, the class with generate a default schema.
        """
        # The global pointing model dict structure is built with default values
        self._gpm_dict = {
            "interface": self._INTERFACE_PREFIX + self._VERSION,
            "antenna": None,
            "band": None,
            "attrs": self._ATTRS_DEF_DICT,
            "coefficients": {},
            "rms_fits": {},
        }
        self.set_attr(JSON_version=self._VERSION)
        for coef in self._DSC_COEFFICIENTS_DICT:
            self._gpm_dict["coefficients"].update({coef: {}})
            for attr, value in self._COEF_DEF_DICT.items():
                self._gpm_dict["coefficients"][coef][attr] = value
        for rms in self._RMS_LIST:
            self._gpm_dict["rms_fits"].update({rms: {}})
            for attr in self._RMS_DEF_DICT:
                self._gpm_dict["rms_fits"][rms][attr] = self._COEF_DEF_DICT[attr]
        # Create schema
        self._schema: dict[str, str | dict] | None = None
        if schema_file_path is not None:
            self._schema = self._load_json_file(schema_file_path)
        if self._schema is None:
            self._schema = self.create_json_schema()

    # TODO: Refactor
    # pylint: disable=too-many-nested-blocks, too-many-branches, too-many-statements
    def create_json_schema(self, filename: str | None = None) -> dict:
        """
        Create a JSON schema for validating the global pointing model coefficient file.

        :param filename: Name of file to write schema to.
        """
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema",
            "title": "SKA-Mid global pointing model coefficients",
            "description": "Pointing coefficients and metadata for antenna/band pairs",
            "type": "object",
            "properties": {},
        }
        __obj0_ = schema["properties"]
        for __k_ in self._gpm_dict.keys():
            __obj0_.update({__k_: {}})
            __obj1_ = __obj0_[__k_]
            if isinstance(self._gpm_dict[__k_], str):
                __obj1_.update({"type": "string"})
                if __k_ == "interface":
                    __obj1_.update(
                        {
                            "$id": "#/properties/interface",
                            "description": "The URL reference to the global pointing "
                            "model schema",
                            "pattern": self._INTERFACE_PATTERN,
                        }
                    )
                elif __k_ == "band":
                    __obj1_.update({"enum": self._BAND_LIST})
                elif __k_ == "antenna":
                    __obj1_.update({"pattern": self._ANTENNA_RE_PATTERN})
            elif isinstance(self._gpm_dict[__k_], dict):
                __obj1_.update({"type": "object"})
                __obj1_.update({"properties": {}})
                __obj2_ = __obj1_["properties"]
                if __k_ == "attrs":
                    for __kk_ in self._gpm_dict[__k_]:
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        if isinstance(self._gpm_dict[__k_][__kk_], list):
                            __obj3_.update({"type": "array", "items": {}})
                            if "date_time" in __kk_:
                                __obj3_["items"].update(
                                    {"pattern": self._ISO_UTC_RE_PATTERN}
                                )
                            elif "eb_id" in __kk_:
                                __obj3_["items"].update(
                                    {"pattern": self._EB_ID_RE_PATTERN}
                                )
                        elif isinstance(self._gpm_dict[__k_][__kk_], str):
                            __obj3_.update({"type": "string"})
                            if "date_time" in __kk_:
                                __obj3_.update({"pattern": self._ISO_UTC_RE_PATTERN})
                elif __k_ == "coefficients":
                    for __kk_, value in self._DSC_COEFFICIENTS_DICT.items():
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        __obj3_.update({"type": "object"})
                        __obj3_.update({"properties": {}})
                        __obj4_ = __obj3_["properties"]
                        for __kkk_ in self._COEF_DEF_DICT:
                            __obj4_.update({__kkk_: {}})
                            __obj5_ = __obj4_[__kkk_]
                            if __kkk_ == "value":
                                __obj5_.update({"type": "number"})
                                __obj5_.update({"minimum": self._MIN_COEF})
                                __obj5_.update({"maximum": self._MAX_COEF})
                            elif __kkk_ == "units":
                                __obj5_.update({"enum": [None, value]})
                            elif __kkk_ == "stderr":
                                __obj5_.update({"type": ["number", "null"]})
                            elif __kkk_ == "used":
                                __obj5_.update({"type": "boolean"})
                        __obj3_.update({"required": ["value"]})
                    __obj1_.update(
                        {"required": list(self._DSC_COEFFICIENTS_DICT.keys())}
                    )
                elif __k_ == "rms_fits":
                    for __kk_ in self._RMS_LIST:
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        __obj3_.update({"type": "object"})
                        __obj3_.update({"properties": {}})
                        __obj4_ = __obj3_["properties"]
                        for __kkk_ in self._RMS_DEF_DICT:
                            __obj4_.update({__kkk_: {}})
                            __obj5_ = __obj4_[__kkk_]
                            if __kkk_ == "value":
                                __obj5_.update({"type": ["number", "null"]})
                            if __kkk_ == "units":
                                __obj5_.update({"type": "string"})
                                __obj5_.update({"enum": ["arcsec"]})
        schema.update({"required": ["interface", "antenna", "band", "coefficients"]})
        if filename is not None:
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(schema, file, indent=2)
        return schema

    def get_coeff_value(self, coeff_name: str) -> float:
        """
        Return the named pointing coefficient from the structure.

        :param coeff_name: Must be a name in the coefficient list.
        :returns:
            - The actual value if present.
            - 0.0 if not present.
            - NaN if invalid coefficient name.
        """
        if coeff_name in self._DSC_COEFFICIENTS_DICT:
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
        if coeff_name in self._DSC_COEFFICIENTS_DICT:
            for key, val in kwargs.items():
                if key in self._COEF_DEF_DICT:
                    if key == "value":
                        if self._MIN_COEF <= val <= self._MAX_COEF:
                            self._gpm_dict["coefficients"][coeff_name][
                                key
                            ] = self._check_float(val)
                    elif key == "stderr":
                        self._gpm_dict["coefficients"][coeff_name][
                            key
                        ] = self._check_float(val)
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
        if rms_name in self._RMS_LIST:
            for key, val in kwargs.items():
                if key in self._RMS_DEF_DICT:
                    self._gpm_dict["rms_fits"][rms_name][key] = self._check_float(val)

    def set_antenna(self, ant_name: str) -> bool:
        """
        Set the antenna name in the structure.

        :param ant_name: Mandatory. No check is done on the name provided.
        :returns: True if ant_name has correct format, else False.
        """
        if re.search(self._ANTENNA_RE_PATTERN, ant_name) is None:
            return False
        self._gpm_dict.update({"antenna": ant_name})
        return True

    def set_band(self, band_name: str) -> bool:
        """
        Set the band name.

        :param band_name: Mandatory. Tested against the list of valid feed names.
        :returns: True if a valid band_name is provided, False if not.
        """
        if band_name in self._BAND_LIST:
            self._gpm_dict.update({"band": band_name})
            return True
        return False

    def set_attr(self, **kwargs: str) -> bool:
        """
        Set named Attr values in the structure.

        The Attrs are not used by the DS controller.

        :keyword Obs_date_time: UTC of pointing observation.
        :keyword eb_id: ID of execution block in ODA.
        :keyword Analysis_date_time: UTC of parameter fit analysis.
        :keyword Version: Version of coefficient fitting code.
        :keyword Comment: Operator comment.
        :returns: True if valid attr keywords are provided, False if not.
        """
        for key, val in kwargs.items():
            if key in self._ATTRS_DEF_DICT:
                self._gpm_dict["attrs"][key] = val
            return False
        return True

    def write_gpm_json(self) -> bool:
        """
        Export the global pointing model JSON object to a file.

        The file will have the antenna and band identification encoded into its name.
        Validate against the schema prior to writing.

        :returns: True if successful, False if not.
        """
        try:
            validate(self._gpm_dict, self._schema)
        except ValidationError as e:
            logger.error(
                "Current pointing model does not match the JSON schema: %s", e.message
            )
            return False
        if (
            self._gpm_dict["antenna"][0:3] in ["SKA", "MKT"]
            and self._gpm_dict["band"] in self._BAND_LIST
        ):
            file_name = f"gpm-{self._gpm_dict['antenna']}-{self._gpm_dict['band']}.json"
            with open(file_name, "w", encoding="utf-8") as file:
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
                    logger.error("Error while dumping JSON file: %s", e)
        return False

    def read_gpm_json(self, file_path: Path) -> bool:
        """
        Import a global pointing model JSON object from a file.

        :param file_path: Path of JSON file containing parameters. Must conform to
            the schema.
        :returns: True if successful read and schema validation, False if not.
        """
        self._gpm_dict = self._load_json_file(file_path)
        if self._gpm_dict is not None:
            try:
                validate(self._gpm_dict, self._schema)
                return True
            except ValidationError as e:
                logger.error(
                    "'%s' does not match the JSON schema: %s", file_path, e.message
                )
        return False

    @staticmethod
    def _check_float(value) -> Any:
        try:
            return float(value)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _load_json_file(file_path: Path) -> dict[str, Any] | None:
        """
        Load JSON file.

        :param file_path: Path of JSON file to load.
        :return: decoded JSON file contents as nested dictionary,
            or None if it failed.
        """
        if file_path.exists():
            with open(file_path, "r", encoding="UTF-8") as file:
                try:
                    return json.load(file)
                except json.JSONDecodeError:
                    logger.error("The file '%s' is not valid JSON.", file_path)
        else:
            logger.warning("The file '%s' does not exist.", file_path)
        return None
