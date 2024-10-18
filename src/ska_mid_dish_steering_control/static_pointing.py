#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created on Sun Jun 23 20:43:47 2024.
# Original author: Justin Jonas @skao.int
"""Module for import/export of a global static pointing model from/to a JSON file."""

import json
import re

from jsonschema import validate


# mypy: ignore-errors
# TODO pylint: disable=consider-using-f-string, broad-exception-caught
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

    # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
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
        """
        self.interface_prefix = "https://schema.skao.int/ska-mid-dish-gpm/"
        self.version = "1.2"
        self.interface_pattern = self.interface_prefix + "[0-9]{1,2}\\.[0-9]{1,2}"
        self.dict = {
            "interface": self.interface_prefix + self.version,
            "antenna": None,
            "band": None,
            "attrs": {},
            "coefficients": {},
            "rms_fits": {},
        }
        self.antenna_re_pattern = "(^SKA[0-9]{3}$)|(^MKT[0-9]{3}$)"
        self.band_list = [
            "Optical",
            "Band_1",
            "Band_2",
            "Band_3",
            "Band_4",
            "Band_5a",
            "Band_5b",
            "Band_6",
        ]
        self.attrs_def_dict = {
            "obs_date_times": [],
            "eb_ids": [],
            "analysis_script": "",
            "analysis_date_time": "",
            "comment": "",
        }
        self.eb_id_re_pattern = "^eb-[a-z0-9]+-2[0-9]{3}[01][0-9][0-3][0-9]-[a-z0-9]+$"
        self.iso_utc_re_pattern = (
            "^2[0-9]{3}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$"
        )
        self.dsc_coefficients_dict = {
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
        self.rms_list = ["xel_rms", "el_rms", "sky_rms"]
        self.coef_def_dict = {
            "value": 0.0,
            "units": None,
            "stderr": None,
            "used": False,
        }
        self.min_coef = -2000.0
        self.max_coef = +2000.0
        self.rms_def_dict = {"value": None, "units": "arcsec"}
        self.schema: dict[str, str | dict] = {}
        self.dict["attrs"] = self.attrs_def_dict
        self.set_attr(JSON_version=self.version)
        for coef in self.dsc_coefficients_dict:
            self.dict["coefficients"].update({coef: {}})
            for attr, value in self.coef_def_dict.items():
                self.dict["coefficients"][coef][attr] = value
        for rms in self.rms_list:
            self.dict["rms_fits"].update({rms: {}})
            for attr in self.rms_def_dict:
                self.dict["rms_fits"][rms][attr] = self.coef_def_dict[attr]

    # TODO: Refactor
    # pylint: disable=too-many-nested-blocks, too-many-branches, too-many-statements
    def create_json_schema(self, filename: str = None) -> None:
        """
        Create a JSON schema for validating the global pointing model coefficient file.

        :param filename: filename
        """
        self.schema = {
            "$schema": "http://json-schema.org/draft-07/schema",
            "title": "SKA-Mid global pointing model coefficients",
            "description": "Pointing coefficients and metadata for antenna/band pairs",
            "type": "object",
            "properties": {},
        }
        __obj0_ = self.schema["properties"]
        for __k_ in self.dict.keys():
            __obj0_.update({__k_: {}})
            __obj1_ = __obj0_[__k_]
            if isinstance(self.dict[__k_], str):
                __obj1_.update({"type": "string"})
                if __k_ == "interface":
                    __obj1_.update(
                        {
                            "$id": "#/properties/interface",
                            "description": "The URL reference to the global pointing"
                            "model schema",
                            "pattern": self.interface_pattern,
                        }
                    )
                elif __k_ == "band":
                    __obj1_.update({"enum": self.band_list})
                elif __k_ == "antenna":
                    __obj1_.update({"pattern": self.antenna_re_pattern})
            elif isinstance(self.dict[__k_], dict):
                __obj1_.update({"type": "object"})
                __obj1_.update({"properties": {}})
                __obj2_ = __obj1_["properties"]
                if __k_ == "attrs":
                    for __kk_ in self.dict[__k_]:
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        if isinstance(self.dict[__k_][__kk_], list):
                            __obj3_.update({"type": "array", "items": {}})
                            if "date_time" in __kk_:
                                __obj3_["items"].update(
                                    {"pattern": self.iso_utc_re_pattern}
                                )
                            elif "eb_id" in __kk_:
                                __obj3_["items"].update(
                                    {"pattern": self.eb_id_re_pattern}
                                )
                        elif isinstance(self.dict[__k_][__kk_], str):
                            __obj3_.update({"type": "string"})
                            if "date_time" in __kk_:
                                __obj3_.update({"pattern": self.iso_utc_re_pattern})
                elif __k_ == "coefficients":
                    for __kk_, value in self.dsc_coefficients_dict.items():
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        __obj3_.update({"type": "object"})
                        __obj3_.update({"properties": {}})
                        __obj4_ = __obj3_["properties"]
                        for __kkk_ in self.coef_def_dict:
                            __obj4_.update({__kkk_: {}})
                            __obj5_ = __obj4_[__kkk_]
                            if __kkk_ == "value":
                                __obj5_.update({"type": "number"})
                                __obj5_.update({"minimum": self.min_coef})
                                __obj5_.update({"maximum": self.max_coef})
                            elif __kkk_ == "units":
                                __obj5_.update({"enum": [None, value]})
                            elif __kkk_ == "stderr":
                                __obj5_.update({"type": ["number", "null"]})
                            elif __kkk_ == "used":
                                __obj5_.update({"type": "boolean"})
                        __obj3_.update({"required": ["value"]})
                    __obj1_.update(
                        {"required": list(self.dsc_coefficients_dict.keys())}
                    )
                elif __k_ == "rms_fits":
                    for __kk_ in self.rms_list:
                        __obj2_.update({__kk_: {}})
                        __obj3_ = __obj2_[__kk_]
                        __obj3_.update({"type": "object"})
                        __obj3_.update({"properties": {}})
                        __obj4_ = __obj3_["properties"]
                        for __kkk_ in self.rms_def_dict:
                            __obj4_.update({__kkk_: {}})
                            __obj5_ = __obj4_[__kkk_]
                            if __kkk_ == "value":
                                __obj5_.update({"type": ["number", "null"]})
                            if __kkk_ == "units":
                                __obj5_.update({"type": "string"})
                                __obj5_.update({"enum": ["arcsec"]})
        self.schema.update(
            {"required": ["interface", "antenna", "band", "coefficients"]}
        )
        if filename is not None:
            with open(filename, "w", encoding="utf-8") as file:
                json.dump(self.schema, file, indent=2)

    def get_coeff_value(self, coeff_name: str) -> float:
        """
        Return the named pointing coefficient from the structure.

        :param coeff_name: Must be a name in the coefficient list.
        :returns:
            - The actual value if present.
            - 0.0 if not present.
            - NaN if invalid coefficient name.
        """
        if coeff_name in self.dsc_coefficients_dict:
            if coeff_name in self.dict["coefficients"]:
                return self.dict["coefficients"][coeff_name]["value"]
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
        if coeff_name in self.dsc_coefficients_dict:
            for key, val in kwargs.items():
                if key in self.coef_def_dict:
                    if key == "value":
                        if self.min_coef <= val <= self.max_coef:
                            self.dict["coefficients"][coeff_name][
                                key
                            ] = self._check_float(val)
                    elif key == "stderr":
                        self.dict["coefficients"][coeff_name][key] = self._check_float(
                            val
                        )
                    else:
                        self.dict["coefficients"][coeff_name][key] = val

    def set_rms(self, rms_name: str, **kwargs: float | str) -> None:
        """
        Set the RMS errors associated with the fit in the structure.

        Not used by the DS controller.

        :param rms_name: Must be a name in the RMS list.
        :keyword value: RMS value.
        :keyword units: Units of value.
        """
        if rms_name in self.rms_list:
            for key, val in kwargs.items():
                if key in self.rms_def_dict:
                    self.dict["rms_fits"][rms_name][key] = self._check_float(val)

    def set_antenna(self, ant_name: str) -> bool:
        """
        Set the antenna name in the structure.

        :param ant_name: Mandatory. No check is done on the name provided.
        :returns: True if ant_name has correct format, else False.
        """
        if re.search(self.antenna_re_pattern, ant_name) is None:
            return False
        self.dict.update({"antenna": ant_name})
        return True

    def set_band(self, band_name: str) -> bool:
        """
        Set the band name.

        :param band_name: Mandatory. Tested against the list of valid feed names.
        :returns: True if a valid band_name is provided, False if not.
        """
        if band_name in self.band_list:
            self.dict.update({"band": band_name})
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
            if key in self.attrs_def_dict:
                self.dict["attrs"][key] = val
            return False
        return True

    def write_gpm_json(self) -> bool:
        """
        Export the global pointing model JSON object to a file.

        The file will have the antenna and band identification encoded into its name.
        Validate against the schema prior to writing.

        :returns: True if successful, False if not.
        """
        self.create_json_schema()
        try:
            validate(self.dict, self.schema)
        except Exception:
            return False
        try:
            if (
                self.dict["antenna"][0:3] == "SKA" or self.dict["antenna"][0:3] == "MKT"
            ) and self.dict["band"] in self.band_list:
                file_name = "gpm-%s-%s.json" % (
                    self.dict["antenna"],
                    self.dict["band"],
                )
                with open(file_name, "w", encoding="utf-8") as file:
                    json.dump(self.dict, file, indent=2)
                return True
            return False
        except Exception:
            return False

    def read_gpm_json(self, filename: str) -> bool:
        """
        Import a global pointing model JSON object from a file.

        :param filename: name of JSON file containing parameters. Must conform to
            the schema.
        :returns: True if successful read and schema validation, False if not.
        """
        with open(filename, encoding="utf-8") as file:
            self.dict = json.load(file)
        self.create_json_schema()
        try:
            validate(self.dict, self.schema)
            return True
        except Exception:
            return False

    def _check_float(self, val):
        try:
            return float(val)
        except Exception:
            return val
