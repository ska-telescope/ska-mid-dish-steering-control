#!/usr/bin/env python3
# Original author: Justin Jonas @skao.int
"""
Script to generate the global static pointing JSON schema used for validation.

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
"""
import json
from pathlib import Path

from ska_mid_dish_steering_control.constants import JSONData
from ska_mid_dish_steering_control.static_pointing import StaticPointingModel

INTERFACE_PATTERN = StaticPointingModel.INTERFACE_PREFIX + "[0-9]{1,2}\\.[0-9]{1,2}"
EB_ID_RE_PATTERN = "^eb-[a-z0-9]+-2[0-9]{3}[01][0-9][0-3][0-9]-[a-z0-9]+$"
ISO_UTC_RE_PATTERN = (
    "^2[0-9]{3}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]Z$"
)


# mypy: ignore-errors
# pylint: disable=too-many-nested-blocks, too-many-branches, too-many-statements
def create_json_schema(file_name: Path) -> None:
    """
    Create a JSON schema for validating the global pointing model coefficient file.

    :param file_name: Path and name of file to write schema to.
    """
    gpm_dict: dict[str, JSONData] = {
        "interface": StaticPointingModel.INTERFACE_PREFIX + StaticPointingModel.VERSION,
        "antenna": "",
        "band": "",
        "coefficients": {},
    }
    for coeff in StaticPointingModel.DSC_COEFFICIENTS_DICT:
        gpm_dict["coefficients"].update({coeff: {}})
    gpm_dict.update({"attrs": StaticPointingModel.ATTRS_DEF_DICT, "rms_fits": {}})
    schema: dict[str, JSONData] = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "title": "SKA-Mid global pointing model coefficients",
        "description": "Pointing coefficients and metadata for antenna/band pairs",
        "type": "object",
        "properties": {},
    }
    obj0 = schema["properties"]
    for key in gpm_dict.keys():
        obj0.update({key: {}})
        obj1 = obj0[key]
        if isinstance(gpm_dict[key], str):
            obj1.update({"type": "string"})
            if key == "interface":
                obj1.update(
                    {
                        "$id": "#/properties/interface",
                        "description": "The URL reference to the global pointing "
                        "model schema",
                        "pattern": INTERFACE_PATTERN,
                    }
                )
            elif key == "band":
                obj1.update({"enum": StaticPointingModel.BAND_LIST})
            elif key == "antenna":
                obj1.update({"pattern": StaticPointingModel.ANTENNA_RE_PATTERN})
        elif isinstance(gpm_dict[key], dict):
            obj1.update({"type": "object"})
            obj1.update({"properties": {}})
            obj2 = obj1["properties"]
            if key == "attrs":
                for key2 in gpm_dict[key]:
                    obj2.update({key2: {}})
                    obj3 = obj2[key2]
                    if isinstance(gpm_dict[key][key2], list):
                        obj3.update({"type": "array", "items": {}})
                        if "date_time" in key2:
                            obj3["items"].update({"pattern": ISO_UTC_RE_PATTERN})
                        elif "eb_id" in key2:
                            obj3["items"].update({"pattern": EB_ID_RE_PATTERN})
                    elif isinstance(gpm_dict[key][key2], str):
                        obj3.update({"type": "string"})
                        if "date_time" in key2:
                            obj3.update({"pattern": ISO_UTC_RE_PATTERN})
            elif key == "coefficients":
                for key2, value in StaticPointingModel.DSC_COEFFICIENTS_DICT.items():
                    obj2.update({key2: {}})
                    obj3 = obj2[key2]
                    obj3.update({"type": "object"})
                    obj3.update({"properties": {}})
                    obj4 = obj3["properties"]
                    for key3 in StaticPointingModel.COEFF_DEF_DICT:
                        obj4.update({key3: {}})
                        obj5 = obj4[key3]
                        if key3 == "value":
                            obj5.update({"type": "number"})
                            obj5.update({"minimum": StaticPointingModel.MIN_COEFF})
                            obj5.update({"maximum": StaticPointingModel.MAX_COEFF})
                        elif key3 == "units":
                            obj5.update({"enum": [None, value]})
                        elif key3 == "stderr":
                            obj5.update({"type": ["number", "null"]})
                        elif key3 == "used":
                            obj5.update({"type": "boolean"})
                    obj3.update({"required": ["value"]})
                obj1.update(
                    {"required": list(StaticPointingModel.DSC_COEFFICIENTS_DICT.keys())}
                )
            elif key == "rms_fits":
                for key2 in StaticPointingModel.RMS_LIST:
                    obj2.update({key2: {}})
                    obj3 = obj2[key2]
                    obj3.update({"type": "object"})
                    obj3.update({"properties": {}})
                    obj4 = obj3["properties"]
                    for key3 in StaticPointingModel.RMS_DEF_DICT:
                        obj4.update({key3: {}})
                        obj5 = obj4[key3]
                        if key3 == "value":
                            obj5.update({"type": ["number", "null"]})
                        if key3 == "units":
                            obj5.update({"type": "string"})
                            obj5.update({"enum": ["arcsec"]})
    schema.update({"required": ["interface", "antenna", "band", "coefficients"]})

    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(schema, file, indent=2)


if __name__ == "__main__":
    create_json_schema(
        Path("src/ska_mid_dish_steering_control/schemas/ska-mid-dish-gpm.json")
    )
