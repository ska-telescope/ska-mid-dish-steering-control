"""Unit tests of the StaticPointingModel class."""
import json
from math import isnan
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
from jsonschema.exceptions import ValidationError

from ska_mid_dish_steering_control.sculib import SPM_SCHEMA_PATH
from ska_mid_dish_steering_control.static_pointing import StaticPointingModel

# mypy: ignore-errors
# pylint: disable=protected-access


@pytest.fixture(name="sp_model")
def static_pointing_model() -> StaticPointingModel:
    """
    Fixture for creating an instance of StaticPointingModel with a valid schema path.

    :return: Instance of StaticPointingModel.
    """
    spm = StaticPointingModel(SPM_SCHEMA_PATH)
    spm.read_gpm_json(Path("tests/resources/gpm-SKA063-Band_2.json"))
    return spm


def test_create_json_schema() -> None:
    """Test the 'create_json_schema' method."""
    sp_model = StaticPointingModel()
    assert sp_model._schema is not None
    with patch("builtins.open", mock_open()) as mock_file, patch(
        "json.dump"
    ) as mock_dump:
        sp_model.create_json_schema(Path("test.json"))
        mock_file.assert_called_once_with(Path("test.json"), "w", encoding="utf-8")
        mock_dump.assert_called_once()


def test_coefficients_property(sp_model: StaticPointingModel) -> None:
    """
    Test the 'coefficients' property.

    :param model: Instance of StaticPointingModel.
    """
    assert isinstance(sp_model.coefficients, list)
    assert len(sp_model.coefficients) == len(sp_model._DSC_COEFFICIENTS_DICT)


def test_get_all_coefficient_values(sp_model: StaticPointingModel) -> None:
    """
    Test the 'get_all_coefficient_values' method.

    :param model: Instance of StaticPointingModel.
    """
    sp_model._gpm_dict["coefficients"]["IA"]["value"] = 10.5
    values = sp_model.get_all_coefficient_values()
    assert len(values) == len(sp_model._DSC_COEFFICIENTS_DICT)
    assert values[0] == 10.5  # Checks that the first coefficient value is set


def test_get_coefficient_value_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'get_coefficient_value' method with valid coefficient name.

    :param model: Instance of StaticPointingModel.
    """
    sp_model._gpm_dict["coefficients"]["IA"]["value"] = 10.5
    assert sp_model.get_coefficient_value("IA") == 10.5


def test_get_coefficient_value_invalid(sp_model: StaticPointingModel) -> None:
    """
    Test 'get_coefficient_value' method with an invalid coefficient name.

    :param model: Instance of StaticPointingModel.
    """
    assert isnan(sp_model.get_coefficient_value("INVALID"))


def test_set_coefficient_value_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_coefficient' method with valid values.

    :param model: Instance of StaticPointingModel.
    """
    sp_model.set_coefficient("IA", value=10.5, stderr=1.0, units="arcsec")
    assert sp_model._gpm_dict["coefficients"]["IA"]["value"] == 10.5
    assert sp_model._gpm_dict["coefficients"]["IA"]["stderr"] == 1.0
    assert sp_model._gpm_dict["coefficients"]["IA"]["units"] == "arcsec"


def test_set_coefficient_value_invalid_range(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_coefficient' method with a value out of range.

    :param model: Instance of StaticPointingModel.
    """
    sp_model.set_coefficient("IA", value=3000)
    assert (
        sp_model._gpm_dict["coefficients"]["IA"].get("value") != 3000
    )  # Should not be set


def test_set_rms_value_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_rms' method with valid values.

    :param model: Instance of StaticPointingModel.
    """
    sp_model._gpm_dict["rms_fits"] = {"xel_rms": {}, "el_rms": {}, "sky_rms": {}}
    sp_model.set_rms("xel_rms", value=2.5)
    assert sp_model._gpm_dict["rms_fits"]["xel_rms"]["value"] == 2.5


def test_set_antenna_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_antenna' method with valid antenna name.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_antenna("SKA001") is True


def test_set_antenna_invalid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_antenna' method with an invalid antenna name.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_antenna("INVALID") is False


def test_get_band(sp_model: StaticPointingModel) -> None:
    """
    Test 'get_band' method.

    :param model: Instance of StaticPointingModel.
    """
    sp_model._gpm_dict["band"] = "Band_1"
    assert sp_model.get_band() == "Band_1"


def test_set_band_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_band' method with valid band name.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_band("Band_1") is True
    assert sp_model._gpm_dict["band"] == "Band_1"


def test_set_band_invalid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_band' method with invalid band name.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_band("Invalid_Band") is False


def test_set_attr_valid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_attr' method with valid attributes.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_attr(obs_date_times="2023-09-18T12:34:56Z") is True
    assert sp_model._gpm_dict["attrs"]["obs_date_times"] == "2023-09-18T12:34:56Z"


def test_set_attr_invalid(sp_model: StaticPointingModel) -> None:
    """
    Test 'set_attr' method with invalid attributes.

    :param model: Instance of StaticPointingModel.
    """
    assert sp_model.set_attr(invalid="INVALID") is False


def test_write_gpm_json_successful(sp_model: StaticPointingModel) -> None:
    """
    Test 'write_gpm_json' method for successful file writing.

    :param model: Instance of StaticPointingModel.
    """
    sp_model._gpm_dict["antenna"] = "SKA001"
    sp_model._gpm_dict["band"] = "Band_1"

    with patch("builtins.open", mock_open()) as mock_file, patch(
        "json.dump"
    ) as mock_dump:
        assert sp_model.write_gpm_json(Path("test.json"), overwrite=True) is True
        mock_file.assert_called_once_with(Path("test.json"), "w", encoding="utf-8")
        mock_dump.assert_called_once()


def test_write_gpm_json_validation_error(
    sp_model: StaticPointingModel, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test 'write_gpm_json' method when validation fails.

    :param model: Instance of StaticPointingModel.
    :param caplog: Captured logs.
    """
    sp_model._gpm_dict["coefficients"]["IA"]["value"] = 3000.0  # Invalid value
    with patch("builtins.open", mock_open()):
        assert sp_model.write_gpm_json(Path("test.json")) is False
        assert (
            "Current pointing model does not match the JSON schema: "
            "3000.0 is greater than the maximum of 2000.0" in caplog.messages
        )


def test_write_gpm_json_dump_error(
    sp_model: StaticPointingModel, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test 'write_gpm_json' method when it encounters a json dump exception.

    :param model: Instance of StaticPointingModel.
    :param caplog: Captured logs.
    """
    with patch("builtins.open", mock_open()), patch(
        "json.dump", side_effect=TypeError("TypeError")
    ) as mock_dump:
        assert sp_model.write_gpm_json(Path("test.json")) is False
        mock_dump.assert_called_once()
        assert "Error while dumping JSON file: TypeError" in caplog.messages


def test_write_gpm_json_file_exists_error(
    sp_model: StaticPointingModel, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test 'write_gpm_json' method when it may not overwrite existing files.

    :param model: Instance of StaticPointingModel.
    :param caplog: Captured logs.
    """
    with patch("builtins.open", side_effect=FileExistsError("FileExistsError")):
        assert sp_model.write_gpm_json(Path("test.json")) is False
        assert (
            "Caught exception trying to write file 'test.json': FileExistsError"
            in caplog.messages
        )


def test_read_gpm_json_successful(sp_model: StaticPointingModel) -> None:
    """
    Test 'read_gpm_json' method for successful file reading.

    :param model: Instance of StaticPointingModel.
    """
    assert (
        sp_model.read_gpm_json(Path("tests/resources/gpm-SKA063-Band_2.json")) is True
    )


def test_read_gpm_json_validation_error(
    sp_model: StaticPointingModel, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test 'read_gpm_json' method when validation fails.

    :param model: Instance of StaticPointingModel.
    :param caplog: Captured logs.
    """
    mock_json = '{"key": "value"}'

    with patch("builtins.open", mock_open(read_data=mock_json)), patch(
        "json.load", return_value=json.loads(mock_json)
    ), patch("jsonschema.validate", side_effect=ValidationError("ValidationError")):
        assert sp_model.read_gpm_json(Path("test.json")) is False
        assert (
            "'test.json' does not match the JSON schema: "
            "'interface' is a required property" in caplog.messages
        )
