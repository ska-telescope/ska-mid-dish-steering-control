"""Unittests for Track Table module."""

import pytest

from ska_mid_dish_steering_control.track_table import TrackTable

@pytest.fixture(name="track_table")
def track_table_fixture() -> TrackTable:
    """Fixture for creating an instance of TrackTable."""
    return TrackTable()

@pytest.mark.parametrize("csv_filename",["tests/resources/track_table_simple.csv",
                                       "tests/resources/v0.1EL88_AZ0_extra_columns.csv",
                                       "tests/resources/track_table_tabs.csv",
                                       "tests/resources/track_table_unix_quoted.csv",
                                       "tests/resources/track_table_excel.csv",])
def test_track_table_load_csv_file(track_table: TrackTable, csv_filename) -> None:
    """Test the 'store_from_csv' method with classic Unix CSV dialect."""
    track_table.store_from_csv(csv_filename)
    assert len(track_table.tai) == 1201
    assert len(track_table.azi) == 1201
    assert len(track_table.ele) == 1201

    # assert that the track_table properties are all a list of floats
    assert all(isinstance(x, float) for x in track_table.tai)
    assert all(isinstance(x, float) for x in track_table.azi)
    assert all(isinstance(x, float) for x in track_table.ele)
