import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.data.normalize import to_train_events


def test_column_map_handles_new_names(tmp_path):
    df = pd.DataFrame(
        {
            "Train No ": [123],
            "ARRIVAL time": ["2024-01-01 10:00"],
            "Departure Time": ["2024-01-01 10:05"],
            "Station Code": ["Alpha"],
        }
    )

    result = to_train_events(df, station_map_path=tmp_path / "station_map.csv")

    assert {
        "train_id",
        "sched_arr",
        "sched_dep",
        "station_id",
    }.issubset(result.columns)

    assert result.loc[0, "train_id"] == 123
    assert pd.notna(result.loc[0, "sched_arr"])
    assert pd.notna(result.loc[0, "sched_dep"])
    assert pd.notna(result.loc[0, "station_id"])


def test_actual_times_mapped_and_delays(tmp_path):
    df = pd.DataFrame(
        {
            "Train No": [456],
            "Arrival time": ["2024-01-01 10:00"],
            "Departure Time": ["2024-01-01 10:05"],
            "Actual Arrival": ["2024-01-01 10:02"],
            "Actual Departure": ["2024-01-01 10:07"],
            "Station Code": ["Beta"],
        }
    )

    result = to_train_events(df, station_map_path=tmp_path / "station_map.csv")

    assert pd.notna(result.loc[0, "act_arr"])
    assert pd.notna(result.loc[0, "act_dep"])
    assert result.loc[0, "arr_delay_min"] == 2
    assert result.loc[0, "dep_delay_min"] == 2
