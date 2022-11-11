import glob
import logging
import re
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import pytest
from apache_beam.testing.util import open_shards
from click.testing import CliRunner

from src.pipelines.dataframe_taxi_example import pipeline


class TaxiRideExampleTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.input_path = str(Path(self.tmpdir.name) / "rides*.csv")
        self.lookup_path = str(Path(self.tmpdir.name) / "lookup.csv")
        self.output_path = str(Path(self.tmpdir.name) / "output.csv")

        folder = Path(__file__).parent
        with open(folder / "sample_rides.csv", "r") as fp:
            sample_rides_text = fp.read()

        with open(folder / "sample_zone.csv", "r") as fp:
            sample_zone_lookup = fp.read()

        # Duplicate sample data in 100 different files to replicate multi-file read
        for i in range(100):
            with open(Path(self.tmpdir.name) / f"rides{i}.csv", "w") as fp:
                fp.write(sample_rides_text)

        with open(self.lookup_path, "w") as fp:
            fp.write(sample_zone_lookup)

    def tearDown(self):
        self.tmpdir.cleanup()

    @pytest.mark.pipelines
    def test_pipeline(self):
        # Assign
        rides = pd.concat(pd.read_csv(path) for path in glob.glob(self.input_path))
        zones = pd.read_csv(self.lookup_path)
        rides = rides.merge(
            zones.set_index("LocationID").Borough,
            right_index=True,
            left_on="DOLocationID",
            how="left",
        )
        expected_counts = rides.groupby("Borough").passenger_count.sum()

        # Act
        runner = CliRunner()
        result = runner.invoke(
            pipeline.run,
            [
                f"--input={self.input_path}",
                f"--zone_lookup={self.lookup_path}",
                f"--output={self.output_path}",
            ],
        )

        # Assert
        assert result.exit_code == 0

        results = []
        with open_shards(f"{self.output_path}-*") as result_file:
            for line in result_file:
                match = re.search(r"(\S+),([0-9\.]+)", line)
                if match is not None:
                    results.append((match.group(1), int(float(match.group(2)))))
                elif line.strip():
                    self.assertEqual(line.strip(), "Borough,passenger_count")
        self.assertEqual(sorted(results), sorted(expected_counts.items()))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
