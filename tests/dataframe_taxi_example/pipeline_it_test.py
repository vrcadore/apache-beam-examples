import logging
import os
import unittest
import uuid

import pandas as pd
import pytest
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

from src.pipelines.dataframe_taxi_example import pipeline


class TaxiRideIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp_location = os.environ.get("PIPELINE_TEST_LOCATION")
        self.outdir = f"{self.temp_location}/taxiride_it-{uuid.uuid4()}"
        self.output_path = os.path.join(self.outdir, "output.csv")

    def tearDown(self):
        FileSystems.delete([self.outdir + "/"])

    @pytest.mark.integration
    def test_pipeline(self):

        # Standard workers OOM with the enrich pipeline
        pipeline_options = PipelineOptions()
        pipeline_options.view_as(WorkerOptions).machine_type = "e2-highmem-2"
        pipeline_options.view_as(GoogleCloudOptions).temp_location = self.temp_location

        input_file = "gs://apache-beam-samples/nyc_taxi/2018/*.csv"
        zone_lookup = "gs://apache-beam-samples/nyc_taxi/misc/taxi+_zone_lookup.csv"

        from apache_beam.testing.test_pipeline import TestPipeline

        with TestPipeline(options=pipeline_options, is_integration_test=True) as p:
            pipeline.run_pipeline(
                pipeline=p,
                input=input_file,
                zone_lookup=zone_lookup,
                output=self.output_path,
            )

            # Verify
            expected = pd.read_csv(
                os.path.join(
                    os.path.dirname(__file__), "data", "taxiride_2018_enrich_truth.csv"
                ),
                comment="#",
            )
            expected = expected.sort_values("Borough").reset_index(drop=True)

            def read_csv(path):
                with FileSystems.open(path) as fp:
                    return pd.read_csv(fp)

            result = pd.concat(
                read_csv(metadata.path)
                for metadata in FileSystems.match([f"{self.output_path}*"])[
                    0
                ].metadata_list
            )
            result = result.sort_values("Borough").reset_index(drop=True)

            pd.testing.assert_frame_equal(expected, result)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
