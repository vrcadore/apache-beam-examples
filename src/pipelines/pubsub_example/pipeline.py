import json
import logging

import apache_beam as beam
import click
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.sql import SqlTransform
from apache_beam.transforms.window import FixedWindows


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option(
    "--output_topic",
    required=True,
    help=(
        "Cloud PubSub topic to write to (e.g. "
        "projects/my-project/topics/my-topic), must be created prior to "
        "running the pipeline."
    ),
)
@click.pass_context
def run_pipeline(
    ctx: click.Context, output_topic: str, save_main_session: bool = True
) -> None:

    pipeline_options = PipelineOptions(ctx.args, save_main_session=True, streaming=True)

    input_params = dict(
        topic="projects/pubsub-public-data/topics/taxirides-realtime",
        timestamp_attribute="ts",
    )

    sql_transform = """
        SELECT
            ride_status,
            COUNT(*) AS num_rides,
            SUM(passenger_count) AS total_passengers
        FROM PCOLLECTION
        WHERE NOT ride_status = 'enroute'
        GROUP BY ride_status
    """

    # fmt: off
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline
            | "Read Topic" >> ReadFromPubSub(**input_params).with_output_types(bytes)
            | "Parse JSON payload" >> beam.Map(json.loads)
            # Use beam.Row to create a schema-aware PCollection
            | "Create beam Row" >> beam.Map(
                lambda x: beam.Row(
                    ride_status=str(x["ride_status"]),
                    passenger_count=int(x["passenger_count"]),
                )
            )
            # SqlTransform will computes result within an existing window
            | "15s fixed windows" >> beam.WindowInto(FixedWindows(15))
            # Aggregate drop offs and pick ups that occur within each 15s window
            | "Transform Data" >> SqlTransform(sql_transform)
            # SqlTransform yields python objects with attributes corresponding to
            # the outputs of the query.
            # Collect those attributes, as well as window information, into a dict
            | "Assemble Dictionary" >> beam.Map(
                lambda row, window=beam.DoFn.WindowParam: {
                    "ride_status": row.ride_status,
                    "num_rides": row.num_rides,
                    "total_passengers": row.total_passengers,
                    "window_start": window.start.to_rfc3339(),
                    "window_end": window.end.to_rfc3339(),
                }
            )
            | "Convert to JSON" >> beam.Map(json.dumps)
            | "UTF-8 encode" >> beam.Map(lambda s: s.encode("utf-8"))
            | "Write to Topic" >> WriteToPubSub(topic=output_topic)
        )
    # fmt: on


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
