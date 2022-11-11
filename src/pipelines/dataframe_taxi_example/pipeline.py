import logging

import apache_beam as beam
import click
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions


def run_pipeline(
    pipeline: beam.Pipeline, input: str, zone_lookup: str, output: str
) -> None:

    rides = pipeline | "Read taxi rides" >> read_csv(input)
    zones = pipeline | "Read zone lookup" >> read_csv(zone_lookup)

    # Enrich taxi ride data with boroughs from zone lookup table
    # Joins on zones.LocationID and rides.DOLocationID, by first making the
    # former the index for zones.
    rides = rides.merge(
        zones.set_index("LocationID").Borough,
        right_index=True,
        left_on="DOLocationID",
        how="left",
    )

    # Sum passengers dropped off per Borough
    agg = rides.groupby("Borough").passenger_count.sum()
    agg.to_csv(output)

    # A more intuitive alternative to the above merge call, but this option
    # doesn't preserve index, thus requires non-parallel execution.
    # rides = rides.merge(zones[['LocationID','Borough']],
    #                    how="left",
    #                    left_on='DOLocationID',
    #                    right_on='LocationID')


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option(
    "--input",
    default="gs://apache-beam-samples/nyc_taxi/misc/sample.csv",
    help="Input file to process.",
)
@click.option("--output", required=True, help="Output file to write results to.")
@click.option(
    "--zone_lookup",
    default="gs://apache-beam-samples/nyc_taxi/misc/taxi+_zone_lookup.csv",
    help="Location for taxi zone lookup CSV.",
)
@click.pass_context
def run(ctx: click.Context, input: str, zone_lookup: str, output: str) -> None:
    """Enrich taxi ride data with zone lookup table and perform a grouped
    aggregation."""

    pipeline_options = PipelineOptions(ctx.args)

    with beam.Pipeline(options=pipeline_options) as p:
        run_pipeline(p, input=input, zone_lookup=zone_lookup, output=output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
