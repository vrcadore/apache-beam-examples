import logging

import apache_beam as beam
import click
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.options.pipeline_options import PipelineOptions


def get_mean_delay_at_top_airports(airline_df):
    arr = airline_df.rename(columns={"arrival_airport": "airport"})
    arr = arr.airport.value_counts()
    dep = airline_df.rename(columns={"departure_airport": "airport"})
    dep = dep.airport.value_counts()
    total = arr + dep
    # Note we keep all to include duplicates - this ensures the result is
    # deterministic.
    # NaNs can be included in the output in pandas 1.4.0 and above, so we
    # explicitly drop them.
    top_airports = total.nlargest(10, keep="all").dropna()
    at_top_airports = airline_df["arrival_airport"].isin(top_airports.index.values)
    return airline_df[at_top_airports].mean()


def input_date(date):
    from datetime import datetime
    parsed = datetime.strptime(date, "%Y-%m-%d")
    if parsed > datetime(2012, 12, 31) or parsed < datetime(2002, 1, 1):
        raise ValueError("There's only data from 2002-01-01 to 2012-12-31")
    return date


def run_flight_delay_pipeline(pipeline, start_date=None, end_date=None, output=None):
    query = f"""
        SELECT
            FlightDate AS date,
            IATA_CODE_Reporting_Airline AS airline,
            Origin AS departure_airport,
            Dest AS arrival_airport,
            DepDelay AS departure_delay,
            ArrDelay AS arrival_delay
        FROM `apache-beam-testing.airline_ontime_data.flights`
        WHERE
            FlightDate >= '{start_date}' AND FlightDate <= '{end_date}' AND
            DepDelay IS NOT NULL AND ArrDelay IS NOT NULL"""

    # Import this here to avoid pickling the main session.
    import time

    from apache_beam.transforms.window import FixedWindows, TimestampedValue

    def to_unixtime(s):
        return time.mktime(s.timetuple())

    # The pipeline will be run on exiting the with block.
    with pipeline as p:
        tbl = (
            p
            | "Read Table"
            >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | "Assign Timestamp"
            >> beam.Map(lambda x: TimestampedValue(x, to_unixtime(x["date"])))
            # Use beam.Select to make sure data has a schema
            # The casts in lambdas ensure data types are properly inferred
            | "Set Schema"
            >> beam.Select(
                date=lambda x: str(x["date"]),
                airline=lambda x: str(x["airline"]),
                departure_airport=lambda x: str(x["departure_airport"]),
                arrival_airport=lambda x: str(x["arrival_airport"]),
                departure_delay=lambda x: float(x["departure_delay"]),
                arrival_delay=lambda x: float(x["arrival_delay"]),
            )
        )

        daily = tbl | "Saily Windows" >> beam.WindowInto(FixedWindows(60 * 60 * 24))

        # group the flights data by carrier
        df = to_dataframe(daily)
        result = df.groupby("airline").apply(get_mean_delay_at_top_airports)
        result.to_csv(output)


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option(
    "--start_date",
    type=input_date,
    default="2012-12-22",
    help="YYYY-MM-DD lower bound (inclusive) for input dataset.",
)
@click.option(
    "--end_date",
    type=input_date,
    default="2012-12-26",
    help="YYYY-MM-DD upper bound (inclusive) for input dataset.",
)
@click.option("--output", required=True, help="Location to write the output.")
@click.pass_context
def run(ctx: click.Context, start_date: str, end_date: str, output: str) -> None:
    """Main entry point; defines and runs the flight delay pipeline."""

    pipeline_options = PipelineOptions(ctx.args)

    with beam.Pipeline(options=pipeline_options) as p:
        run_flight_delay_pipeline(
            pipeline=p,
            start_date=start_date,
            end_date=end_date,
            output=output,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
