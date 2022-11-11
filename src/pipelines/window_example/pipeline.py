import logging

import apache_beam as beam
import click
from apache_beam.transforms import window

TABLE_SCHEMA = (
    "word:STRING, count:INTEGER, " "window_start:TIMESTAMP, window_end:TIMESTAMP"
)


def find_words(element):
    import re
    return re.findall(r"[A-Za-z\']+", element)


class FormatDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = "%Y-%m-%d %H:%M:%S.%f UTC"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [
            {
                "word": element[0],
                "count": element[1],
                "window_start": window_start,
                "window_end": window_end,
            }
        ]


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option(
    "--input_topic",
    required=True,
    help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".',
)
@click.option(
    "--output_table",
    required=True,
    help=(
        "Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE."
    ),
)
@click.pass_context
def run(
    ctx: click.Context,
    input_topic: str,
    output_table: str,
    save_main_session: bool = True,
) -> None:
    """A streaming word-counting workflow.
    Important: streaming pipeline support in Python Dataflow is in development
    and is not yet available for use.
    """

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(ctx.args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text from PubSub messages.
        lines = p | beam.io.ReadFromPubSub(input_topic)

        # Get the number of appearances of a word.
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        transformed = (
            lines
            | "Split" >> (beam.FlatMap(find_words).with_output_types(str))
            | "PairWithOne" >> beam.Map(lambda x: (x, 1))
            | "Window Agg" >> beam.WindowInto(window.FixedWindows(2 * 60, 0))
            | "Group" >> beam.GroupByKey()
            | "Count" >> beam.Map(count_ones)
            | "Format" >> beam.ParDo(FormatDoFn())
        )

        # Write to BigQuery.
        _ = transformed | "Write" >> beam.io.WriteToBigQuery(
            output_table,
            schema=TABLE_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
