import csv
import logging

import apache_beam as beam
import click
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class ParseFileFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self, headers):
        self.headers = headers

    def process(self, elem):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.

        Args:
            element: the element being processed
        Returns:
            The processed element.
        """
        row = list(csv.reader([elem]))[0]
        dictionary = {key: value for (key, value) in zip(self.headers, row)}
        logging.info(dictionary)
        yield dictionary


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option("--input", required=True, help="File to read in.")
@click.option("--output", required=True, help="BQ output table(dataset.tablename")
@click.pass_context
def run_pipeline(ctx: click.Context, input: str, output: str) -> None:
    """Pipeline for reading data from a Cloud Storage bucket and writing the
    results to BigQuery
    """

    pipeline_options = PipelineOptions(ctx.args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    schema = ",".join(
        [
            "id:INTEGER",
            "first_name:STRING",
            "last_name:STRING",
            "email:STRING",
            "gender:STRING",
        ]
    )

    bq_params = dict(
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )

    with beam.Pipeline(options=pipeline_options) as p:

        user_request_header = ["id", "first_name", "last_name", "email", "gender"]

        input_rows = (
            p
            | "ReadFile" >> ReadFromText(input, skip_header_lines=1)
            | "ParseFile" >> beam.ParDo(ParseFileFn(user_request_header))
        )

        _ = input_rows | "WriteToBigQuery" >> WriteToBigQuery(output, **bq_params)

        result = p.run()
        result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
