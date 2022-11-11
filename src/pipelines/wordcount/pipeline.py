import logging
import re
from typing import Iterable

import apache_beam as beam
import click
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element: str) -> Iterable[str]:
        """Returns an iterator over the words of this element.

        The element is a line of text.  If the line is blank, note that, too.

        Args:
            element: the element being processed

        Returns:
            The processed element.
        """
        return re.findall(r"[\w\']+", element, re.UNICODE)


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option(
    "--input",
    default="gs://dataflow-samples/shakespeare/kinglear.txt",
    help="File to read in.",
)
@click.option(
    "--output", required=True, help="Output prefix for files to write results to."
)
@click.pass_context
def run(
    ctx: click.Context, input: str, output: str, save_main_session: bool = True
) -> None:
    """Main entry point; defines and runs the wordcount pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(ctx.args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        lines = p | "Read" >> ReadFromText(input)

        counts = (
            lines
            | "Split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | "PairWithOne" >> beam.Map(lambda x: (x, 1))
            | "GroupAndSum" >> beam.CombinePerKey(sum)  # type: ignore
        )

        # Format the counts into a PCollection of strings.
        def format_result(word, count):
            return f"{word}: {count}"

        formatted_line = counts | "Format" >> beam.MapTuple(format_result)

        # Write the output using a "Write" transform that has side effects.
        _ = formatted_line | "Write" >> WriteToText(output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
