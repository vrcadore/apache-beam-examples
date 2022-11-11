import logging

import apache_beam as beam
import click
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


class Count1(beam.PTransform):
    """Count as a subclass of PTransform, with an apply method."""

    def expand(self, pcoll):
        return (
            pcoll | "ParWithOne" >> beam.Map(lambda v: (v, 1)) | beam.CombinePerKey(sum)
        )


def run_count1(input, output, options):
    """Runs the first example pipeline."""
    logging.info("Running first pipeline")
    with beam.Pipeline(options=options) as p:
        (p | beam.io.ReadFromText(input) | Count1() | beam.io.WriteToText(output))


@beam.ptransform_fn
def Count2(pcoll):  # pylint: disable=invalid-name
    """Count as a decorated function."""
    return pcoll | "PairWithOne" >> beam.Map(lambda v: (v, 1)) | beam.CombinePerKey(sum)


def run_count2(input, output, options):
    """Runs the second example pipeline."""
    logging.info("Running second pipeline")
    with beam.Pipeline(options=options) as p:
        (
            p
            | ReadFromText(input)
            | Count2()  # pylint: disable=no-value-for-parameter
            | WriteToText(output)
        )


@beam.ptransform_fn
def Count3(pcoll, factor=1):  # pylint: disable=invalid-name
    """Count as a decorated function with a side input.
    Args:
        pcoll: the PCollection passed in from the previous transform
        factor: the amount by which to count
    Returns:
        A PCollection counting the number of times each unique element occurs.
    """
    return (
        pcoll
        | "PairWithOne" >> beam.Map(lambda v: (v, factor))
        | beam.CombinePerKey(sum)
    )


def run_count3(input, output, options):
    """Runs the third example pipeline."""
    logging.info("Running third pipeline")
    with beam.Pipeline(options=options) as p:
        (
            p
            | ReadFromText(input)
            | Count3(2)  # pylint: disable=no-value-for-parameter
            | WriteToText(output)
        )


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

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(ctx.args)

    run_count1(input, output, pipeline_options)
    run_count2(input, output, pipeline_options)
    run_count3(input, output, pipeline_options)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
