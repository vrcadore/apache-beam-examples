import logging
import re
from typing import Any, Iterable

import apache_beam as beam
import click
from apache_beam import pvalue
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class SplitLinesToWordsFn(beam.DoFn):
    """A transform to split a line of text into individual words.
    This transform will have 3 outputs:
        - main output: all words that are longer than 3 characters.
        - short words output: all other words.
        - character count output: Number of characters in each processed line.
    """

    # These tags will be used to tag the outputs of this DoFn.
    OUTPUT_TAG_SHORT_WORDS = "tag_short_words"
    OUTPUT_TAG_CHARACTER_COUNT = "tag_character_count"

    def process(self, element: str) -> Iterable[Any]:
        """Receives a single element (a line) and produces words and character
        counts.
        Important things to note here:
            - For a single element you may produce multiple main outputs:
                words of a single line.
            - For that same input you may produce multiple outputs, potentially
                across multiple PCollections
            - Outputs may have different types (count) or may share the same type
                (words) as with the main output.
        Args:
        element: processing element.
        Yields:
        words as main output, short words as tagged output, line character count
        as tagged output.
        """
        # yield a count (integer) to the OUTPUT_TAG_CHARACTER_COUNT tagged
        # collection.
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_CHARACTER_COUNT, len(element))

        words = re.findall(r"[A-Za-z\']+", element)
        for word in words:
            if len(word) <= 3:
                # yield word as an output to the OUTPUT_TAG_SHORT_WORDS tagged
                # collection.
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_SHORT_WORDS, word)
            else:
                # yield word to add it to the main collection.
                yield word


class CountWords(beam.PTransform):
    """A transform to count the occurrences of each word.
    A PTransform that converts a PCollection containing words into a PCollection
    of "word: count" strings.
    """

    def expand(self, pcoll):
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        def format_result(word_count):
            (word, count) = word_count
            return "%s: %s" % (word, count)

        return (
            pcoll
            | "pair_with_one" >> beam.Map(lambda x: (x, 1))
            | "group" >> beam.GroupByKey()
            | "count" >> beam.Map(count_ones)
            | "format" >> beam.Map(format_result)
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
    """Runs the workflow counting the long words and short words separately."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(ctx.args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        lines = p | ReadFromText(input)

        # with_outputs allows accessing the explicitly tagged outputs of a DoFn.
        split_lines_result = lines | beam.ParDo(SplitLinesToWordsFn()).with_outputs(
            SplitLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS,
            SplitLinesToWordsFn.OUTPUT_TAG_CHARACTER_COUNT,
            main="words",
        )

        words, _, _ = split_lines_result  # type: ignore
        short_words = split_lines_result.tag_short_words  # type: ignore
        character_count = split_lines_result.tag_character_count  # type: ignore

        _ = (
            character_count
            | "Pair With Key" >> beam.Map(lambda x: ("chars_temp_key", x))
            | "Group By Key" >> beam.GroupByKey()
            | "Count Chars" >> beam.Map(lambda char_counts: sum(char_counts[1]))
            | "Write Chars" >> WriteToText(output + "-chars")
        )

        _ = (
            short_words
            | "Count Short Words" >> CountWords()
            | "Write Short Words" >> WriteToText(output + "-short-words")
        )

        _ = (
            words
            | "Count Words" >> CountWords()
            | "Write Words" >> WriteToText(output + "-words")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
