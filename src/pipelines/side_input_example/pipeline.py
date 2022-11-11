import logging
from random import randrange

import apache_beam as beam
import click
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pvalue import AsList, AsSingleton


def create_groups(group_ids, corpus, word, ignore_corpus, ignore_word):
    """Generate groups given the input PCollections."""

    def attach_corpus_fn(group, corpus, ignore):
        selected = None
        len_corpus = len(corpus)
        while not selected:
            c = list(corpus[randrange(0, len_corpus)].values())[0]
            if c != ignore:
                selected = c

        yield (group, selected)

    def attach_word_fn(group, words, ignore):
        selected = None
        len_words = len(words)
        while not selected:
            c = list(words[randrange(0, len_words)].values())[0]
            if c != ignore:
                selected = c

        yield group + (selected,)

    return (
        group_ids
        | "Attach corpus"
        >> beam.FlatMap(attach_corpus_fn, AsList(corpus), AsSingleton(ignore_corpus))
        | "Attach word"
        >> beam.FlatMap(attach_word_fn, AsList(word), AsSingleton(ignore_word))
    )


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.option("--output", required=True)
@click.option("--ignore_corpus", default="")
@click.option("--ignore_word", default="")
@click.option("--num_groups", default=1)
@click.pass_context
def run(
    ctx: click.Context,
    output: str,
    ignore_corpus: str,
    ignore_word: str,
    num_groups: int,
    save_main_session: bool = True,
) -> None:
    """
    A Dataflow job that uses BigQuery sources as a side inputs.
    Illustrates how to insert side-inputs into transforms in three different forms,
    as a singleton, as a iterator, and as a list.

    This workflow generate a set of tuples of the form (groupId, corpus, word) where
    groupId is a generated identifier for the group and corpus and word are randomly
    selected from corresponding rows in BQ dataset 'publicdata:samples.shakespeare'.

    Users should specify the number of groups to form and optionally a corpus and/or
    a word that should be ignored when forming groups.
    """

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(ctx.args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        group_ids = [f"id{i}" for i in range(0, int(num_groups))]

        query_corpus = "select UNIQUE(corpus) from publicdata:samples.shakespeare"
        query_word = "select UNIQUE(word) from publicdata:samples.shakespeare"
        ignore_corpus = ignore_corpus
        ignore_word = ignore_word

        p_corpus = p | "Read Corpus" >> beam.io.ReadFromBigQuery(query=query_corpus)
        p_word = p | "Read Words" >> beam.io.ReadFromBigQuery(query=query_word)
        p_ignore_corpus = p | "Create Ignore Corpus" >> beam.Create([ignore_corpus])
        p_ignore_word = p | "Create Ignore Word" >> beam.Create([ignore_word])
        p_group_ids = p | "Create Groups" >> beam.Create(group_ids)

        p_groups = create_groups(
            p_group_ids,
            p_corpus,
            p_word,
            p_ignore_corpus,
            p_ignore_word,
        )

        _ = p_groups | WriteToText(output)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
