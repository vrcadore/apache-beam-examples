import logging
import unittest

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that, equal_to

from src.pipelines.side_input_example import pipeline


class BigQuerySideInputTest(unittest.TestCase):

    @pytest.mark.components
    def test_create_groups(self):

        # Arrange
        IDS = ["A", "B", "C"]
        CORPUS = [{"f": "corpus1"}, {"f": "corpus2"}]
        WORDS = [{"f": "word1"}, {"f": "word2"}]
        IGNORE_CORPUS = ["corpus1"]
        IGNORE_WORDS = ["word1"]
        EXPECTED_OUTPUT = [
            ("A", "corpus2", "word2"),
            ("B", "corpus2", "word2"),
            ("C", "corpus2", "word2"),
        ]

        # Act

        from apache_beam.testing.test_pipeline import TestPipeline
        with TestPipeline() as p:

            group_ids_pcoll = p | "CreateGroupIds" >> beam.Create(IDS)
            corpus_pcoll = p | "CreateCorpus" >> beam.Create(CORPUS)
            words_pcoll = p | "CreateWords" >> beam.Create(WORDS)
            ignore_corpus_pcoll = p | "CreateIgnoreCorpus" >> beam.Create(IGNORE_CORPUS)
            ignore_word_pcoll = p | "CreateIgnoreWord" >> beam.Create(IGNORE_WORDS)

            groups = pipeline.create_groups(
                group_ids_pcoll,
                corpus_pcoll,
                words_pcoll,
                ignore_corpus_pcoll,
                ignore_word_pcoll,
            )

            # Assert
            assert_that(groups, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
