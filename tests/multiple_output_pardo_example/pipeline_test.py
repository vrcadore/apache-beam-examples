import logging
import re
import tempfile
import unittest

import pytest
from apache_beam.testing.util import open_shards
from click.testing import CliRunner

from src.pipelines.multiple_output_pardo import pipeline


class MultipleOutputParDo(unittest.TestCase):

    SAMPLE_TEXT = "A whole new world\nA new fantastic point of view"
    EXPECTED_SHORT_WORDS = [("A", 2), ("new", 2), ("of", 1)]
    EXPECTED_WORDS = [
        ("whole", 1),
        ("world", 1),
        ("fantastic", 1),
        ("point", 1),
        ("view", 1),
    ]

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents.encode("utf-8"))
            return f.name

    def get_wordcount_results(self, result_path):
        results = []
        with open_shards(result_path) as result_file:
            for line in result_file:
                match = re.search(r"([A-Za-z]+): ([0-9]+)", line)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
        return results

    @pytest.mark.pipelines
    def test_multiple_output_pardo(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        result_prefix = f"{temp_path}.result"
        logging.info(temp_path)

        runner = CliRunner()
        result = runner.invoke(
            pipeline.run,
            f"--input={temp_path}* --output={result_prefix}"
        )

        # Assert
        assert result.exit_code == 0

        expected_char_count = len("".join(self.SAMPLE_TEXT.split("\n")))
        with open_shards(result_prefix + "-chars-*-of-*") as f:
            contents = f.read()
            self.assertEqual(expected_char_count, int(contents))

        short_words = self.get_wordcount_results(result_prefix + "-short-words-*-of-*")
        self.assertEqual(sorted(short_words), sorted(self.EXPECTED_SHORT_WORDS))

        words = self.get_wordcount_results(result_prefix + "-words-*-of-*")
        self.assertEqual(sorted(words), sorted(self.EXPECTED_WORDS))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
