import collections
import logging
import re
import tempfile
import unittest

import pytest
from apache_beam.testing.util import open_shards
from click.testing import CliRunner

from src.pipelines.wordcount import pipeline


class WordCountTest(unittest.TestCase):

    SAMPLE_TEXT = "a b c a b a\nacento gráfico\nJuly 30, 2018\n\n aa bb cc aa bb aa"

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents.encode("utf-8"))
            return f.name

    @pytest.mark.pipelines
    def test_wordcount(self):
        # Assign
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        expected_words = collections.defaultdict(int)

        for word in re.findall(r"[\w\']+", self.SAMPLE_TEXT, re.UNICODE):
            expected_words[word] += 1

        # Act
        runner = CliRunner()
        result = runner.invoke(
            pipeline.run,
            f"--input={temp_path}* --output={temp_path}.result"
        )

        # Assert
        assert result.exit_code == 0

        # Parse result file and compare.
        results = []
        with open_shards(f"{temp_path}.result-*-of-*") as result_file:
            for line in result_file:
                match = re.search(r"(\S+): ([0-9]+)", line)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
            self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
