###########################################################################
#
#  Copyright 2021 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

"""Test harness for BQFlow utils.

To run this test harness the following ENV variables must be set:
  - BQFLOW_PROJECT - Google Cloud Project name.
  - BQFLOW_USER - Optional path to user credentials to run the test.
  - BQFLOW_SERVICE - Optional path to service credentials to run the test.
  - BQFLOW_KEY - Optional Google Cloud Project API key.
  - BQFLOW_TIMEZONE - Optional, defaults to 'America/Los_Angeles'.
  - BQFLOW_VERBOSE - Set to True or False, to toggle screen prints.

Example:
  export BQFLOW_PROJECT='gtech-kenjora'
  export BQFLOW_USER='/Users/kenjora/user.json'
  python -m unittest tests.TestCSV.test_find_utf8_split
"""

import unittest
import io
import os

from bqflow.util.csv import response_utf8_stream
from bqflow.util.configuration import Configuration
from bqflow.util.discovery_to_bigquery import Discovery_To_BigQuery
from bqflow.util.vertexai_api import Image
from bqflow.util.vertexai_api import ImageAI
from bqflow.util.vertexai_api import TextAI


class TestCSV(unittest.TestCase):
  """Test the csv module.  Currently only testing utf-8 function but WIP."""

  def test_find_utf8_split(self):
    """Tests: response_utf8_stream so reports stream correctly.

    Verify that encoding is an issue when not boundary aligned.
    Run boundary detection against 3 different UTF-8 encodings of different byte lengths.
    Pick 17 (prime) as a chunk size to ensure utf-8 byte boundary is hit.
    Run against multiple chunks to ensure test goes in and out of utf-8 alignement.
    """

    string_ascii = bytes('"#$%&()*+,-./0123456789:;<=>?@ABCDEF', 'utf-8')
    string_arabic = bytes('،؛؟ءآأؤإئابةتثجحخدذرزسشصضطظعغـفقكلم', 'utf-8')
    string_misc = bytes('⌀⌂⌃⌄⌅⌆⌇⌈⌉⌊⌋⌌⌍⌎⌏⌐⌑⌒⌓⌔⌕⌖⌗⌘⌙⌚⌛⌜⌝⌞⌟⌠⌡⌢⌣', 'utf-8')
    string_cjk = bytes('豈更車勞擄櫓爐盧老蘆虜路露魯鷺碌祿綠菉錄縷陋勒諒量', 'utf-8')

    # verify raw split causes error
    try:
      string_ascii[:17].decode("utf-8")
    except UnicodeDecodeError:
      self.fail("ASCII bytes should fit within utf-8.")

    with self.assertRaises(UnicodeDecodeError):
      string_arabic[:17].decode("utf-8")

    with self.assertRaises(UnicodeDecodeError):
      string_misc[:17].decode("utf-8")

    with self.assertRaises(UnicodeDecodeError):
      string_cjk[:17].decode("utf-8")

    # verify various utf-8 lengths work
    self.assertEqual(next(response_utf8_stream(io.BytesIO(string_ascii), 17)), '"#$%&()*+,-./0123')
    self.assertEqual(next(response_utf8_stream(io.BytesIO(string_arabic), 17)), '،؛؟ءآأؤإ')
    self.assertEqual(next(response_utf8_stream(io.BytesIO(string_misc), 17)), '⌀⌂⌃⌄⌅')

    # verify middle and last parts of splits work
    chunks = response_utf8_stream(io.BytesIO(string_cjk), 17)
    self.assertEqual(next(chunks), '豈更車勞擄')
    self.assertEqual(next(chunks), '櫓爐盧老蘆虜')
    self.assertEqual(next(chunks), '路露魯鷺碌祿')
    self.assertEqual(next(chunks), '綠菉錄縷陋')
    self.assertEqual(next(chunks), '勒諒量')


class TestAI(unittest.TestCase):
  """Test the vertexai_api module."""

  def setUp(self) -> None:
    """Initialize any credentials and time units for the test, read from ENV."""

    super().setUp()

    self.assertIsNotNone(
        os.environ.get('BQFLOW_PROJECT'),
        msg='No env variable, run: export BQFLOW_PROJECT="GCP PROJECT"',
    )

    self.assertIsNotNone(
        os.environ.get('BQFLOW_USER') or os.environ.get('BQFLOW_SERVICE'),
        msg='No env variable, run: export BQFLOW_USER="CREDENTIALS PATH"',
    )

    self.config = Configuration(
        project=os.environ.get('BQFLOW_PROJECT'),
        service=os.environ.get('BQFLOW_SERVICE'),
        user=os.environ.get('BQFLOW_USER'),
        key=os.environ.get('BQFLOW_KEY'),
        timezone=os.environ.get('BQFLOW_TIMEZONE', 'America/Los_Angeles'),
        verbose=os.environ.get('BQFLOW_VERBOSE', 'false').lower() == 'true',
    )

    self.auth = 'service' if os.environ.get('BQFLOW_KEY') else 'user'

  def test_text_ai(self):
    """Test main path TextAI functionality."""

    text_ai = TextAI(config=self.config, auth=self.auth)

    self.assertEqual(
        text_ai.safely_generate_text(
            prompt='Please say just the word "hi".'
        ),
        'hi'
    )

    self.assertEqual(
        text_ai.safely_generate_list(
            prompt='Generate a python list with the integers 0 to 9 in sequential order.'
        ),
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    )

    self.assertEqual(
        text_ai.safely_generate_dict(
            prompt='Generate a python dict with the word "cat" as key and "dog" as value.'
        ),
        {'cat': 'dog'}
    )

    self.assertEqual(
        text_ai.safely_generate_html(
            prompt='Wrap the sentence "The red fox." into an HTML paragraph tag.'
        ),
        '<p>The red fox.</p>'
    )

  def test_image_ai(self):
    """Test main path ImageAI functionality."""

    image_ai = ImageAI(config=self.config, auth=self.auth)

    image = next(image_ai.safely_generate_image(
        prompt='Generate a picture of a dog.'
    ))
    self.assertIsInstance(image, Image)

    image = next(image_ai.safely_edit_image(
        prompt='Add a cat next to the dog.',
        base_image=image
    ))
    self.assertIsInstance(image, Image)

if __name__ == '__main__':
  unittest.main()


class TestDiscovery(unittest.TestCase):
  """Test the discovery_to_bigquery module."""

  def test_discovery(self):
    spec = Discovery_To_BigQuery('dfareporting', 'v4').method_schema('userProfiles.get', False)

    self.assertEqual(
        spec,
        [ {'description': '', 'name': 'accountId', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'accountName', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'etag', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'kind', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'profileId', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'subAccountId', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'subAccountName', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'description': '', 'name': 'userName', 'type': 'STRING', 'mode': 'NULLABLE' }
        ]
    )
