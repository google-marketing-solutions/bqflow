###########################################################################
#
#  Copyright 2025 Google LLC
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

"""This module provides helpers to work with vertex image and text AI.

It mainly provides default parameters, some error checking, and retries.
It is designed to be as thin as possible hence the use of argument unpacking.
Some interoperability with PIL is also provided.

For reference TextAI parameters can be found at:

https://github.com/googleapis/python-aiplatform/blob/main/vertexai/generative_models/_generative_models.py

For reference ImageAI parameters can be found at:

https://github.com/googleapis/python-aiplatform/blob/main/vertexai/vision_models/_vision_models.py
"""

import copy
import io
import json
import re
import time

try:
  import dirtyjson
except ModuleNotFoundError as e:
  raise ModuleNotFoundError(
      'PLEASE RUN: python3 -m pip install dirtyjson'
  ) from e

from typing import Any, Callable, Iterator
from google.api_core.exceptions import InternalServerError
from google.api_core.exceptions import ResourceExhausted

try:
  import vertexai
  from vertexai.generative_models import GenerationConfig
  from vertexai.generative_models import GenerativeModel
  from vertexai.generative_models import HarmBlockThreshold
  from vertexai.generative_models import HarmCategory
  from vertexai.generative_models import Part
  from vertexai.preview.vision_models import ImageGenerationModel
  from vertexai.preview.vision_models import Image
except ModuleNotFoundError as e:
  raise ModuleNotFoundError(
      'PLEASE RUN: python3 -m pip install google-cloud-aiplatform'
  ) from e

try:
  from PIL import Image as PIL_Image
except ImportError:
  raise ImportError('PLEASE RUN: python3 -m pip install pillow')

from bqflow.util.auth import get_credentials
from bqflow.util.configuration import Configuration

RETRIES = 3
REGION = 'us-central1'

RE_LIST = re.compile(r'\[.*\]', re.DOTALL)
RE_DICT = re.compile(r'{.*}', re.DOTALL)
RE_HTML = re.compile(r'<.*>', re.DOTALL)

TEXT_MODEL = 'gemini-1.5-pro'
TEXT_GENERATE_PARAMETERS = {
    'generation_config': GenerationConfig(
        temperature=None,
        top_p=None,
        top_k=None,
        candidate_count=1,
        max_output_tokens=8192,
        seed=1
    ),
    'safety_settings': {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    },
    'stream': False
}

IMAGE_SIZE = (1024, 1024)
IMAGE_MODEL = 'imagegeneration@006'
IMAGE_GENERATE_PARAMETERS = {
    'output_mime_type': 'image/jpeg',
    'compression_quality': 92,
    'person_generation': 'dont_allow',
    'number_of_images': 1,
    'seed': 1,
    'safety_filter_level': 'block_few',
    'add_watermark': False
}
IMAGE_EDIT_PARAMETERS = {
    'edit_mode': 'product-image',
    'mask_mode': 'foreground',
    'product_position': 'reposition',
    'output_mime_type': 'image/jpeg',
    'compression_quality': 92,
    'person_generation': 'dont_allow',
    'guidance_scale': 9,
    'number_of_images': 1,
    'seed': 1,
    'safety_filter_level': 'block_few'
}


def retry_ai(
    function: Callable,
    **parameters: dict
) -> Any:
  """Wraps AI API calls in retry blocks to handle or enhance error messages.

  Args:
    function: a vertex model function to call
    **parameters: parameters for the vertex model

  Returns:
    response: any vertex API response object

  Throws:
    Any unhandled or retry exhausted vertex API exceptions.
  """

  for retry in range(RETRIES, 0, -1):
    try:
      return function(**parameters)

    except InternalServerError:
      if retry > 0:
        print(f'THROTTLE ERROR RETRY {retry}...')
        time.sleep(10)
      else:
        raise

    except ResourceExhausted:
      if retry > 0:
        print(f'THROTTLE ERROR RETRY {retry}...')
        time.sleep(10)
      else:
        raise

    except AttributeError as ex:
      error_message = str(ex)
      if retry > 0 and 'Content has no parts' in error_message:
        print(f'ATTRIBUTE ERROR RETRY {retry}:', error_message)
        print(f'Error: {ex} the response may have safety issues.\n')
        time.sleep(10)
      else:
        raise

    except Exception as ex:
      error_message = str(ex)
      if (retry > 0 and (
          '429 Quota exceeded' in error_message
          or '503 The service is currently unavailable' in error_message
          or '403' in error_message
      )):
        print(f'GENERAL EXCEPTION RETRY {retry}:', type(ex), str(ex))
        time.sleep(10)
      else:
        raise


class TextAI():
  """Helper for generating text, dict, list, or HTML using vertex AI."""

  def __init__(
      self,
      config: Configuration,
      auth: str,
      location: str = REGION,
      text_model: str = TEXT_MODEL
  ) -> None:
    """Construct an AI helper, providing project and authentication data.

    Initialize vertex API and select the correct model for text processing.

    Args:
      config: see util/configuration.py
      auth: either 'user' or 'service' used to create and/or read the report.
      location: a cloud region where the vertex API is enabled
      text_model: name of the LLM to use for text generation

    Returns: None
    """

    vertexai.init(
        project=config.project,
        location=location,
        credentials=get_credentials(config, auth)
    )

    self.config = config
    self.auth = auth
    self.text_model = GenerativeModel(text_model)

  def safely_generate_text(
      self,
      prompt: str,
      parts: list = None,
      **kwargs: dict
  ) -> str:
    """Helper for generate_content with defaults and retry."""

    if not kwargs:
      kwargs = copy.deepcopy(TEXT_GENERATE_PARAMETERS)

    kwargs['contents'] = parts + [prompt] if parts else [prompt]
    return retry_ai(self.text_model.generate_content, **kwargs).text.strip()

  def safely_generate_dict(
      self,
      prompt: str,
      parts: list = None,
      **kwargs: dict
  ) -> dict:
    """Helper that extracts a dict from the AI response."""
    try:
      text = self.safely_generate_text(prompt, parts, **kwargs)
      return dirtyjson.loads(RE_DICT.search(text).group(0))
    except dirtyjson.error.Error:
      print('Parse JSON Error:', text)
      raise
    except AttributeError as e:
      raise AttributeError(f'No dict found:\n {text}') from e
    except json.JSONDecodeError:
      print('Parse JSON Error:', text)
      raise

  def safely_generate_list(
      self,
      prompt: str,
      parts: list = None,
      **kwargs: dict
  ) -> list:
    """Helper that extracts a list from the AI response."""
    try:
      text = self.safely_generate_text(prompt, parts, **kwargs)
      return dirtyjson.loads(RE_LIST.search(text).group(0))
    except dirtyjson.error.Error:
      print('Parse JSON Error:', text)
      raise
    except AttributeError as e:
      raise AttributeError(f'No list found:\n {text}') from e
    except json.JSONDecodeError:
      print('Parse JSON Error:', text)
      raise

  def safely_generate_html(
      self,
      prompt: str,
      parts: list = None,
      **kwargs: dict
  ) -> str:
    """Helper that extracts HTML from the AI response."""
    try:
      text = self.safely_generate_text(prompt, parts, **kwargs)
      return RE_HTML.search(text).group(0)
    except AttributeError as e:
      raise AttributeError(f'No HTML tags found:\n {text}') from e


class ImageAI():
  """Helper to make working with images easier, mostly provides defaults."""

  def __init__(
      self,
      config: Configuration,
      auth: str,
      location: str = REGION,
      image_model: str = IMAGE_MODEL
  ) -> None:
    """Construct an AI helper, providing project and authentication data.

    Initialize vertex API and select the correct model for image processing.

    Args:
      config: see util/configuration.py
      auth: either 'user' or 'service' used to create and/or read the report.
      location: a cloud region where the vertex API is enabled
      image_model: name of the vision model to use for image generation

    Returns: None
    """

    vertexai.init(
        project=config.project,
        location=location,
        credentials=get_credentials(config, auth)
    )

    self.config = config
    self.auth = auth
    self.image_model = ImageGenerationModel.from_pretrained(image_model)

  @staticmethod
  def pil_to_image(image: PIL_Image) -> Image:
    """Helper that converts PIL image to Vertex image."""
    if image:
      image_bytes = io.BytesIO()
      image.save(image_bytes, image.format)
      image_bytes.seek(0)
      return Image(image_bytes.getvalue())
    else:
      return None

  @staticmethod
  def resize_image(image: Image, size: (int, int)) -> Image:
    """Helper that resizes image and preserves type."""
    if image:
      image_pil = image._pil_image
      image_pil.resize(size)
      return ImageAI.pil_to_image(image_pil)
    else:
      return None

  @staticmethod
  def max_image(image: Image) -> Image:
    """Helper that enforces maximum image size and preserves type."""
    if image:
      size = image._pil_image.size
      if size[0] > IMAGE_SIZE[0] or size[1] > IMAGE_SIZE[1]:
        return ImageAI.resize_image(image, IMAGE_SIZE)
    return image

  def safely_generate_image(
      self,
      prompt: str,
      **kwargs: dict
  ) -> Iterator[Any]:
    """Helper for _generate_images with defaults, max size, and retry."""

    if not kwargs:
      kwargs = copy.deepcopy(IMAGE_GENERATE_PARAMETERS)

    kwargs['prompt'] = prompt
    yield from retry_ai(self.image_model._generate_images, **kwargs)

  def safely_edit_image(
      self,
      prompt: str,
      base_image: Image,
      **kwargs: dict
  ) -> Iterator[Any]:
    """Helper for edit_image with defaults, max size, and retry."""

    if not kwargs:
      kwargs = copy.deepcopy(IMAGE_EDIT_PARAMETERS)

    kwargs['prompt'] = prompt
    kwargs['base_image'] = ImageAI.max_image(base_image)
    kwargs['mask'] = ImageAI.max_image(kwargs.get('mask'))

    yield from retry_ai(self.image_model.edit_image, **kwargs)
