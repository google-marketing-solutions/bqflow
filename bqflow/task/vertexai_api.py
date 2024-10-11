###########################################################################
#
#  Copyright 2020 Google LLC
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

"""This task calls the VERTEX API client passing parameters for running a model.

https://github.com/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/community/bigquery_ml/bq_ml_with_vision_translation_nlp.ipynb

Input can be given using a BigQuery query or hard coded parameters.  This allows
running groups of prompts and condolidates them into a single results table.

Calling image generation:

  { "vertexai_api":{
    "description":"Generate images using vertext imagen model.",
    "auth":"user",
    "location":"us-central1",
    "model":{
      "class":"vertexai.preview.vision_models.ImageGenerationModel",
      "name":"imagegeneration@005",
      "function":"_generate_images",
      "type":"pretrained"
    },
    "destination":{
      "drive":"1sknq05IWjBdic2otU1NiXKaFxSctffeo"
    },
    "kwargs_remote": {
      "bigquery":{
        "dataset": "BQFlowVertex",
        "query": "
          SELECT *
          FROM UNNEST([
            STRUCT('puppy' AS uri, STRUCT(1 AS number_of_images, 1 AS seed,
            'Picture of a cute puppy.' AS prompt) AS parameters),
            STRUCT('kitten' AS uri, STRUCT(1 AS number_of_images, 1 AS seed,
            'Picture of a cute kitten.' AS prompt) AS parameters),
            STRUCT('duckling' AS uri, STRUCT(1 AS number_of_images, 1 AS seed,
            'Picture of a cute duckling.' AS prompt) AS parameters)
          ])
        "
      }
    }
  }}

Calling text generation:

  { "vertexai_api": {
    "auth": "user",
    "location": "us-central1",
    "model": {
      "class": "vertexai.preview.language_models.TextGenerationModel",
      "name": "text-bison@001",
      "type": "pretrained"
    },
    "destination": {
      "bigquery": {
        "dataset": "BQFlowVertex",
        "table": "VERTEX_TextData",
        "schema": [
          { "name": "URI", "type": "STRING", "mode": "REQUIRED" },
          { "name": "Text", "type": "STRING", "mode": "REQUIRED" }
        ]
      }
    },
    "kwargs_remote": {
      "bigquery": {
        "dataset": "cse_hackathon",
        "query": "
          SELECT *
          FROM UNNEST([
            STRUCT('puppy' AS uri, STRUCT(1024 AS max_output_tokens, 0.8 AS
            top_p, 'Picture of a cute puppy.' AS prompt) AS parameters),
            STRUCT('kitten' AS uri, STRUCT(1024 AS max_output_tokens, 0.8 AS
            top_p, 'Picture of a cute kitten.' AS prompt) AS parameters),
            STRUCT('duckling' AS uri, STRUCT(1024 AS max_output_tokens, 0.8 AS
            top_p, 'Picture of a cute duckling.' AS prompt) AS parameters)
          ])
        "
      }
    }
  }}
"""

import importlib
import io

from collections.abc import Iterator, Mapping
from vertexai.preview.vision_models import Image

try:
  import vertexai
except ModuleNotFoundError as e:
  raise ModuleNotFoundError(
      'PLEASE RUN: python3 -m pip install google-cloud-aiplatform'
  ) from e

try:
  from PIL import Image as PIL_Image
except ImportError:
  PIL_Image = None

from bqflow.util.auth import get_credentials
from bqflow.util.configuration import Configuration
from bqflow.util.data import get_rows, put_rows
from bqflow.util.drive import Drive
from bqflow.util.log import Log


def resize_image(path: str, size: (int, int)) -> bytes:
  """A basic image resizer."""
  if PIL_Image is None:
    raise ModuleNotFoundError(
        'TO RESIZE IMAGES PLEASE RUN: python3 -m pip install pillow'
    )
  with PIL_Image.open(path) as img:
    img_bytes = io.BytesIO()
    img.resize(size)
    img.save(img_bytes, img.format)
    img_bytes.seek(0)
    return img_bytes.getvalue()


def vertexai_api(
    config: Configuration, log: Log, task: Mapping
) -> Iterator[Mapping]:
  """A wrapper for the Vertex API client allowing calls from workflows.

  This wrapper consolidates multiple calls into a single table.

  Args:
    config: an object conatining the credentials and project settings
    log: required as part of the factory interface but not used here.
    task: the parameters passed from the workflow (see top level doc).

  Returns:
    A list of rows with the passed in schema.
  """

  # authenticate
  vertexai.init(
      project=config.project,
      location=task['location'],
      credentials=get_credentials(config, task['auth']),
  )

  # get model
  import_path, import_class = task['model']['class'].rsplit('.', 1)
  model_class = getattr(importlib.import_module(import_path), import_class)

  # get function
  if task['model']['type'] == 'tuned':
    model = model_class.get_tuned_model(task['model']['name'])
    model_function = getattr(model, task['model']['function'])
  else:
    try:
      model = model_class.from_pretrained(task['model']['name'])
    except AttributeError:
      model = model_class(task['model']['name'])
    model_function = getattr(model, task['model']['function'])

  # get parameters
  if 'kwargs' in task:
    kwargs_list = (
        task['kwargs']
        if isinstance(task['kwargs'], (list, tuple))
        else [task['kwargs']]
    )
  elif 'kwargs_remote' in task:
    kwargs_list = get_rows(
        config, task['auth'], task['kwargs_remote'], as_object=True
    )

  # write results
  def vertex_api_combine():
    for kwargs in kwargs_list:
      if config.verbose:
        print(kwargs['uri'])

      if 'base_image' in kwargs['parameters']:
        if 'resize' in task['model']:
          kwargs['parameters']['base_image'] = Image(
              resize_image(
                  kwargs['parameters']['base_image'], task['model']['resize']
              )
          )
        else:
          kwargs['parameters']['base_image'] = Image.load_from_file(
              location=kwargs['parameters']['base_image']
          )
      if 'mask' in kwargs['parameters']:
        if 'resize' in task['model']:
          kwargs['parameters']['mask'] = Image(
              resize_image(
                  kwargs['parameters']['mask'], task['model']['resize']
              )
          )
        else:
          kwargs['parameters']['mask'] = Image.load_from_file(
              location=kwargs['parameters']['mask']
          )

      yield kwargs['uri'], model_function(**kwargs['parameters']), kwargs[
          'parameters'
      ].get('output_mime_type', 'txt').replace('image/', '').replace(
          'jpeg', 'jpg'
      )

  if 'bigquery' in task['destination']:
    return put_rows(
        config=config,
        auth=task['auth'],
        destination=task['destination'],
        rows=[
            [response[0], response[1].text.strip()]
            for response in vertex_api_combine()
        ],
    )
  elif 'drive' in task['destination']:
    for uri, images, extension in vertex_api_combine():
      for index, image in enumerate(images):
        Drive(config, task['auth']).file_create(
            name=f'{uri}-{index}.{extension}',
            data=image._image_bytes,
            parent=task['destination']['drive'],
            overwrite=True
        )
  elif 'local' in task['destination']:
    for uri, images, extension in vertex_api_combine():
      for index, image in enumerate(images):
        image.save(f'{task["destination"]["local"]}/{uri}-{index}.{extension}')
  else:
    raise NotImplementedError(
        'The destination parameter must include "bigquery", "drive" or "local".'
        'See bqflow/task/vertex_api.py for examples.'
    )
