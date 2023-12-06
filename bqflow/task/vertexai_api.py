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

'''
This task calls the VERTEX API client and passes parameters for running a model.

Input can be given using a BigQuery query or hard coded parameters.  This allows
running groups of prompts and condolidates them into a single results table.

Calling JSON (add this to the workflow):

  { "vertexai_api":{
    "auth":"user",
    "location":"us-central1",
    "model":{
      "class":"vertexai.preview.language_models.TextGenerationModel",
      "name":"text-bison@001",
      "type":"pretrained"
    },
    "destination":{
      "bigquery":{
        "dataset":"cse_hackathon",
        "table":"VERTEX_Text_Data",
        "schema":[
          { "name":"URI", "type":"STRING", "mode":"REQUIRED" },
          { "name":"Text", "type":"STRING", "mode":"REQUIRED" }
        ]
      }
    },
    "kwargs_remote":{
      "bigquery":{
        "dataset":"cse_hackathon",
        "query":"SELECT lineItemId AS uri, STRUCT(0 AS temperature, 1024 AS max_output_tokens, 0.8 AS top_p, 40 AS top_k, 'Write Something' AS Pro AS prompt) AS parameters
           FROM `DV360_LineItems_Targeting`
        "
      }
    }
  }}
'''

import importlib

try:
  import vertexai
except ModuleNotFoundError as e:
  raise ModuleNotFoundError('PLEASE RUN: python3 -m pip install google-cloud-aiplatform') from e


from bqflow.util.auth import get_credentials
from bqflow.util.data import get_rows, put_rows

# is this faster: https://github.com/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/community/bigquery_ml/bq_ml_with_vision_translation_nlp.ipynb

def vertexai_api(config, log, task):
  """A wrapper for the Vertex API client allowing calls from workflows.

  This wrapper consolidates multiple calls into a single table.

  Args:
    config (class): an object conatining the credentials and project settings
    log (class): an object that can be logged to.
    task (dict): the parameters passed from the workflow (see top level doc).

  Returns (list):
    A list of rows with the passed in schema.
  """

  # authenticate
  vertexai.init(
    project=config.project,
    location=task['location'],
    credentials=get_credentials(config, task['auth'])
  )

  # get model
  import_path, import_class = task['model']['class'].rsplit('.', 1)
  model_class = getattr(importlib.import_module(import_path), import_class)

  if task['model']['type'] == "tuned":
    model = model_class.get_tuned_model(task['model']['name'])
  else:
    model = model_class.from_pretrained(task['model']['name'])

  # get parameters
  if 'kwargs' in task:
    if isinstance(task['kwargs'], (list, tuple)):
      kwargs_list = task['kwargs']
    else:
      kwargs_list = [task['kwargs']]

  elif 'kwargs_remote' in task:
    kwargs_list = get_rows(
      config,
      task['auth'],
      task['kwargs_remote'],
      as_object=True
    )

  # write results
  def vertex_api_combine():
    for kwargs in kwargs_list:
      if config.verbose:
        print('.', end='', flush=True)
      yield kwargs['uri'], model.predict(**kwargs['parameters'])

  return put_rows(
    config = config,
    auth = task['auth'],
    destination = task['destination'],
    rows = vertex_api_combine()
  )
