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

"""Moves data to and from Google Rest API and BigQuery.

Intended to be thin and pass control to recipe JSON for flexibility.
Task designed to be used with a recipe.
Leverages discovery document to define schema.
The results and error sections are optional.

For invocation see:
scripts/google_api_to_bigquery.json

Example Read DV360 Partner Name and Id Into BigQuery:

   { "google_api": {
      "auth": "user",
      "api": "displayvideo",
      "version": "v1",
      "function": "partners.list",
      "kwargs": {"fields":"partners.displayName,partners.partnerId,nextPageToken"},
      "results": {
        "bigquery": {
          "dataset": "DV_Barnacle",
          "table": "DV_Partners"
        }
      }
    }}

Example Read DV360 Advertisers defined in another BigQuery table and append the advertiserId:

    { "google_api": {
      "auth": "user",
      "api": "displayvideo",
      "version": "v1",
      "function": "advertisers.lineItems.list",
      "kwargs_remote":{
        "bigquery":{
          "dataset":"DV_Targeting_Audit",
          "query":"SELECT CAST(advertiserId AS STRING) AS advertiserId FROM `DV_Targeting_Audit.DV_Advertisers`;",
          "legacy":false
        }
      },
      "append":[
         { "name": "advertiserId", "type": "INTEGER", "mode": "REQUIRED" }
      ],
      "iterate":true,
      "results": {
        "bigquery": {
          "dataset": "DV_Targeting_Audit",
          "table": "DV_LineItems"
        }
      }
    }}

Example Write Insertion Orders into DV360 from BigQuery:

    { "google_api": {
      "auth": "user",
      "api": "displayvideo",
      "version": "v1",
      "function": "advertisers.insertionOrders.patch",
      "kwargs_remote":{
        "bigquery":{
          "dataset":"CM_DV_Demo",
          "table":"DV_IO_Patch"
        }
      },
      "results": {
        "bigquery": {
          "dataset": "CM_DV_Demo",
          "table": "DV_IO_Patch_Results"
        }
      }
    }}
"""

import traceback

from googleapiclient.errors import HttpError

from util.bigquery_api import BigQuery
from util.data import get_rows
from util.data import put_rows
from util.cm_api import get_profile_for_api
from util.google_api import API
from util.discovery_to_bigquery import Discovery_To_BigQuery


def google_api_initilaize(config, api_call, alias=None):
  """Some Google API calls require a lookup or pre-call, add it here.

  Modifies the API call before actual execution with any data
  specifically required by an endpoint.  Currently:

    > dfa-reporting - look up user profile and add to call.

  Args:
    api_call (dict): the JSON for the API call as defined in recipe.
    alias (string): mostly used to signal a list behavior (change to iterate in future?)

  Returns (dict):
    A modified JSON with additional API values added.
    Currently mostly used by dfareporting API to add profile and account.

  Raises:
    ValueError: If a required key in the recipe is missing.
  """

  if api_call['function'].endswith('list') or alias == 'list':
    api_call['iterate'] = True

  if api_call['api'] == 'dfareporting':

    if not api_call['function'].startswith('userProfiles'):
      profile_id = get_profile_for_api(
        config,
        api_call['auth'],
        api_call['kwargs']['id'] if api_call['function'] == 'accounts.get' else api_call['kwargs']['accountId']
      )
      api_call['kwargs']['profileId'] = profile_id


def google_api_build_results(config, auth, api_call, results):
  """Builds the BigQuery table to house the Google API call results.

  Optional piece of the recipe, will create a BigQuery table for results.
  Takes results, which defines a bigquery endpoint, and adds fields.

  Args:
    auth (string): either "user" or "service" to make the BigQuery call.
    api_call (dict): the JSON for the API call as defined in recipe.
    results (dict): defines where the data will be written

  Returns (dict):
    A modified results JSON with additional API values added.

  Raises:
    ValueError: If a required key in the recipe is missing.
  """

  if 'bigquery' in results:

    if 'schema' not in results['bigquery']:
      results['bigquery']['schema'] = Discovery_To_BigQuery(
        api_call['api'],
        api_call['version'],
        api_call.get('key', None),
        api_call.get('labels', None),
      ).method_schema(
        api_call['function'],
        api_call.get('iterate', False)
      )

    if 'auth' not in results['bigquery']:
      results['bigquery']['auth'] = auth

    if 'format' not in results['bigquery']:
      results['bigquery']['format'] = 'JSON'

    results['bigquery']['header'] = False

    BigQuery(
      config,
      results['bigquery']['auth'],
    ).table_create(
      config.project,
      results['bigquery']['dataset'],
      results['bigquery']['table'],
      results['bigquery']['schema'],
      overwrite=False,
      expiration_days=results['bigquery'].get('expiration_days')
    )

  #print('SCHEMA', results['bigquery']['schema'])
  #exit()
  return results


def google_api_append(schema, values, rows):
  """Append columns to the rows containing the kwargs used to call the API.
  Args:
    schema (dict): name of the key to use for the api arguments
    values (dict): the kwargs used to call the API
    rows (list): a list of rows to add the prefix to each one

  Returns (list):
    A generator containing the rows
  """

  for row in rows:
    for s in schema:
      row[s['name']] = values[s['name']]
    yield row


def google_api_execute(config, auth, api_call, results, append=None):
  """Execute the actual API call and write to the end points defined.

  The API call is completely defined at this point.
  The results and error definition is optional.

  Args:
    auth (string): either "user" or "service" to make the API call.
    api_call (dict): the JSON for the API call as defined in recipe.
    results (dict): defines where the data will be written
    append (dict): optional parameters to append to each row, given as BQ schema

  Returns (dict):
    None, all data is transfered between API / BigQuery

  Raises:
    ValueError: If a required key in the recipe is missing.
  """

  rows = API(config, api_call).execute()

  if results:
    # check if single object needs conversion to rows
    if isinstance(rows, dict):
      rows = [rows]

    # check if simple string API results
    elif results.get('bigquery', {}).get('format', 'JSON') == 'CSV':
      rows = [[r] for r in rows]

    if config.verbose:
      print('.', end='', flush=True)

    if append:
      rows = google_api_append(append, api_call['kwargs'], rows)

    yield from map(lambda r: Discovery_To_BigQuery.clean(r), rows)


def google_api(config, log, task):
  """Task handler for recipe, delegates all JSON parameters to functions.

  Executes the following steps:
    1. Define the API call.
    2. Define the results destination.
    3. Define the error destination.

  The results table for BigQuery is created first as blank, this allows
  writes from multiple API calls to aggregate into a single table.

  The API call can be specified via kwargs or kwargs_remote.
    kwargs - hard coded values for the API call as a dictionary.
    kwargs_remote - values loaded from a source such as BigQuery.

  Args:
    None, all parameters are exposed via task.

  Returns:
    None, all data is read and written as a side effect.

  Raises:
    ValueError: If a required key in the recipe is missing.
  """

  if config.verbose:
    print(
      'GOOGLE_API',
      task['api'],
      task['version'],
      task['function']
    )

  api_call = {
    'auth': task['auth'],
    'api': task['api'],
    'version': task['version'],
    'function': task['function'],
    'iterate': task.get('iterate', False),
    'limit': task.get('limit'),
    'key': task.get('key', config.key),
    'labels': task.get('labels'),
    'headers': task.get('headers'),
  }

  result_table = google_api_build_results(
    config,
    task['auth'],
    api_call,
    task.get('results', {})
  )

  if task.get('append'):
    result_table['bigquery']['schema'].extend(task.get('append'))

  # get parameters from JSON
  if 'kwargs' in task:
    kwargs_list = task['kwargs'] if isinstance(
      task['kwargs'], (list, tuple)
    ) else [task['kwargs']]

  # get parameters from remote location ( such as BigQuery )
  elif 'kwargs_remote' in task:
    kwargs_list = get_rows(
      config,
      task['auth'],
      task['kwargs_remote'],
      as_object=True
    )

  # no parameters, ensures at least one call is made
  else:
    kwargs_list = [{}]

  def google_api_combine():
    # loop through paramters and make possibly multiple API calls
    for kwargs in kwargs_list:
      api_call['kwargs'] = kwargs
      google_api_initilaize(config, api_call, task.get('alias'))

      try:
        yield from google_api_execute(
          config,
          task['auth'],
          api_call,
          result_table,
          task.get('append')
        )
    
        log.write(
          'OK',
          task.get('description', '{}.{}.{}@{}'.format(
            task['api'],
            task['version'],
            task['function'],
            task['auth']
          )),
          [{'Key': k, 'Value': str(v) } for k, v in api_call['kwargs'].items()]
        )
      except HttpError as e:
        log.write(
          'ERROR',
          task.get('description', '{}.{}.{}@{}: {}'.format(
            task['api'],
            task['version'],
            task['function'],
            task['auth'],
            str(e)
          )),
          [{'Key': k, 'Value': str(v) } for k, v in api_call['kwargs'].items()]
        )
        if config.verbose:
          traceback.print_exc()
    
  results = put_rows(
    config,
    task['auth'],
    result_table, # may have its own auth
    google_api_combine()
  )

  return results
