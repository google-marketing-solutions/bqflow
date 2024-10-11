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

"""Developer tool for examining Google API endpoints."""


import argparse
import json
import pprint
import textwrap
from typing import Union

from bqflow.util.configuration import Configuration
from bqflow.util.discovery_to_bigquery import Discovery_To_BigQuery
from bqflow.util.google_api import API


def flatten_json(
    data: Union[dict[str, dict], list[dict]],
    prefix: str = '',
    flattened: Union[list[str], None] = None,
) -> list[str]:
  """Flattens a nested JSON structure into a single-level dictionary.

  Args:
      data: The JSON data to flatten (can be a dictionary or list).
      prefix: Optional used to build flattened keys (starts as empty).
      flattened: Optional list for flattened keys (used for recursion).

  Returns:
      A flattened dictionary where keys represent nested paths, and values are
      the corresponding data.
  """

  if flattened is None:
    flattened = []

  if isinstance(data, dict):
    for key, value in data.items():
      if isinstance(value, dict):
        if key == 'object':
          flatten_json(value, prefix, flattened)
        else:
          new_prefix = f'{prefix}.{key}' if prefix else key
          flattened.append(new_prefix)
          flatten_json(value, new_prefix, flattened)
  elif isinstance(data, list):
    for i, value in enumerate(data):
      new_prefix = f'{prefix}[{i}]'
      flattened.append(new_prefix)
      flatten_json(value, new_prefix, flattened)

  return flattened


def main():

  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      description=textwrap.dedent("""\
      Command line interface for running Google API calls.  Any API works.
      Allows developers to quickly test and debug API calls before building them
      into scripts.  Useful for debugging permission or call errors.

      Examples:
        - Pull a DBM report via API.
          - https://developers.google.com/bid-manager/v1/queries/getquery
          - python api.py -api doubleclickbidmanager -version v1 -function queries.getquery -kwargs '{ "queryId": 132865172 }' -u [credentials path]

        - Pull a list of placements:
          - https://developers.google.com/doubleclick-advertisers/v3.3/placements/list
          - python api.py -api dfareporting -version v3.3 -function placements.list -kwargs '{ "profileId":2782211 }' -u [credentials path]

        - Show schema for Campaign Manager advertiser list endpoint.
          - https://developers.google.com/doubleclick-advertisers/v4/advertisers/list
          - python api.py -api dfareporting -version v4 -function advertisers.list --schema
          - python api.py -api dfareporting -version v4 -function Advertiser --object
          - python api.py -api dfareporting -version v4 -function Advertiser --struct

        - Show schema for Advertiser object.
          - python api.py -api dfareporting -version v4 -function Advertiser --object

  """),
  )

  # get parameters
  parser.add_argument('-api', help='api to run, name of product api')
  parser.add_argument('-version', help='version of api')
  parser.add_argument('-function', help='function or resource to call in api')
  parser.add_argument('-uri', help='uri to use in api', default=None)
  parser.add_argument(
      '-developer-token', help='developer token to pass in header', default=None
  )
  parser.add_argument(
      '-login-customer-id',
      help='customer to log in with when manipulating an MCC',
      default=None,
  )
  parser.add_argument(
      '-kwargs',
      help='kwargs to pass to function, json string of name:value pairs',
  )

  parser.add_argument('--iterate', help='force iteration', action='store_true')
  parser.add_argument(
      '--limit',
      type=int,
      help='optional, number of records to return',
      default=None,
  )
  parser.add_argument(
      '--schema',
      help='return function as BigQuery schema, function = [endpoint.method]',
      action='store_true',
  )
  parser.add_argument(
      '--object',
      help='return resource as JSON discovery document, function = [resource]',
      action='store_true',
  )
  parser.add_argument(
      '--flatten',
      help='return resource as JSON discovery document, function = [resource]',
      action='store_true',
  )
  parser.add_argument(
      '--struct',
      help='return resource as BigQuery structure, function = [resource]',
      action='store_true',
  )

  parser.add_argument(
      '--key', '-k', help='API Key of Google Cloud Project.', default=None
  )
  parser.add_argument(
      '--service',
      '-s',
      help='Path to SERVICE credentials json file.',
      default=None,
  )
  parser.add_argument(
      '--client',
      '-c',
      help='Path to CLIENT credentials json file.',
      default=None,
  )
  parser.add_argument(
      '--user', '-u', help='Path to USER credentials json file.', default=None
  )
  parser.add_argument(
      '--timezone',
      '-tz',
      help='Time zone to run schedules on.',
      default='America/Los_Angeles',
  )
  parser.add_argument(
      '--verbose',
      '-v',
      help='Print all the steps as they happen.',
      action='store_true',
  )

  args = parser.parse_args()
  config = Configuration(
      user=args.user,
      client=args.client,
      service=args.service,
      key=args.key,
      verbose=args.verbose,
  )

  # show schema
  if args.object:
    print(
        json.dumps(
            Discovery_To_BigQuery(args.api, args.version).resource_json(
                args.function
            ),
            indent=2,
            default=str,
        )
    )

  elif args.flatten:
    print(
        '\n'.join(
            flatten_json(
                Discovery_To_BigQuery(args.api, args.version).resource_json(
                    args.function
                )
            )
        )
    )

  elif args.struct:
    print(
        Discovery_To_BigQuery(args.api, args.version).resource_struct(
            args.function
        )
    )

  # show schema
  elif args.schema:
    print(
        json.dumps(
            Discovery_To_BigQuery(args.api, args.version).method_schema(
                args.function
            ),
            indent=2,
            default=str,
        )
    )

  # or fetch results
  else:

    # the api wrapper takes parameters as JSON
    job = {
        'auth': 'service' if args.service else 'user',
        'api': args.api,
        'version': args.version,
        'function': args.function,
        'key': args.key,
        'uri': args.uri,
        'kwargs': json.loads(args.kwargs),
        'headers': {},
        'iterate': args.iterate,
        'limit': args.limit,
    }

    if args.developer_token:
      job['headers']['developer-token'] = args.developer_token

    if args.login_customer_id:
      job['headers']['login-customer-id'] = args.login_customer_id

    # run the API call
    results = API(config, job).execute()

    # display results
    if args.iterate:
      for result in results:
        pprint.PrettyPrinter().pprint(result)
    else:
      pprint.PrettyPrinter().pprint(results)


if __name__ == '__main__':
  main()
