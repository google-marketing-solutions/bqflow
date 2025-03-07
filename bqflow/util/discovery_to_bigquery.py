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

"""Transaltes Google API Discovery Documents into BigQuery schema.

See documentation at:
  - https://developers.google.com/discovery/v1/reference/apis
  - https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types

For example:

  print(json.dumps(
    Discovery_To_BigQuery('displayvideo', 'v1').resource_schema('Advertiser'),
    indent = 2
  ))

  print(json.dumps(
    Discovery_To_BigQuery('dfareporting', 'v3.4').method_schema('sites', 'list'),
    indent = 2
  ))
"""

import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from urllib import request


DATETIME_RE = re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}\.?\d+Z')
DESCRIPTION_LENGTH = 1024
RECURSION_DEPTH = 2


def preferred_version(
  api_name: str,
  key: str = None
) -> str:
  """Helper to get default API version.

  Args:
    api_name: the API endpoint name, for example dfareporting.
    key: optional key: https://cloud.google.com/docs/authentication/api-keys

  Returns:
    The API version.

  Raises:
    HttpError: If the wrong API values are specified.
  """

  api_url = 'https://discovery.googleapis.com/discovery/v1/apis?name=%s&key=%s&preferred=true' % (
    api_name,
    key or ''
  )
  api_info = json.load(request.urlopen(api_url))
  return api_info['items'][0]['version']


class Discovery_To_BigQuery():
  """Collection of Discovery to BigQuery operations on a given API version.

  Class is required to maintain a cache between method calls.  The constructor
  sets up the API endpoint, all other calls translate data.
  """

  def __init__(
    self,
    api_name: str,
    api_version: str,
    key: str = None,
    labels: str = None,
    recursion_depth: int = RECURSION_DEPTH
  ) -> None:
    """Initialize the API endpoint.

    Args:
      api_name: the API endpoint name, for example dfareporting.
      api_version: the API endpoint version, for example v3.4.
      key: optional key: https://cloud.google.com/docs/authentication/api-keys
      labels: optional and rearely used to version the discovery document
      recursion_depth: if a schema is recursive, how deep to nest.

    Returns:
      None

    Raises:
      HttpError: If the wrong API values are specified.
    """

    self.key = key or ''
    self.labels = labels or ''
    self.recursion_depth = recursion_depth
    api_url = 'https://%s.googleapis.com/$discovery/rest?version=%s&key=%s&labels=%s' % (
      api_name,
      api_version,
      self.key,
      self.labels
    )
    self.api_document = json.load(request.urlopen(api_url))

  def to_type(
    self,
    entry: Mapping
  ) -> str:
    """Convert a Discovery API Document type to a BigQuery type.

    Called internally but exposed for convenience.

    Args:
      entry: discovery type format: https://developers.google.com/discovery/v1/type-format

    Returns:
      Bigquery type: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    """

    t = entry.get('type')
    f = entry.get('format')

    if t == 'any':
      return 'STRING'
    elif t == 'array':
      return 'REPEATED'
    elif t == 'boolean':
      return 'BOOLEAN'
    elif t == 'integer':
      return 'INT64'
    elif t == 'number':
      if f == 'double':
        return 'FLOAT64'
      else:
        return 'FLOAT'
    elif t == 'object':
      return 'STRUCT'
    elif t == 'string':
      if f == 'byte':
        return 'BYTES'
      elif f == 'date':
        return 'DATE'
      elif f == 'date-time':
        return 'TIMESTAMP'
      elif f == 'int64':
        return 'STRING' #'INT64' because APIs require string ids
      elif f == 'uint64':
        return 'STRING' #'INT64' because APIs require string ids
      else:
        return 'STRING'
    else:
      return 'STRING'

  def to_schema(
    self,
    entry: Mapping,
    parents: Mapping = None
  ) -> Sequence:
    """Convert a Discovery API Document schema to a BigQuery schema.

    Recursively crawls the discovery document reference tree to build schema.
    Leverages recursion depth passed in constructor to stop if necessary.

    Sort the keys to get consitent field order, important for BigQuery DML.

    Args:
      entry: a discovery document schema definition.
      parents: used to track recursion depth for a specific schema branch

    Returns:
      A BigQuery schema object.
    """

    bigquery_schema = []

    if parents is None:
      parents = {}

    for key, value in sorted(entry.items()):

      # when the entry is { "type": "object", "someObject": {..} }, ignores "type"
      if not isinstance(value, Mapping):
        continue

      # struct with ref
      if '$ref' in value:
        parents.setdefault(value['$ref'], 0)
        if parents[value['$ref']] < self.recursion_depth:
          parents[value['$ref']] += 1
          bigquery_schema.append({
            'name': key,
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': self.to_schema(
              self.api_document['schemas'][value['$ref']]['properties'],
              parents
            )
          })
        parents[value['$ref']] -= 1

      # struct embedded
      elif 'properties' in value:
        bigquery_schema.append({
            'name': key,
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': self.to_schema(
                value['properties'],
                parents
            )
        })

      # array embedded
      elif 'items' in value:

        # array with ref
        if '$ref' in value['items']:
          parents.setdefault(value['items']['$ref'], 0)
          if parents[value['items']['$ref']] < self.recursion_depth:
            parents[value['items']['$ref']] += 1
            bigquery_schema.append({
              'name': key,
              'type': 'RECORD',
              'mode': 'REPEATED',
              'fields': self.to_schema(
                self.api_document['schemas'][value['items']['$ref']]
                ['properties'],
                parents
              )
            })
            parents[value['items']['$ref']] -= 1

        # array with struct
        elif value['items']['type'] == 'object':
          bigquery_schema.append({
            'name': key,
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': self.to_schema(
              value['items'],
              parents
            )
          })

        # array with scalar
        else:
          bigquery_schema.append({
            'description': (
              ','.join(value['items'].get('enum', []))
            )[:DESCRIPTION_LENGTH],
            'name': key,
            'type': self.to_type(value['items']),
            'mode': 'REPEATED',
          })

      # scalar
      else:
        if 'additionalProperties' in value:
          print('SKIPPING AMBIGUOUS RECORD:', value)
        else:
          bigquery_schema.append({
            'description': (
              ','.join(value.get('enum', []))
            )[:DESCRIPTION_LENGTH],
            'name': key,
            'type': self.to_type(value),
            'mode': 'NULLABLE'
          })

    return bigquery_schema

  def to_json(
    self,
    from_api: Mapping = None,
    from_json: Mapping = None,
    parents: Mapping = None
  ) -> Mapping:
    """Returns a Discovery API Document schema with all refrences extrapolated.

    Recursively crawls the discovery document reference tree to build document.
    Leverages recursion depth passed in constructor to stop if necessary.

    Sort for consistency with to_schema.

    Args:
      from_api: the api schema to extrapolate
      from_json: new object with references replaced, not passed by caller
      parents: used to track recursion depth for a specific schema branch

    Returns:
      A Discovery API Document schema object.
    """

    if parents is None:
      parents = {}

    if from_api:
      from_json = deepcopy(from_api)

    for key, value in sorted(from_json.items()):

      # when the entry is { "type": "object", "someObject": {..} }, ignores "type"
      if not isinstance(value, Mapping):
        continue

      if '$ref' in value:
        ref = value['$ref']
        parents.setdefault(ref, 0)
        if parents[ref] < self.recursion_depth:
          parents[ref] += 1
          del value['$ref']
          value['type'] = 'dict'
          value['object'] = self.to_json(
            from_api = self.api_document['schemas'][ref]['properties'],
            parents = parents
          )
          parents[ref] -= 1
        else:
          from_json[key] = None

      else:
        self.to_json(from_json = value, parents = parents)

    return from_json

  def to_struct(
    self,
    from_api: Mapping = None,
    from_json: Mapping = None,
    indent: int = 2
  ) -> str:
    """Translates a Discovery API Document schema to a BigQuery STRUCT.

    Recursively crawls the discovery document reference tree to build struct.
    Leverages recursion depth passed in constructor to stop if necessary.

    Sort for consistency with to_schema.

    Args:
      from_api: the api schema to extrapolate
      from_json: new object with references replaced, not passed by caller
      parents: used to track recursion depth for a specific schema branch

    Returns:
      A BigQuery STRUCT object that can be pasted into a query.
    """

    if from_api:
      from_json = self.to_json(from_api = from_api)

    fields = []
    spaces = ' ' * indent

    for key, value in sorted(from_json.items()):

      # when the entry is { "type": "object", "someObject": {..} }, ignores "type"
      if not isinstance(value, Mapping):
        continue

      if value['type'] == 'dict':
        fields.append('%sSTRUCT(\n%s\n%s) AS %s' % (
          spaces,
          self.to_struct(from_json = value['object'], indent = indent + 2),
          spaces,
          key
        ))
      elif value['type'] == 'array':
        if 'enum' in value['items']:
          fields.append('%s[%s\n%s] AS %s' % (
            spaces,
            'STRING',
            spaces,
            key
          ))
        else:
          fields.append('%s[STRUCT(\n%s\n%s)] AS %s' % (
            spaces,
            self.to_struct(from_json = value['items'], indent = indent + 2),
            spaces,
            key
          ))
      else:
        fields.append('%sCAST(NULL AS %s) AS %s' % (spaces, value['type'].upper(), key))

    return ',\n'.join(fields)

  def resource_json(
    self,
    resource: str
  ) -> Mapping:
    """Return Discovery API Document json for a resource.

    Expands all the references.

    Args:
      resource: the name of the Google API resource

    Returns:
      A dictionary representation of the resource.
    """

    resource = self.api_document['schemas'][resource]['properties']
    return self.to_json(from_api = resource)

  def resource_schema(
    self,
    resource: str
  ) -> Mapping:
    """Return BigQuery schema for a Discovery API resource.

    Args:
      resource: the name of the Google API resource

    Returns:
      A dictionary representation of the resource.
    """

    entry = self.api_document['schemas'][resource]['properties']
    return self.to_schema(entry)

  def resource_struct(
    self,
    resource: str
  ) -> str:
    """Return BigQuery STRUCT for a Discovery API resource.

    Args:
      resource: the name of the Google API resource

    Returns:
      A string STRUCT of the resource ready to be used in a query.
    """

    resource = self.api_document['schemas'][resource]['properties']
    return self.to_struct(from_api = resource)

  def method_schema(
    self,
    method: str,
    iterate: bool = False
  ) -> Mapping:
    """Return BigQuery schema for a Discovery API function.

    Use the full dot notation of the rest API function.

    Args:
      method: the dot notation name of the Google API function
      iterate: if true, return only iterable schema

    Returns:
      A dictionary representation of the resource.
    """

    endpoint, method = method.rsplit('.', 1)
    resource = self.api_document

    for e in endpoint.split('.'):
      resource = resource['resources'][e]
    resource = resource['methods'][method]['response']['$ref']

    # get schema
    properties = self.api_document['schemas'][resource]['properties']
    schema = self.to_schema(properties)

    # List responses wrap their items in a paginated response object
    # Unroll it to return item schema instead of repsonse schema
    if iterate or ('List' in resource and resource.endswith('Response')):
      for entry in schema:
        if entry['type'] == 'RECORD':
          return entry['fields']
        elif entry['mode'] == 'REPEATED':
          entry['mode'] = 'NULLABLE'
          return [entry]
      # raise exception after checking all fields for a list
      raise AttributeError('Unahandled discovery schema.')
    else:
      return schema
