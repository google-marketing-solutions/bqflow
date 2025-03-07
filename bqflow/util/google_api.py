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

# TODO: b/331785669 - Functions should be named using lower_snake_case.

"""Thin wrapper around Google Sevice API.

This does not change or augment the standard API calls other than the following:

  - Allows passing of auth parameter to constructor, required for switching.
  - Execute statement is overloaded to include iterator for responses with
  nextPageToken.
  - Retries handle some common errors and have a back off scheme.
  - JSON based configuration allows wrokflow definitions.
  - Pre-defined functions for each API can be added to fix version and uri
  options.
"""

import base64
from collections.abc import Mapping, Sequence
import datetime
import json
import time
from typing import Any, Callable, Union
import ssl

from googleapiclient.errors import HttpError
from googleapiclient.discovery import Resource
import httplib2
from typing_extensions import Self

from bqflow.util.configuration import Configuration
from bqflow.util.auth import get_service

try:
  import httplib
except ModuleNotFoundError as e:
  import http.client as httplib

RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, httplib.NotConnected,
                        httplib.IncompleteRead, httplib.ImproperConnectionState,
                        httplib.CannotSendRequest, httplib.CannotSendHeader,
                        httplib.ResponseNotReady, httplib.BadStatusLine)

RETRIABLE_STATUS_CODES = [500, 502, 503, 504]


def _clean(
  struct: Union[Mapping[str, Any], Sequence[Any]]
) -> Union[Mapping[str, Any], Sequence[Any]]:
  """Helper to recursively clean up JSON data for API call.

  Converts bytes -> base64.
  Converts date -> str (yyyy-mm-dd).

  Args:
    struct: The kwargs being cleaned up.

  Returns:
    The kwargs with replacments.
  """

  if isinstance(struct, list):
    for key, value in struct.items():
      if isinstance(value, bytes):
        struct[key] = base64.standard_b64encode(value).decode("ascii")
      elif isinstance(value, datetime.date):
        struct[key] = str(value)
      else:
        _clean(value)
  elif isinstance(struct, list):
    for index, value in enumerate(struct):
      if isinstance(value, bytes):
        struct[index] = base64.standard_b64encode(value).decode("ascii")
      elif isinstance(value, datetime.date):
        struct[index] = str(value)
      else:
        _clean(value)
  return struct


def API_Retry(
  job: Any,
  key: str = None,
  retries: int = 3,
  wait: int = 31
) -> Any:
  """API retry that includes back off and some common error handling.

  CAUTION:  Total timeout cannot exceed 5 minutes or the SSL token expires for
  all future calls.

  For critical but recoverable errors, the back off executes [retry] times.
  Each time the [wait] is doubled.
  By default retries are: 0:31 + 1:02 + 2:04 = 3:37 (minutes)
  The recommended minimum wait is 60 seconds for most APIs.

  - Errors retried: 429, 500, 503
  - Errors ignored: 409 - already exists (for create only and returns None)
  - Errors raised: ALL OTHERS

  Args:
    job: API call path, everything before the execute() statement to retry.
    key: Optional key from json reponse to return.
    retries: Number of times to try the job.
    wait: Time to wait in seconds between retries.

  Returns:
    JSON result of job or key value from JSON result if job succeed.
    None if object already exists.

  Raises:
    - Any exceptions not listed in comments above.
  """

  try:
    # try to run the job and return the response
    data = job.execute()
    return data if not key else data.get(key, [])

  # API errors
  except HttpError as e:
    # errors that can be overcome or re-tried (403 is rate limit with inspect)
    if e.resp.status in [403, 409, 429, 500, 503]:
      content = json.loads(e.content.decode())
      # already exists (ignore benign)
      if content['error']['code'] == 409:
        return None
      # permission denied (won't change on retry so raise)
      elif (
        content.get('error', {}).get('status') == 'PERMISSION_DENIED'
        or content.get('error', {}).get('errors', [{}])[0].get('reason')
        in ('forbidden', 'accountDisabled')
      ):
        print('ERROR DETAILS:', e.content.decode())
        raise
      elif retries > 0:
        print('API ERROR:', str(e))
        print('API RETRY / WAIT:', retries, wait)
        time.sleep(wait)
        return API_Retry(job, key, retries - 1, wait * 2)
      # if no retries, raise
      else:
        print('ERROR DETAILS:', e.content.decode())
        raise
    # raise all other errors that cannot be overcome
    else:
      raise

  # HTTP transport errors
  except RETRIABLE_EXCEPTIONS as e:
    if retries > 0:
      print('HTTP ERROR:', str(e))
      print('HTTP RETRY / WAIT:', retries, wait)
      time.sleep(wait)
      return API_Retry(job, key, retries - 1, wait * 2)
    else:
      raise

  # SSL timeout errors
  except ssl.SSLError as e:
    # most SSLErrors are not retriable, only timeouts, but
    # SSLError has no good error type attribute, so we search the message
    if retries > 0 and 'timed out' in e.message:
      print('SSL ERROR:', str(e))
      print('SSL RETRY / WAIT:', retries, wait)
      time.sleep(wait)
      return API_Retry(job, key, retries - 1, wait * 2)
    else:
      raise


class API_Iterator_Instance():
  """A helper class that iterates results, automatically called by execute.

  This is a standard python iterator definition, no need to document
  functions.

  The only job this has is to handle Google API iteration, as such it can be
  called on any API call that reurns a 'nextPageToken' in the result.

  For example if calling the DCM list placement API:

    https://developers.google.com/doubleclick-advertisers/v3.4/placements/list

    function = get_service(
      config,
      'dfareporting',
      'v3.4',
      'user'
    ).placements().list
    kwargs = { 'profile_id': 1234, 'archived': False }
    for placement in API_Iterator(function, kwargs):
      print(placement)

  Can be called independently but automatically built into API...execute()
  so use that instead.

  Args:
    function: (function) function of API call to iterate, definiton not
      instance. kwargs arguments to be passed to the function on each fetch
      results optional, the first set of results given (if already fetched)
   kwargs: arguments to pass to fucntion when making call.
   results: optional / used recursively, prior call results to continue.
   limit: maximum number of records to return

  Returns:
    Iterator over JSON objects or Mapping or other depending on API.
  """

  def __init__(
    self,
    function: str,
    kwargs: Mapping[str, Any],
    results: Mapping[str, Any]=None,
    limit: int = None
  ):
    self.function = function
    self.kwargs = kwargs
    self.limit = limit
    self.results = results
    self.position = 0
    self.count = 0
    self.iterable = None
    self.__find_tag__()

  def __find_tag__(self):
    # find the only list item for a paginated response
    # JSON will only have list type, so ok to be specific
    if self.results:  # None and {} both excluded
      for tag in iter(self.results.keys()):
        if isinstance(self.results[tag], list):
          self.iterable = tag
          break

      # this shouldn't happen but some APIs simply omit the key if no results
      if self.iterable is None:
        print(
          'WARNING API RETURNED NO KEYS WITH LISTS:',
          ', '.join(self.results.keys())
        )

  def __iter__(self):
    return self

  def __next__(self):
    return self.next()

  def next(self):

    # if no initial results, get some, empty results {} different
    if self.results is None:
      self.results = API_Retry(self.function(**self.kwargs))
      self.__find_tag__()

    # if empty results or exhausted page, get next page
    if self.iterable and self.position >= len(self.results[self.iterable]):
      page_token = self.results.get('nextPageToken', None)
      if page_token:

        if 'body' in self.kwargs:
          self.kwargs['body']['pageToken'] = page_token
        else:
          self.kwargs['pageToken'] = page_token

        self.results = API_Retry(self.function(**self.kwargs))
        self.position = 0

      else:
        raise StopIteration

    # if results remain, return them (sometimes the iterable is missing)
    if self.iterable and self.position < len(
      self.results.get(self.iterable, [])
    ):
      value = self.results[self.iterable][self.position]
      self.position += 1

      # if reached limit, stop
      if self.limit is not None:
        self.count += 1
        if self.count > self.limit:
          raise StopIteration

      # otherwise return next value
      return value

    # if pages and results exhausted, stop
    else:
      raise StopIteration


def API_Iterator(
  function: Callable,
  kwargs: Mapping[str, Any],
  results: Mapping[str, Any]=None,
  limit: int = None
) -> Any:
  """See API_Iterator_Instance for documentaion, this is an iter wrapper."""

  return iter(API_Iterator_Instance(function, kwargs, results, limit))


class API():
  """A wrapper around Google API with built in helpers.

  The wrapper mimics function calls, storing the m in a stack, until it
  encounters execute(). Then it uses the stored stack and arguments to call the
  actual API. Allows handlers on execute such as API_Retry and API_Iterator.

  See module level description for wrapped changes to Google API.  The class
  is designed to be a JSON connector, hence the configuraton is a JSON object.

  api = {
    "api":"doubleclickbidmanager",
    "version": "v1.1",
    "auth": "user",
    "iterate": False
  }
  api = API(config, api).placements().list(profile_id = 1234,
  archived = False).execute()

  Args:
    config: see example above, configures all authentication parameters
    api: see example above, configures all API parameters

  Returns:
    If nextpageToken in result or iterate is True: return iterator of API
    response
    Otherwise: returns API response
  """

  def __init__(self, config: Configuration, api: Mapping[str, Any]) -> None:
    self.config = config
    self.api = api['api']
    self.version = api['version']
    self.auth = api['auth']
    self.uri = api.get('uri')
    self.key = api.get('key')
    self.labels = api.get('labels')
    self.function_stack = list(filter(None, api.get('function', '').split('.')))
    self.function_kwargs = _clean(api.get('kwargs', {}))
    self.iterate = api.get('iterate', False)
    self.limit = api.get('limit')
    self.headers = api.get('headers', {})

    self.function = None
    self.job = None
    self.response = None

  # for debug purposes
  def __str__(self) -> str:
    return '%s.%s.%s' % (self.api, self.version, '.'.join(self.function_stack))

  # builds API function stack
  def __getattr__(self, function_name: str) -> Self:
    self.function_stack.append(function_name)

    def function_call(**kwargs):
      self.function_kwargs = _clean(kwargs)
      return self

    return function_call

  def call(self, function_chain: str) -> Self:
    """For calling function via string (chain using dot notation).
    """
    for function_name in function_chain.split('.'):
      self.function_stack.append(function_name)
    return self

  # matches API execute with built in iteration and retry handlers
  def execute(
    self,
    run: bool = True,
    iterate: bool = False,
    limit: int = None
  ) -> Any:
    """Executes a method by walking the discovery document and passing parameters.

    The function chain represents the desired method in the discovery document
    to call.  This function crawls it, returns errors along the way, then passes
    parameters to the final endpoint.

    Leverages iteration managers and exception handlers for the user.
    Also handle any iteration or limits passed in on top of the API call.

    Args:
      run: set to false if only the method not the cal is desired. Used for jobs.
      iterate: also passed in the class initialize, used to signal pagination.
      limit: an artificial record stop, used with sorting to reduce long calls

    Returns:
      a dictionary, iterator, or function method depending on inputs

    Raises:
      TypeError: A common error that is thrown when the APi gets the wrong type.
      Many different errors that API_Retry cannot handle.
    """

    # start building call sequence with service object
    self.function = get_service(
      config = self.config,
      api = self.api,
      version = self.version,
      auth = self.auth,
      headers = self.headers,
      key = self.key,
      labels = self.labels,
      uri_file = self.uri
    )

    # build calls along stack
    # do not call functions, the abstract is needed for iterator page next calls
    for f_n in self.function_stack:
      self.function = getattr(
        self.function if isinstance(
          self.function,
          Resource
        ) else self.function(),
        f_n
      )

    # for cases where job is handled manually, save the job
    try:
      self.job = self.function(**self.function_kwargs)
    except TypeError as ex:
      raise TypeError(
        'SEE EXCEPTION ABOVE, ARE YOU MISSING A PARAMETER OR'
        ' PASSING AN ID INTO API AS AN INT INSTEAD OF A STRING?'
      ) from ex

    if run:
      self.response = API_Retry(self.job)

      # if expect to iterate through records
      if iterate or self.iterate:
        return API_Iterator(
          self.function,
          self.function_kwargs,
          self.response,
          limit or self.limit
        )

      # if basic response, return object as is
      else:
        return self.response

    # if not run, just return job object (for chunked upload for example)
    else:
      return self.job

  def upload(self, retries: int = 5, wait: int = 61) -> None:
    """Allows calling upload method instead of execute.

    In some cases the Google API uploads data which requires a special handler.
    This calls execute but returns the job so the call can be chunked.

    Args:
      retries: how many times to attempt on recoverable exceptions.
      wait: how many seconds to wait per retry, doubles every retry

    Returns:
      Nothing, if upload suceeeds no exception is throwm.

    Raises:
      Unrecoverable exceptions or if retries exhausted.
    """

    job = self.execute(run = False)
    response = None

    while response is None:
      error = None

      try:
        print('Uploading file...')
        status, response = job.next_chunk()
        if 'id' in response:
          print("Object id '%s' was successfully uploaded." % response['id'])
        else:
          raise AssertionError(
            f'The upload failed with an unexpected response: {response}.'
          )

      except HttpError as e:
        if retries > 0 and e.resp.status in RETRIABLE_STATUS_CODES:
          error = 'A retriable HTTP error %d occurred:\n%s' % (
              e.resp.status, e.content.decode())
        else:
          raise

      except RETRIABLE_EXCEPTIONS as e:
        if retries > 0:
          error = 'A retriable error occurred: %s' % e
        else:
          raise

      if error is not None:
        print(error)
        retries -= 1
        wait = wait * 2
        print('Sleeping %d seconds and then retrying...' % wait)
        time.sleep(wait)


class _API_DFAReporting(API):
  """Overload class to process special profileId parameter.
  """

  def execute(self, *args: Sequence[Any], **kwargs: Mapping[str, Any]) -> Any:
    """Translate accountId into profileId, so workflows can be profile agnostic.
    """

    if 'accountId' in self.function_kwargs:
      for profile in API_DCM(
        config = self.config,
        auth = self.auth,
        iterate = True
      ).userProfiles().list().execute():
        if profile['accountId'] == str(self.function_kwargs['accountId']):
          self.function_kwargs['profileId'] = profile['profileId']
          del self.function_kwargs['accountId']

    if 'accountId' in self.function_kwargs:
      raise AttributeError(
        f'Add a user profile to DCM account {self.function_kwargs["accountId"]}'
      )

    return super().execute(*args, **kwargs)


def API_Auto(
  config: Configuration,
  api: Mapping[str, Any]
) -> API:
  """This factory helps plug holes in each API by allowing customization.
  """

  if  api['api'] == 'dfareporting':
    return _API_DFAReporting(config, api)
  else:
    return API(config, api)


class API_BigQuery(API):
  """BigQuery helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'bigquery',
        'version': 'v2',
        'auth': auth,
        'iterate': iterate
    })


class API_SecretManager(API):
  """SecretManager helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'secretmanager',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_DBM(API):
  """DBM helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'doubleclickbidmanager',
        'version': 'v2',
        'auth': auth,
        'iterate': iterate
    })


class API_Sheets(API):
  """DBM helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'sheets',
        'version': 'v4',
        'auth': auth,
        'iterate': iterate
    })


class API_Slides(API):
  """Slides helper for Google API.  Defines agreed upon version.
  """

  def __init__(
      self,
      config: Configuration,
      auth: str,
      iterate: bool = False
  ) -> None:
    super().__init__(
        config=config,
        api={
            'api': 'slides',
            'version': 'v1',
            'auth': auth,
            'iterate': iterate
        })


class API_DCM(API):
  """DCM helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'dfareporting',
        'version': 'v4',
        'auth': auth,
        'iterate': iterate
    })


class API_Datastore(API):
  """Datastore helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'datastore',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_StackDriver(API):
  """StackDriver helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'logging',
        'version': 'v2',
        'auth': auth,
        'iterate': iterate
    })


class API_PubSub(API):
  """PubSub helper for Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'pubsub',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_SearchAds(API):
  """Search Ads helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'doubleclicksearch',
        'version': 'v2',
        'auth': auth,
        'iterate': iterate
    })


class API_Analytics(API):
  """Analytics helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'analytics',
        'version': 'v3',
        'auth': auth,
        'iterate': iterate
    })


class API_AnalyticsReporting(API):
  """AnalyticsReporting helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'analyticsreporting',
        'version': 'v4',
        'auth': auth,
        'iterate': iterate
    })


class API_YouTube(API):
  """YouTube helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'youtube',
        'version': 'v3',
        'auth': auth,
        'iterate': iterate
    })


class API_Drive(API):
  """Drive helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'drive',
        'version': 'v3',
        'auth': auth,
        'iterate': iterate
    })


class API_Cloud(API):
  """Cloud project helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'cloudresourcemanager',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_DV360(API):
  """Cloud project helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'displayvideo',
        'version': 'v2',
        'auth': auth,
        'iterate': iterate
    })


class API_Storage(API):
  """Cloud storage helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'storage',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_Gmail(API):
  """Gmail helper Google API.  Defines agreed upon version.
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'gmail',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_Compute(API):
  """Compute helper Google API. Defines agreed upon version.

  https://cloud.google.com/compute/docs/reference/rest/v1/
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'compute',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })


class API_Vision(API):
  """Vision helper Google API. Defines agreed upon version.

  https://cloud.google.com/vision/docs/reference/rest
  """

  def __init__(
    self,
    config: Configuration,
    auth: str,
    iterate: bool = False
  ) -> None:
    super().__init__(
      config = config,
      api = {
        'api': 'vision',
        'version': 'v1',
        'auth': auth,
        'iterate': iterate
    })
