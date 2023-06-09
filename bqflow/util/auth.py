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

import sys
import socket
import threading

from googleapiclient import discovery
from googleapiclient.http import HttpRequest

from bqflow.util.auth_wrapper import CredentialsFlowWrapper
from bqflow.util.auth_wrapper import CredentialsServiceWrapper
from bqflow.util.auth_wrapper import CredentialsUserWrapper

DISCOVERY_CACHE = {}
APIS_WITHOUT_DISCOVERY_DOCS = ('oauth',)

# set timeout to 10 minutes ( reduce socket.timeout: The read operation timed out )
socket.setdefaulttimeout(600)

def get_credentials(config, auth):

  if auth == 'user':
    try:
      return CredentialsUserWrapper(
        config.user,
        config.client,
        config.browserless
      )
    except (KeyError, ValueError) as e:
      print('')
      print('ERROR: You are attempting to access an API endpoiont that requires Google OAuth USER authentication but have not provided credentials to make that possible.')
      print('')
      print('SOLUTION: Specify a -u [user credentials path] parameter on the command line.')
      print('          Alternaitvely specify a -u [user credentials path to be created] parameter and a -c [client credentials path] parameter on the command line.')
      print('          Alternaitvely if running a recipe, include { "setup":{ "auth":{ "user":"[JSON OR PATH]" }}} in the JSON.')
      print('')
      print('Client JSON Parameter Missing:', str(e))
      print('')
      sys.exit(1)

  elif auth == 'service':
    try:
      return CredentialsServiceWrapper(
        config.service
      )
    except (KeyError, ValueError) as e:
      print('')
      print('ERROR: You are attempting to access an API endpoint that requires Google Cloud SERVICE authentication but have not provided credentials to make that possible.')
      print('')
      print('SOLUTION: Specify a -s [service credentials path] parameter on the command line.')
      print('          Alternaitvely if running a recipe, include { "setup":{ "auth":{ "service":"[JSON OR PATH]" }}} in the JSON.')
      print('')
      print('Client JSON Parameter Missing:', str(e))
      print('')
      sys.exit(1)


def get_service(config,
  api='gmail',
  version='v1',
  auth='service',
  scopes=None,
  headers=None,
  key=None,
  labels=None,
  uri_file=None
):
  global DISCOVERY_CACHE

  class HttpRequestCustom(HttpRequest):

    def __init__(self, *args, **kwargs):
      if headers:
        kwargs['headers'].update(headers)
      super(HttpRequestCustom, self).__init__(*args, **kwargs)

  if not key:
    key = config.key

  cache_key = api + version + auth + str(key) + str(threading.current_thread().ident) + config.fingerprint()

  if cache_key not in DISCOVERY_CACHE:
    credentials = get_credentials(config, auth)

    if uri_file:
      uri_file = uri_file.strip()
      if uri_file.startswith('{'):
        DISCOVERY_CACHE[cache_key] = discovery.build_from_document(
          uri_file,
          credentials=credentials,
          developerKey=key,
          requestBuilder=HttpRequestCustom
       )
      else:
        with open(uri_file, 'r') as cache_file:
          DISCOVERY_CACHE[cache_key] = discovery.build_from_document(
            cache_file.read(),
            credentials=credentials,
            developerKey=key,
            requestBuilder=HttpRequestCustom
          )
    else:
      # See: https://github.com/googleapis/google-api-python-client/issues/1225
      try:
        # Enables private API access
        if key or labels and api not in APIS_WITHOUT_DISCOVERY_DOCS:
          uri_template = discovery.V2_DISCOVERY_URI
          if key: uri_template += "&key={}".format(key)
          if labels: uri_template += "&labels={}".format(labels)
        else:
          uri_template = None

        DISCOVERY_CACHE[cache_key] = discovery.build(
          api,
          version,
          credentials=credentials,
          developerKey=key,
          requestBuilder=HttpRequestCustom,
          discoveryServiceUrl=uri_template,
          static_discovery=False
        )
      # PATCH: static_discovery not present in google-api-python-client < 2, default version in colab
      # ALTERNATE WORKAROUND: pip install update google-api-python-client==2.3 --no-deps --force-reinstall
      except TypeError:
        # Enables private API access
        uri_template = discovery.V1_DISCOVERY_URI
        if key: uri_template += "&key={}".format(key)
        if labels: uri_template += "&labels={}".format(labels)

        DISCOVERY_CACHE[cache_key] = discovery.build(
          api,
          version,
          credentials=credentials,
          developerKey=key,
          requestBuilder=HttpRequestCustom,
          discoveryServiceUrl=uri_template
        )

  return DISCOVERY_CACHE[cache_key]


def get_client_type(credentials):
  client_json = CredentialsFlowWrapper(credentials, credentials_only=True)
  return next(iter(client_json.keys()))


def get_profile(config):
  service = get_service(config, 'oauth2', 'v2', 'user')
  return service.userinfo().get().execute()
