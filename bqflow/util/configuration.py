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

"""The global variable class of BQFlow.

Project loads JSON and parameters and combines them for execturion.  It handles
three important concepts:

  1. Load the JSON and make all task parameters available to python scripts.
  2. Load authentication, all three parameters are optional if scripts do not
     use them.  The following parameters can be passed for authentication.

    user.json - user credentials json ( generated from client ), is refreshed
                by BQFlow as required.  Can be provided as a local path
                or a Cloud Bucket Storage path for distributed jobs.

    service.json - service credentials json ( generated from cloud project ).
                   Passed as a local path or an embedded json object for
                   distributed jobs.

    client.json - client credentials json ( generated from cloud project ).
                  Also require a user json path which will be written to after
                  client authnetication.  Once authenticated this client is not
                  required.

    Credentials can be specified in one of three ways for maximum flexibility:

    A. Specify credentials on command line (highest priority if given)
       --user / -u = user credentials path
       --client / -c = client credentials path (requires user credentials path)
       --service / -s = service credentials path

    B. Use default credentials, these must be specified for security reasons:
       --service / -s = "DEFAULT"

"""

import os
import json
import hashlib
import datetime

# handle python 3.8-3.9 transition
try:
  from zoneinfo import ZoneInfo
except:
  from pytz import timezone as ZoneInfo

class Configuration():

  def __init__(
    self,
    project=None,
    service=None,
    client=None,
    user=None,
    key=None,
    timezone='America/Los_Angeles',
    verbose=False
  ):
    """Used in BQFlow scripts as programmatic entry point.

    Args:
      * project: (string) See module description.
      * service: (string) See module description.
      * client: (string) See module description.
      * user: (string) See module description.
      * key: (string) See module description.
      * verbose: (boolean) See module description.
      * timezone: (string) See module description.
      * args: (dict) dictionary of arguments (used with argParse).

    Returns:
      Nothing.
    """

    self.project = project
    self.service = service
    self.client = client
    self.user = user
    self.verbose = verbose
    self.key = key

    self.days = []
    self.hours = []

    self.timezone = ZoneInfo(timezone)
    self.now = datetime.datetime.now(self.timezone)
    self.date = self.now.date()
    self.hour = self.now.hour

    if self.verbose:
      print('DATE:', self.now.date())
      print('HOUR:', self.now.hour)


  def auth_options(self):
    if self.user and self.service:
      return 'BOTH'
    elif self.user:
      return 'USER'
    elif self.service:
      return 'SERVICE'
    else:
      return 'NONE'


  def fingerprint(self):
    """Provide value that can be used as a cache key.
    """

    h = hashlib.sha256()
    h.update(json.dumps(self.project).encode())
    return h.hexdigest()
