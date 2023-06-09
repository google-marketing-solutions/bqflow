#!/usr/bin/env python3

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

import json
import argparse
import textwrap

from bqflow.util.auth import get_profile
from bqflow.util.configuration import Configuration


def main():

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
    Creates USER credentials from Google Cloud Project CLIENT Credentials and displays profile information if it worked.
    CLIENT credentials are required to run this script, to obtain the JSON file...

      Step 1: Configure Authentication Consent Screen ( do only once )
      ----------------------------------------
        A. Visit: https://console.developers.google.com/apis/credentials/consent
        B. Choose Internal if you have GSuite, otherwise choose External.
        C. For Application Name enter: BQFlow
        D. All other fields are optional, click Save.

      Step 2: Create CLIENT Credentials ( do only once )
      ----------------------------------------
        A. Visit: https://console.developers.google.com/apis/credentials/oauthclient
        B. Choose Desktop.
        C. For Name enter: BQFlow.
        D. Click Create and ignore the confirmation pop-up.

      Step 3: Download CLIENT Credentials File ( do only once )"
      ----------------------------------------"
        A. Visit: https://console.developers.google.com/apis/credentials"
        B. Find your newly created key under OAuth 2.0 Client IDs and click download arrow on the right."
        C. The downloaded file is the CLIENT credentials, use its path for the --client -c parameter.

      Step 4: Run Auth Workflow To Get User Credentials File ( do only once )"
      ----------------------------------------"
        A. Run this command with parameters, see Examples below.
        B. The user.json file will be created and can be used to access Google APIs.
        C. The user profile will be printed to the screen.

        Note:
          DOES NOT WORK IN A VM because of the Google Authentication Flow, do this from your local machine.

        Examples: 
          python bqflow/auth.py -c [CLIENT file path, you have this] -u [USER file path, file will be created]
          python bqflow/auth.py -c client.json  and -u user.json

        All scopes are controlled by: bqflow/bqflow/config.py

  """))

  # initialize project
  parser.add_argument(
    '--client',
    '-c',
    help='Path to CLIENT credentials json file.',
    default=None
  )

  parser.add_argument(
    '--user',
    '-u',
     help='Path to USER credentials json file.',
    default=None
  )

  parser.add_argument(
    '--browserless',
    '-b',
    action='store_true',
     help='Run the authentication without access to a local browser.',
    default=False
  )

  args = parser.parse_args()
  config = Configuration(
    user=args.user,
    client=args.client,
    browserless=args.browserless
  )

  # get profile to verify everything worked
  print('Profile:', json.dumps(get_profile(config), indent=2, sort_keys=True))


if __name__ == '__main__':
  main()
