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

import os

# replace with default credentials in future release
UI_PROJECT = os.environ.get('BQFLOW_PROJECT', 'Missing, optional, set on ENV, used to fetch remote credentials.')
UI_SERVICE = os.environ.get('BQFLOW_SERVICE', 'Missing, optional, set on ENV, used to fetch remote credentials.')

# used for user authentication
APPLICATION_NAME = 'BQFlow Client'
APPLICATION_SCOPES = [
    'https://www.googleapis.com/auth/userinfo.profile',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/doubleclickbidmanager',
    'https://www.googleapis.com/auth/cloudplatformprojects',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/doubleclicksearch',
    'https://www.googleapis.com/auth/content',
    'https://www.googleapis.com/auth/ddmconversions',
    'https://www.googleapis.com/auth/datastore',
    'https://www.googleapis.com/auth/logging.write',
    'https://www.googleapis.com/auth/logging.read',
    'https://www.googleapis.com/auth/pubsub',
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/analytics',
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/display-video',
    'https://www.googleapis.com/auth/adwords',
    'https://www.googleapis.com/auth/adsdatahub',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/cloud-vision',
]
