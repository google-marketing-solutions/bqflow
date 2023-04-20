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

# https://developers.google.com/drive/api/v3/manage-uploads
# https://developers.google.com/drive/api/v3/reference/about#methods

import re
import mimetypes
from io import BytesIO
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

from util.google_api import API_Drive
from util import misc


DRIVE_CHUNKSIZE = misc.memory_scale(maximum=200 * 1024**3, multiple=256 * 1024)


class Drive():
  '''Implement file handling helpers, mainly lookup by name instead of id.

   These are helpers to reduce error handling and file searching overhead.

   ```
   drive = Drive(config, task['auth'])
   file = drive.file_find(**task['kwargs'])
   ```

  Handles all serialization, pagination, and type convesion.

  '''

  def __init__(self, config, auth:str) -> None:
    '''Construct a drive factory, providing project and authentication data.

    Args:
     config, required - see: util/configuration.py
     auth, required - either "user" or "service" used to create and/or read the report.

    Returns: None

  '''
    self.config = config
    self.auth = auth


  def about(self, fields='importFormats'):
    return API_Drive(self.config, self.auth).about().get(fields=fields).execute()


  def file_id(self, url_or_name):

    if url_or_name.startswith('https://drive.google.com/open?id='):
      return url_or_name.split('?id=', 1)[-1]

    elif url_or_name.startswith('https://docs.google.com/'):
      m = re.search(
          '^(?:https:\/\/docs.google.com\/\w+\/d\/)([a-zA-Z0-9-_]+)(?:\/.*)?$',
          url_or_name)
      if m:
        return m.group(1)

    elif url_or_name.startswith('https://datastudio.google.com/'):
      m = re.search(
          '^(?:https:\/\/datastudio.google.com\/c\/\w+\/)([a-zA-Z0-9-_]+)(?:\/.*)?$',
          url_or_name)
      if m:
        return m.group(1)
  
    # check if name given convert to ID "Some Document"
    else:
      document = self.file_find(url_or_name)
      if document:
        return document['id']
  
        # check if just ID given, "1uN9tnb-DZ9zZflZsoW4_34sf34tw3ff"
      else:
        m = re.search('^([a-zA-Z0-9-_]+)$', url_or_name)
        if m:
          return m.group(1)
  
    # probably a mangled id or name does not exist
    if config.verbose:
      print('DOCUMENT DOES NOT EXIST', url_or_name)
    return None
  
  
  def file_get(self, drive_id):
    return API_Drive(self.config, self.auth).files().get(fileId=drive_id).execute()
  
  
  def file_exists(self, name):
    drive_id = self.file_id(name)
    if drive_id:
      try:
        API_Drive(self.config, self.auth).files().get(fileId=drive_id).execute()
        return True
      except HttpError:
        return False
    return False
  
  
  def file_list(self, parent=None):
    query = "trashed = false"
    if parent:
      query = "%s and '%s' in parents" % (query, parent)
    yield from API_Drive(self.config, self.auth, iterate=True).files().list(q=query).execute()
  
  
  def file_find(self, name, parent=None):
    query = "trashed = false and name = '%s'" % name
    if parent:
      query = "%s and '%s' in parents" % (query, parent)
  
    try:
      return next(API_Drive(self.config, self.auth, iterate=True).files().list(q=query).execute())
    except StopIteration:
      return None
  
  
  def file_delete(self, name, parent=None):
    drive_id = self.file_id(name)
  
    if drive_id:
      API_Drive(self.config, self.auth).files().delete(fileId=drive_id).execute()
      return True
    else:
      return False
  
  
  def file_create(self, name, filename, data, parent=None):
    """ Checks if file with name already exists ( outside of trash ) and
  
      if not, uploads the file.  Determines filetype based on filename extension
      and attempts to map to Google native such as Docs, Sheets, Slides, etc...
  
      For example:
      -  ```file_create('user', 'Sample Document', 'sample.txt', BytesIO('File
      contents'))```
      -  Creates a Google Document object in the user's drive.
  
      -  ```file_Create('user', 'Sample Sheet', 'sample.csv',
      BytesIO('col1,col2\nrow1a,row1b\n'))````
      -  Creates a Google Sheet object in the user's drive.
  
      See: https://developers.google.com/drive/api/v3/manage-uploads
  
      ### Args:
      -  * name: (string) name of file to create, used as key to check if file
      exists
      -  * filename: ( string) specified as "file.extension" only to automate
      detection of mime type.
      -  * data: (BytesIO) any file like object that can be read from
      -  * parent: (string) the Google Drive to upload the file to
  
      ### Returns:
      -  * JSON specification of file created or existing.
  
      """
  
    # attempt to find the file by name ( not in trash )
    drive_file = self.file_find(name, parent)
  
    # if file exists, return it, prevents obliterating user changes
    if drive_file:
      if config.verbose:
        print('Drive: File exists.')
  
    # if file does not exist, create it
    else:
      if config.verbose:
        print('Drive: Creating file.')
  
      # file mime is used for uplaod / fallback
      # drive mime attempts to map to a native Google format
      file_mime = mimetypes.guess_type(filename, strict=False)[0]
      drive_mime = self.about('importFormats')['importFormats'].get(
        file_mime, file_mime
      )[0]
  
      if config.verbose:
        print('Drive Mimes:', file_mime, drive_mime)
  
      # construct upload object, and stream upload in chunks
      body = {
        'name': name,
        'parents': [parent] if parent else [],
        'mimeType': drive_mime,
      }
  
      media = MediaIoBaseUpload(
        BytesIO(data or ' '),  # if data is empty BAD REQUEST error occurs
        mimetype=file_mime,
        chunksize=DRIVE_CHUNKSIZE,
        resumable=True
      )
  
      drive_file = API_Drive(self.config, self.auth).files().create(
        body=body,
        media_body=media,
        fields='id'
      ).execute()
  
    return drive_file
  
  
  def file_copy(self, source_name, destination_name):
    destination_id = self.file_id(destination_name)
  
    if destination_id:
      if config.verbose:
        print('Drive: File exists.')
      return self.file_get(destination_id)
  
    else:
      source_id = self.file_id(source_name)
  
      if source_id:
        body = {'visibility': 'PRIVATE', 'name': destination_name}
        return API_Drive(self.config, self.auth).files().copy(fileId=source_id, body=body).execute()
      else:
        return None
  
  
  def folder_create(self, name, parent=None):
    body = {
        'name': name,
        'parents': [parent] if parent else [],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    return API_Drive(self.config, self.auth).files().create(body=body, fields='id').execute()
