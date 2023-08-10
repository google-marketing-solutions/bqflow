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

# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets

import re

from googleapiclient.errors import HttpError

from bqflow.util.google_api import API_Sheets
from bqflow.util.drive import Drive


class Sheets():
  
  def __init__(self, config, auth):
    '''Add configuration and authentication variables.
  
    Args:
      config - see util/configuration.py
      auth - user or service
    '''

    self.config = config
    self.auth = auth

  def sheet_id(self, url_or_name):
    '''Pull sheet id from URL, name, or id itself
  
    Args:
      url_or_name - one of: URL, document title, or id
  
    Returns:
      String containing document id used by API calls.
    '''
  
    # check if URL given, convert to ID "https://docs.google.com/spreadsheets/d/1uN9tnb-DZ9zZflZsoW4_34sf34tw3ff/edit#gid=4715"
    if url_or_name.startswith('https://docs.google.com/spreadsheets/d/'):
      m = re.search(
          '^(?:https:\/\/docs.google.com\/spreadsheets\/d\/)?([a-zA-Z0-9-_]+)(?:\/.*)?$',
          url_or_name)
      if m:
        return m.group(1)
  
    # check if name given convert to ID "Some Document"
    else:
      sheet = Drive(self.config, self.auth).file_find(url_or_name)
      if sheet:
        return sheet['id']
  
        # check if just ID given, "1uN9tnb-DZ9zZflZsoW4_34sf34tw3ff"
      else:
        m = re.search('^([a-zA-Z0-9-_]+)$', url_or_name)
        if m:
          return m.group(1)
  
    # probably a mangled id or name does not exist
    if self.config.verbose:
      print('SHEET DOES NOT EXIST', url_or_name)
    return None
  
  
  def sheet_url(self, url_or_name):
    '''Normalizes a full sheet URL from some key.
  
    Args:
      url_or_name - one of: URL, document title, or id
  
    Returns:
      URL of sheet.
    '''
  
    return 'https://docs.google.com/spreadsheets/d/%s/' % self.sheet_id(url_or_name)
  
  
  def tab_range(self, sheet_tab, sheet_range):
    '''Helper for creating range format.
  
    Args:
      sheet_tab - name of tab in sheet
      sheet_range - A1 notation
  
    Returns:
      String containing full sheet range specification.
    '''
  
    if sheet_range:
      return '%s!%s' % (sheet_tab, sheet_range)
    else:
      return sheet_tab
  
  
  def sheet_get(self, sheet_url_or_name):
    '''Get sheets definition.
  
    Args:
      sheet_url_or_name - one of: URL, document title, or id
  
    Returns:
      Dictionary with all sheets information from Rest API.
    '''
  
    sheet_id = self.sheet_id(sheet_url_or_name)
    if sheet_id:
      return API_Sheets(self.config, self.auth).spreadsheets().get(
        spreadsheetId=sheet_id
      ).execute()
    else:
      return None
  
  
  def tab_id(self, sheet_url_or_name, sheet_tab):
    '''Pull sheet tab id from URL, name, or id itself.
  
    Args:
      url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
  
    Returns:
      Pair of sheet id and tab id.
    '''
  
    sheet_id = None
    tab_id = None
    spreadsheet = self.sheet_get(sheet_url_or_name)
    if spreadsheet:
      sheet_id = spreadsheet['spreadsheetId']
      for tab in spreadsheet.get('sheets', []):
        if tab['properties']['title'] == sheet_tab:
          tab_id = tab['properties']['sheetId']
          break
    return sheet_id, tab_id
  
  
  def tab_read(self, sheet_url_or_name, sheet_tab, sheet_range=''):
    '''Pull sheet id from URL, name, or id itself
  
    Args:
      sheet_url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
      sheet_range - A1 notation or blank if whole sheet
  
    No Return
    '''
    if self.config.verbose:
      print('SHEETS READ', sheet_url_or_name, sheet_tab, sheet_range)
    sheet_id = self.sheet_id(sheet_url_or_name)
    if sheet_id:
      return API_Sheets(self.config, self.auth).spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=self.tab_range(sheet_tab, sheet_range)
      ).execute().get('values')
    else:
      raise ValueError('Sheet does not exist for %s: %s' % (self.config, self.auth, sheet_url_or_name))
  
  
  # TIP: Specify sheet_range as 'Tab!A1' coordinate, the API will figure out length and height based on data
  def tab_write(self, sheet_url_or_name, sheet_tab, sheet_range, rows, append=False, valueInputOption='RAW'):
    '''Write to sheets for specified range.
  
    Args:
      sheet_url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
      sheet_range - A1 notation or blank if whole sheet
      rows - list of lists representing rows.
      append - if true, rows will be added after last row with data.
      valueInputOption - see APi docs.
  
    No Return
    '''
  
    if self.config.verbose:
      print('SHEETS WRITE', sheet_url_or_name, sheet_tab, sheet_range)
    sheet_id = self.sheet_id(sheet_url_or_name)
    tab_range = self.tab_range(sheet_tab, sheet_range)
    body = {'values': list(rows)}
  
    if append:
      API_Sheets(self.config, self.auth).spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=tab_range,
        body=body,
        valueInputOption=valueInputOption,
        insertDataOption='OVERWRITE'
      ).execute()
    else:
      API_Sheets(self.config, self.auth).spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range,
        body=body,
        valueInputOption=valueInputOption
      ).execute()
  
  
  def tab_clear(self, sheet_url_or_name, sheet_tab, sheet_range):
    '''Clear a sheet in the specified range.
  
    Args:
      url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
      sheet_range - A1 notation or blank if whole sheet
  
    No Return
    '''
  
    if self.config.verbose:
      print('SHEETS CLEAR', sheet_url_or_name, sheet_tab, sheet_range)
    sheet_id = self.sheet_id(sheet_url_or_name)
    if sheet_id:
      API_Sheets(self.config, self.auth).spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=self.tab_range(sheet_tab, sheet_range),
        body={}
      ).execute()
    else:
      raise ValueError('Sheet does not exist for %s: %s' % (self.config, self.auth, sheet_url_or_name))
  
  
  def tab_copy(self, from_sheet_url_or_name, from_sheet_tab, to_sheet_url_or_name, to_sheet_tab, overwrite=False):
    '''Copy a tab to a specific name, without the 'Copy Of...'.
  
    Args:
      from_url_or_name - one of: URL, document title, or id
      from_sheet_tab - name of tab to get id for
      to_url_or_name - one of: URL, document title, or id
      to_sheet_tab - name of tab to get id for
      overwrite - if false will not perform operation if destination exists.
  
    No Return
    '''
  
    if self.config.verbose:
      print('SHEETS COPY', from_sheet_url_or_name, from_sheet_tab,
            to_sheet_url_or_name, to_sheet_tab)
  
    # convert human readable to ids
    from_sheet_id, from_tab_id = self.tab_id(from_sheet_url_or_name, from_sheet_tab)
    to_sheet_id, to_tab_id = self.tab_id(to_sheet_url_or_name, to_sheet_tab)
  
    # overwrite only if does not exist
    if overwrite or to_tab_id is None:
  
      # copy tab between sheets, the name changes to be "Copy of [from_sheet_tab]"
      copy_sheet = API_Sheets(self.config, self.auth).spreadsheets().sheets().copyTo(
        spreadsheetId=from_sheet_id,
        sheetId=from_tab_id,
        body={
          'destinationSpreadsheetId': to_sheet_id,
        }
      ).execute()
  
      body = {'requests': []}
  
      # if destination tab exists, delete it
      if to_tab_id:
        body['requests'].append({'deleteSheet': {'sheetId': to_tab_id}})
  
      # change the copy name to the designated name, remove "Copy of "
      body['requests'].append({
          'updateSheetProperties': {
              'properties': {
                  'sheetId': copy_sheet['sheetId'],
                  'title': to_sheet_tab
              },
              'fields': 'title'
          }
      })
  
      API_Sheets(self.config, self.auth).spreadsheets().batchUpdate(
        spreadsheetId=to_sheet_id,
        body=body
      ).execute()
  
  
  def batch_update(self, sheet_url_or_name, data):
    '''Helper for performing batch operations.
  
    Args:
      sheet_url_or_name - one of: URL, document title, or id
      data - JSON data for sending to batch request
  
    No Return
    '''
  
    sheet_id = self.sheet_id(sheet_url_or_name)
    API_Sheets(self.config, self.auth).spreadsheets().batchUpdate(
      spreadsheetId=sheet_id,
      body=data
    ).execute()
  
  
  def values_batch_update(self, sheet_url_or_name, data):
    '''Helper for performing batch value operations.
  
    Args:
      sheet_url_or_name - one of: URL, document title, or id
      data - JSON data for sending to batch request
  
    No Return
    '''
  
    sheet_id = self.sheet_id(sheet_url_or_name)
    API_Sheets(self.config, self.auth).spreadsheets().values().batchUpdate(
      spreadsheetId=sheet_id,
      body=data
    ).execute()
  
  
  def tab_create(self, sheet_url_or_name, sheet_tab):
    '''Create a tab in a sheet.
  
    Args:
      url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
  
    No Return
    '''
  
    sheet_id, tab_id = self.tab_id(sheet_url_or_name, sheet_tab)
    if tab_id is None:
      self.batch_update(
          sheet_url_or_name,
          {'requests': [{
              'addSheet': {
                  'properties': {
                      'title': sheet_tab
                  }
              }
          }]})
  
  
  def tab_delete(self, sheet_url_or_name, sheet_tab):
    '''Delete a tab in a sheet.
  
    Args:
      url_or_name - one of: URL, document title, or id
      sheet_tab - name of tab to get id for
  
    No Return
    '''
  
    if self.config.verbose:
      print('SHEETS DELETE', sheet_url_or_name, sheet_tab)
  
    spreadsheet = self.sheet_get(sheet_url_or_name)
    if spreadsheet:
      if len(
          spreadsheet['sheets']
      ) == 1 and spreadsheet['sheets'][0]['properties']['title'] == sheet_tab:
        Drive(self.config, self.auth).file_delete(spreadsheet['properties']['title'], parent=None)
      else:
        sheet_id, tab_id = self.tab_id(sheet_url_or_name, sheet_tab)
        # add check to see if only tab, then delete whole sheet
        if tab_id is not None:
          self.batch_update(
              sheet_url_or_name,
              {'requests': [{
                  'deleteSheet': {
                      'sheetId': tab_id,
                  }
              }]})
  
  
  def tab_rename(self, sheet_url_or_name, old_sheet_tab, new_sheet_tab):
    '''Rename a tab in a sheet.
  
    Args:
      url_or_name - one of: URL, document title, or id
      old_sheet_tab - name of tab to get id for
      new_sheet_tab - name of tab to get id for
  
    No Return
    '''
  
    sheet_id, tab_id = self.tab_id(sheet_url_or_name, old_sheet_tab)
    if tab_id is not None:
      self.batch_update(
          sheet_url_or_name,
          { 'requests': [{
                  'updateSheetProperties': {
                      'properties': {
                          'sheetId': tab_id,
                          'title': new_sheet_tab
                      },
                      'fields': 'title'
                  }
              }]
          })
  
  
  def sheet_create(self, sheet_name, sheet_tab, template_sheet=None, template_tab=None):
    """ Checks if sheet with name already exists ( outside of trash ) and
  
    if not, creates the sheet. Both sheet and tab must be provided or both must be
    omitted to create
    a blank sheet and tab.
  
    Args:
      * sheet_name: (string) name of sheet to create, used as key to check if it
        exists in the future.
      * sheet_tab: (string) name of the tab to create.
      * template_sheet: (string) optional sheet to copy tempalte from.
      * template_tab: (string) optional tab to copy template from.
      * parent: (string) the Google Drive to upload the file to.
  
    Returns:
      * JSON specification of the file created or existing.
  
    """
  
    created = False
  
    # check if sheet and tab exist
    sheet_id, tab_id = self.tab_id(sheet_name, sheet_tab)
  
    # if no sheet create it and the tab
    if sheet_id is None:
      if self.config.verbose:
        print('SHEET CREATE', sheet_name, sheet_tab)
      body = {
          'properties': {
              'title': sheet_name,
          },
          'sheets': [{
              'properties': {
                  'title': sheet_tab,
              }
          }]
      }
      spreadsheet = API_Sheets(self.config, self.auth).spreadsheets().create(body=body).execute()
      sheet_id = spreadsheet['spreadsheetId']
      tab_id = spreadsheet['sheets'][0]['properties']['title']
      created = True
  
    # if creating tab from template
    if (created or tab_id is None) and template_sheet and template_tab:
      if self.config.verbose:
        print('SHEET TAB COPY', sheet_tab)
      self.tab_copy(template_sheet, template_tab, sheet_id, sheet_tab, True)
  
    # if creating a blank tab
    elif tab_id is None:
      if self.config.verbose:
        print('SHEET TAB CREATE', sheet_name, sheet_tab)
      self.tab_create(sheet_name, sheet_tab)
  
    # if sheet and tab already exist
    else:
      if self.config.verbose:
        print('SHEET EXISTS', sheet_name, sheet_tab)
  
    return sheet_id, tab_id, created
