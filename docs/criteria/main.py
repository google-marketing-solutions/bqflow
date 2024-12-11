###########################################################################
#
#  Copyright 2024 Google LLC
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

"""Helper for generating the criteria file used to look up identifiers.

No parameters, just run it and it will create the lookup file by downloading
relevant files from Google website and converting to a dictionary.
"""

import csv
import io
import json
import requests
import zipfile

CRITERIA_CSV = {
    'affinity categories': 'https://developers.google.com/static/google-ads/api/data/tables/affinity-categories.csv',
    'ages': 'https://developers.google.com/static/google-ads/api/data/tables/ages.csv',
    'weekdays': 'https://developers.google.com/static/google-ads/api/data/tables/days.csv',
    'genders': 'https://developers.google.com/static/google-ads/api/data/tables/genders.csv',
    'income range': 'https://developers.google.com/static/google-ads/api/data/tables/income-range.csv',
    'in market categories': 'https://developers.google.com/static/google-ads/api/data/tables/in-market-categories.csv',
    'language codes': 'https://developers.google.com/static/google-ads/api/data/tables/languagecodes.csv',
    'life events': 'https://developers.google.com/static/google-ads/api/data/tables/life-events.csv',
    'mobile app categories': 'https://developers.google.com/static/google-ads/api/data/tables/mobileappcategories.csv',
    'mobile carriers': 'https://developers.google.com/static/google-ads/api/data/tables/mobilecarriers.csv',
    'mobile platforms': 'https://developers.google.com/static/google-ads/api/data/tables/mobileplatforms.csv',
    'smartphone user interest': 'https://developers.google.com/static/google-ads/api/data/tables/smartphone-userinterest.csv',
    'parents': 'https://developers.google.com/static/google-ads/api/data/tables/parents.csv',
    'platforms': 'https://developers.google.com/static/google-ads/api/data/tables/platforms.csv',
    'products services': 'https://developers.google.com/static/google-ads/api/data/tables/productsservices.csv',
    'chain ids': 'https://developers.google.com/static/google-ads/api/data/tables/chain-ids.csv',
    'country codes': 'https://developers.google.com/static/google-ads/api/data/tables/country-codes.csv',
    'currency codes': 'https://developers.google.com/static/google-ads/api/data/tables/currencycodes.csv',
    'locales': 'https://developers.google.com/static/google-ads/api/data/tables/locales.csv',
    'gls services': 'https://developers.google.com/static/google-ads/api/data/tables/gls_services.csv',
    'rich media codes': 'https://developers.google.com/static/google-ads/api/data/tables/richmediacodes.csv',
    'time zones': 'https://developers.google.com/static/google-ads/api/data/tables/timezones.csv',
}

CRITERIA_ZIP = {
    'geos': 'https://developers.google.com/static/google-ads/api/data/geo/geotargets-2024-10-10.csv.zip',
}


def load_identifiers_zip(url: str) -> dict:
  """Helper that loads data from a ZIP file into a dictionary.
  """

  response = requests.get(url)
  response.raise_for_status()  # Raise an exception for bad status codes
  with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
    csv_file_name = next(
        (name for name in zip_ref.namelist() if name.endswith('.csv')),
        None
    )
    if csv_file_name:
      with zip_ref.open(csv_file_name) as csv_file:
        rows = iter(csv.reader(io.TextIOWrapper(csv_file, 'utf-8')))
        return {'headers': next(rows), 'rows': list(rows)}


def load_identifiers_csv(url: str) -> dict:
  """Helper that loads data from a CSV file into a dictionary.
  """

  response = requests.get(url)
  response.raise_for_status()  # Raise an exception for bad status codes
  rows = iter(
      csv.reader(io.TextIOWrapper(io.BytesIO(response.content), 'utf-8'))
  )
  return {'headers': next(rows), 'rows': list(rows)}


if __name__ == '__main__':

  records = {}
  for criteria_name, criteria_url in CRITERIA_CSV.items():
    records[criteria_name] = load_identifiers_csv(url=criteria_url)

  for criteria_name, criteria_url in CRITERIA_ZIP.items():
    records[criteria_name] = load_identifiers_zip(url=criteria_url)

  with open('criteria.js', 'w') as f:
    f.write(f'const criteria = {json.dumps(records, separators=(",", ":"))}\n')
