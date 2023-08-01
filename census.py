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

import argparse
import textwrap

CENSUS_GEOGRAPHY = "zip_codes"
CENSUS_YEAR = "2018"
CENSUS_SPAN = "5yr"
CENSUS_KEY = 'Zip'


CENSUS_FIELDS = [
#{
#  'category': None,
#  'denominator': None,
#  'columns': ['geo_id', 'total_pop']
#}, 

#total_pop - (female_pop + male_pop) AS gender
{ 'category': 'gender',
  'denominator': 'total_pop',
  'columns': [
    'female_pop',
    'male_pop',
  ]
},

# male_pop - (male_under_5 + male_5_to_9 + male_10_to_14 + male_15_to_17 + male_18_to_19 + male_20 + male_21 + male_22_to_24 + male_25_to_29 + male_30_to_34 + male_35_to_39 + male_40_to_44 + male_45_to_49 + male_50_to_54 + male_55_to_59 + male_65_to_66 + male_67_to_69 + male_70_to_74 + male_75_to_79 + male_80_to_84 + male_85_and_over) # remainder is male_60_to_64
{ 'category': 'male age',
  'denominator': 'male_pop',
  'columns': [
    '-male_under_5',
    '-male_5_to_9',
    '-male_10_to_14',
    '-male_15_to_17',
    'male_18_to_19',
    'male_20',
    'male_21',
    'male_22_to_24',
    'male_25_to_29',
    'male_30_to_34',
    'male_35_to_39',
    'male_40_to_44',
    'male_45_to_49',
    'male_50_to_54',
    'male_55_to_59',
    '+male_60_to_64',
    'male_65_to_66',
    'male_67_to_69',
    'male_70_to_74',
    'male_75_to_79',
    'male_80_to_84',
    'male_85_and_over',
  ],
},

# female_pop - (female_under_5 + female_5_to_9 + female_10_to_14 + female_15_to_17 + female_18_to_19 + female_20 + female_21 + female_22_to_24 + female_25_to_29 + female_30_to_34 + female_35_to_39 + female_40_to_44 + female_45_to_49 + female_50_to_54 + female_55_to_59 + female_60_to_61 + female_62_to_64 + female_65_to_66 + female_67_to_69 + female_70_to_74 + female_75_to_79 + female_80_to_84 + female_85_and_over) # combine female_60_to_61 + female_62_to_64 to get female_60_to_64
{ 'category': 'female age',
  'denominator': 'female_pop',
  'columns': [
    '-female_under_5',
    '-female_5_to_9',
    '-female_10_to_14',
    '-female_15_to_17',
    'female_18_to_19',
    'female_20',
    'female_21',
    'female_22_to_24',
    'female_25_to_29',
    'female_30_to_34',
    'female_35_to_39',
    'female_40_to_44',
    'female_45_to_49',
    'female_50_to_54',
    'female_55_to_59',
    'female_60_to_61',
    'female_62_to_64',
    'female_65_to_66',
    'female_67_to_69',
    'female_70_to_74',
    'female_75_to_79',
    'female_80_to_84',
    'female_85_and_over',
  ]
},

# households - (income_less_10000 + income_10000_14999 + income_15000_19999 + income_20000_24999 + income_25000_29999 + income_30000_34999 + income_35000_39999 + income_40000_44999 + income_45000_49999 + income_50000_59999 + income_60000_74999 + income_75000_99999 + income_100000_124999 + income_125000_149999 + income_150000_199999 + income_200000_or_more)

{ 'category': 'income',
  'denominator': 'households',
  'columns': [
    'income_less_10000',
    'income_10000_14999',
    'income_15000_19999',
    'income_20000_24999',
    'income_25000_29999',
    'income_30000_34999',
    'income_35000_39999',
    'income_40000_44999',
    'income_45000_49999',
    'income_50000_59999',
    'income_60000_74999',
    'income_75000_99999',
    'income_100000_124999',
    'income_125000_149999',
    'income_150000_199999',
    'income_200000_or_more',
  ]
},
#{ 'category': 'poverty',
#  'denominator': None,
#  'columns': [
#    'poverty',
#    'gini_index',
#  ]
#},

# housing_units - (occupied_housing_units + vacant_housing_units)
{ 'category': 'housing status',
  'denominator': 'housing_units',
  'columns': [
    'occupied_housing_units',
    'vacant_housing_units',
  ]
},

# occupied_housing_units - (housing_units_renter_occupied + owner_occupied_housing_units)
{ 'category': 'housing occupancy',
  'denominator': 'occupied_housing_units',
  'columns': [
    'housing_units_renter_occupied',
    'owner_occupied_housing_units',
  ]
},

# vacant_housing_units - (vacant_housing_units_for_rent + vacant_housing_units_for_sale) # remainder is vacant_housing_units_idle
{ 'category': 'housing listing',
  'denominator': 'vacant_housing_units',
  'columns': [
    'vacant_housing_units_for_rent',
    'vacant_housing_units_for_sale',
    '+vacant_housing_units_idle',
  ],
},

# housing_units - (mortgaged_housing_units) # remainder is paid off housing units

{ 'category': 'housing deed',
  'denominator': 'housing_units',
  'columns': [
    'mortgaged_housing_units',
    '+paid_off_housing_units',
  ],
},

# housing_units - million_dollar_housing_units #remaineder is < 1 million
{ 'category': 'housing value',
  'denominator': 'housing_units',
  'columns': [
    'million_dollar_housing_units',
    '+sub_million_dollar_housing_units',
  ],
},

# housing_units - (dwellings_1_units_detached + dwellings_1_units_attached + dwellings_2_units + dwellings_3_to_4_units + dwellings_5_to_9_units + dwellings_10_to_19_units + dwellings_20_to_49_units + dwellings_50_or_more_units + mobile_homes), # dont count group_quarters
{ 'category': 'housing size',
  'denominator': 'housing_units',
  'columns': [
    'dwellings_1_units_detached',
    'dwellings_1_units_attached',
    'dwellings_2_units',
    'dwellings_3_to_4_units',
    'dwellings_5_to_9_units',
    'dwellings_10_to_19_units',
    'dwellings_20_to_49_units',
    'dwellings_50_or_more_units',
    'mobile_homes',
  ]
},

# housing_units - ( housing_built_1939_or_earlier + housing_built_2000_to_2004 + housing_built_2005_or_later) # remainder is housing 1940-1999
{ 'category': 'housing age',
  'denominator': 'housing_units',
  'columns': [
    'housing_built_1939_or_earlier',
    '+housing_built_1940_to_1999',
    'housing_built_2000_to_2004',
    'housing_built_2005_or_later',
  ],
},

#housing_units_renter_occupied - (rent_over_50_percent + rent_40_to_50_percent + rent_35_to_40_percent + rent_30_to_35_percent + rent_25_to_30_percent + rent_20_to_25_percent + rent_15_to_20_percent + rent_10_to_15_percent + rent_under_10_percent + rent_burden_not_computed) 
{ 'category': 'housing burden',
  'denominator': 'housing_units_renter_occupied',
  'columns': [
    'rent_over_50_percent',
    'rent_40_to_50_percent',
    'rent_35_to_40_percent',
    'rent_30_to_35_percent',
    'rent_25_to_30_percent',
    'rent_20_to_25_percent',
    'rent_15_to_20_percent',
    'rent_10_to_15_percent',
    'rent_under_10_percent',
    'rent_burden_not_computed',
  ]
},

#{ 'category': 'housing mobility',
#  'denominator': 'households',
#  'columns': [
#    'different_house_year_ago_different_city',
#    'different_house_year_ago_same_city'
#  ]
#},

# two_parent_families_with_young_children - (two_parents_in_labor_force_families_with_young_children + two_parents_father_in_labor_force_families_with_young_children + two_parents_mother_in_labor_force_families_with_young_children + two_parents_not_in_labor_force_families_with_young_children) AS family_children_working,
{ 'category': 'family two parent',
  'denominator': 'two_parent_families_with_young_children',
  'columns': [
    'two_parents_in_labor_force_families_with_young_children',
    'two_parents_father_in_labor_force_families_with_young_children',
    'two_parents_mother_in_labor_force_families_with_young_children',
    'two_parents_not_in_labor_force_families_with_young_children',
  ]
},

#  households - (nonfamily_households + family_households) AS family_nf,
{ 'category': 'family',
  'denominator': 'households',
  'columns': [
    'nonfamily_households',
    'family_households',
  ]
},


#  households - (married_households + male_male_households + female_female_households) # remainder is single households
{ 'category': 'family makeup',
  'denominator': 'households',
  'columns': [
    'female_female_households',
    'male_male_households',
    'married_households',
    '+single_households',
  ],
},

#  families_with_young_children - ( one_parent_families_with_young_children + two_parent_families_with_young_children) 
{ 'category': 'family parents',
  'denominator': 'families_with_young_children',
  'columns': [
    'two_parent_families_with_young_children',
    'one_parent_families_with_young_children',
  ]
},

#households - (no_cars + one_car + two_cars + three_cars + four_more_cars)
{ 'category': 'cars owned',
  'denominator': 'households',
  'columns': [
    'no_cars',
    'one_car',
    'two_cars',
    'three_cars',
    'four_more_cars',
  ]
},

# commuters_16_over - (commute_less_10_mins + commute_10_14_mins + commute_15_19_mins + commute_20_24_mins + commute_25_29_mins + commute_30_34_mins + commute_35_44_mins + commute_45_59_mins + commute_60_more_mins)
{ 'category': 'commuter time',
  'denominator': 'commuters_16_over',
  'columns': [
    'commute_less_10_mins',
    'commute_10_14_mins',
    'commute_15_19_mins',
    'commute_20_24_mins',
    'commute_25_29_mins',
    'commute_30_34_mins',
    'commute_35_44_mins',
    'commute_45_59_mins',
    'commute_60_more_mins',
  ]
},

# commuters_16_over - (walked_to_work + commuters_by_public_transportation + commuters_by_car_truck_van) AS commuter_method,
{ 'category': 'commuter method',
  'denominator': 'commuters_16_over',
  'columns': [
    'walked_to_work',
    'commuters_by_public_transportation',
    'commuters_by_car_truck_van',
    '+commuters_by_other',
  ]
},

# commuters_by_car_truck_van - (commuters_by_carpool + commuters_drove_alone) AS commuter_private,
{ 'category': 'commuter private',
  'denominator': 'commuters_by_car_truck_van',
  'columns': [
    'commuters_by_carpool',
    'commuters_drove_alone',
  ]
},

# commuters_by_public_transportation - (commuters_by_bus + commuters_by_subway_or_elevated ) AS commuter_public,
{ 'category': 'commuter public',
  'denominator': 'commuters_by_public_transportation',
  'columns': [
    'commuters_by_bus',
    'commuters_by_subway_or_elevated',
  ]
},


{ 'category': 'education degree',
  'denominator': 'total_pop',
  'columns': [
    '-in_school',
    'in_undergrad_college',
    'less_than_high_school_graduate',
    'high_school_diploma',
    'high_school_including_ged',
    'less_one_year_college',
    'one_year_more_college',
    'some_college_and_associates_degree',
    'associates_degree',
    'bachelors_degree',
    'bachelors_degree_2',
    'masters_degree',
    'graduate_professional_degree',
  ]
},

# civilian_labor_force - (employed_pop + unemployed_pop)
{ 'category': 'labor status',
  'denominator': 'civilian_labor_force',
  'columns': [
    'employed_pop',
    '-unemployed_pop',
  ],
},

# pop_in_labor_force - (civilian_labor_force + armed_forces)
{ 'category': 'labor force',
  'denominator': 'pop_in_labor_force',
  'columns': [
    'civilian_labor_force',
    'armed_forces',
  ]
},

# pop_16_over - (pop_in_labor_force + not_in_labor_force)
{ 'category': 'labor participation',
  'denominator': 'pop_16_over',
  'columns': [
    'pop_in_labor_force',
    'not_in_labor_force',
  ]
},

# employed_pop - (employed_agriculture_forestry_fishing_hunting_mining + employed_arts_entertainment_recreation_accommodation_food + employed_construction + employed_education_health_social + employed_finance_insurance_real_estate + employed_information + employed_manufacturing + employed_other_services_not_public_admin + employed_public_administration + employed_retail_trade + employed_science_management_admin_waste + employed_transportation_warehousing_utilities + employed_wholesale_trade) as emp,
{ 'category': 'labor employed',
  'denominator': 'employed_pop',
  'columns': [
    'employed_agriculture_forestry_fishing_hunting_mining',
    'employed_arts_entertainment_recreation_accommodation_food',
    'employed_construction',
    'employed_education_health_social',
    'employed_finance_insurance_real_estate',
    'employed_information',
    'employed_manufacturing',
    'employed_other_services_not_public_admin',
    'employed_public_administration',
    'employed_retail_trade',
    'employed_science_management_admin_waste',
    'employed_transportation_warehousing_utilities',
    'employed_wholesale_trade',
  ]
}, 

# employed_pop - (occupation_management_arts + occupation_natural_resources_construction_maintenance + occupation_production_transportation_material + occupation_sales_office + occupation_services)
{ 'category': 'labor occupation',
  'denominator': 'employed_pop',
  'columns': [
    'occupation_management_arts',
    'occupation_natural_resources_construction_maintenance',
    'occupation_production_transportation_material',
    'occupation_sales_office',
    'occupation_services',
  ]
},

# occupation_management_arts - management_business_sci_arts_employed
{ 'category': 'occupation_managment_arts',
  'denominator': 'occupation_management_arts',
  'columns': [
    'management_business_sci_arts_employed',
  ]
},

# occupation_sales_office - sales_office_employed
{ 'category': 'occupation_sales_office',
  'denominator': 'occupation_sales_office',
  'columns': [
    'sales_office_employed'
  ]
},
]


def census_total():
  ''' Attempt to add up census populations.'''

  query = 'SELECT\n'
  for segment in CENSUS_FIELDS:
    query += '  {} - ({}) AS {},\n'.format(
      segment['denominator'],
      ' + '.join(c.replace('-', '') for c in segment['columns'] if c[0] != '+'),
      segment['category'].replace(' ', '_')
    )
  query += 'FROM `bigquery-public-data.census_bureau_acs.%s_%s_%s`' % (CENSUS_GEOGRAPHY, CENSUS_YEAR, CENSUS_SPAN)
  query += 'WHERE GEO_ID="94920"'

  return query


def census_gap():
  query = 'SELECT *,\n'
  for segment in CENSUS_FIELDS:
    extra = [c.replace('+', '') for c in segment['columns'] if c[0] == '+']
    if extra:
      extra = extra[0]
      query += ' {} -  ({}) AS {},\n'.format(
        segment['denominator'],
        ' + '.join(c.replace('-', '') for c in segment['columns'] if c[0] != '+'),
        extra,
      ) 
  query += 'FROM `bigquery-public-data.census_bureau_acs.%s_%s_%s`' % (CENSUS_GEOGRAPHY, CENSUS_YEAR, CENSUS_SPAN)
  return query


def census_normalize():
  ''' Convert the census populations to percentages.'''

  query = 'SELECT geo_id AS Geo_Id, total_pop AS Total_Pop,\n'

  for segment in CENSUS_FIELDS:
    query += '\n  /* %s */\n' % segment['category']
    for column in segment['columns']:
      if column[0] == '-': continue
      query += '  SAFE_DIVIDE(%s, %s) AS %s,\n' % (column.replace('+', ''), segment['denominator'], column.replace('+', '').title())

  query += 'FROM CENSUS_GAP\n'

  return query


def census_pivot():
  ''' Change census columns to rows.'''

  query = 'WITH CENSUS_GAP AS (\n{}\n),\n\n'.format(census_gap())
  query += 'CENSUS_NORMALIZED AS (\n{}\n),\n\n'.format(census_normalize())
  query += 'CENSUS_PIVOT AS (\nSELECT\n  Geo_Id AS {},\n  CASE\n'.format(CENSUS_KEY)

  for s in CENSUS_FIELDS:
    if s['category']:
      query += "    WHEN Dimension IN ({}) THEN '{}'\n".format(
        ','.join("'{}'".format(c.replace('+', '')).title() for c in s['columns'] if c[0] != '-'),
        s['category'].title()
      ) 

  query += '  END AS Segment,\n  Dimension,\n   CAST((Total_Pop * Share) AS INT64) AS Pop,\n  Share\n'
  query += 'FROM CENSUS_NORMALIZED\n'
  query += 'UNPIVOT(Share FOR Dimension IN ({}))\n)\n\n'.format(','.join(c.replace('+', '').title() for s in CENSUS_FIELDS for c in s['columns'] if s['category'] and c[0] != '-'))

  query += 'SELECT * FROM CENSUS_PIVOT'

  return query


def main():

  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      description=textwrap.dedent("""\
      Command line to help manipulate BigQuery Census tables.

      Examples:
        To normalize the census: python census.py -normalize
        To pivot the census: python census.py -pivot

  """))

  # create parameters
  parser.add_argument('-n', '--normalize', help='Normalize the census values into percentages.', action='store_true', required=False)
  parser.add_argument('-p', '--pivot', help='Pivot the census columns into rows.', action='store_true')
  parser.add_argument('-g', '--gap', help='Gap the census fields that are missing.', action='store_true')
  parser.add_argument('-t', '--total', help='Pivot the census totals.', action='store_true')

  args = parser.parse_args()

  if args.total:
    print(census_total())

  if args.gap:
    print(census_gap())

  if args.pivot:
    print(census_pivot())

  if args.normalize:
    print(census_normalize())


if __name__ == '__main__':
  main()
