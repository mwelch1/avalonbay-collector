import datetime
import os
import re
import time
import requests
from airtable import Airtable


LISTINGS_URL = os.environ.get(
    'LISTINGS_URL', "https://api.avalonbay.com/json/reply/ApartmentSearch")
COMMUNITY_CODE = os.environ.get('COMMUNITY_CODE', "CA117")
MIN_PRICE = os.environ.get('MIN_PRICE', 2000)
MAX_PRICE = os.environ.get('MAX_PRICE', 8000)
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_KEY = os.environ.get('AIRTABLE_BASE_KEY')
AIRTABLE_TABLE_NAME = os.environ.get('AIRTABLE_TABLE_NAME')


def get_listings():
    params = dict(
        communityCode=COMMUNITY_CODE,
        min=MIN_PRICE,
        max=MAX_PRICE,
        _=time.time()
    )
    resp = requests.get(LISTINGS_URL, params=params)
    results = resp.json()['results']['availableFloorPlanTypes']
    return results


def filter_listings(results):
    available_units = list()
    for result in results:
        availableFloorPlans = result['availableFloorPlans']
        for availableFloorPlan in availableFloorPlans:
            for finishPackages in availableFloorPlan['finishPackages']:
                for apartment in finishPackages['apartments']:
                    available_date = apartment['pricing']['availableDate']
                    details = {
                        'Unit': apartment['apartmentNumber'],
                        'Type': f"{apartment['beds']} beds and {apartment['baths']} baths",
                        'Collection': finishPackages['finishPackageName'],
                        'SqFt': apartment['apartmentSize'],
                        'Available Date': available_date,
                        'Rent': apartment['pricing']['effectiveRent'],
                        'Floorplan': [{'url': availableFloorPlan['floorPlanImage']}],
                        'Available': True
                    }
                    available_units.append(details)
    return available_units


def insert_or_update(airtable, units):
    # Update known units or create new entries
    for unit in units:
        match = airtable.match('Unit', unit['Unit'])
        if match:
            print(f"Found existing entry for unit {unit['Unit']}")
            airtable.update(match['id'], unit)
        else:
            print(f"Creating new entry for unit {unit['Unit']}")
            airtable.insert(unit)


def cleanup(airtable, units):
    new_units = [x['Unit'] for x in units]
    for page in airtable.get_iter():
        for record in page:
            unit = record['fields']['Unit']
            if unit not in new_units:
                print(f"Marking unit {unit} as unavailable")
                airtable.update(record['id'], {'Available': False})


if __name__ == '__main__':
    airtable = Airtable(base_key=AIRTABLE_BASE_KEY, table_name=AIRTABLE_TABLE_NAME,
                        api_key=AIRTABLE_API_KEY)
    units = filter_listings(get_listings())
    insert_or_update(airtable, units)
    cleanup(airtable, units)
