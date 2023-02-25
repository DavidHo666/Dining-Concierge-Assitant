import json
from decimal import Decimal
from yelpapi import YelpAPI
from pprint import pprint
from datetime import datetime
import os
import boto3

session = boto3.Session(region_name='us-east-1',
                        aws_access_key_id=os.getenv('KEY_ID'),
                        aws_secret_access_key=os.getenv('SECRET_KEY'))
IDs = set()
def get_restaurant(cuisine, total_num = 1000):
    with YelpAPI(os.getenv('YELP_API_KEY')) as yelp_api:
        all_restaurants = []
        for offset in range(0, total_num, 50):
            print(f"Getting {cuisine}, offset: {offset}")
            response = yelp_api.search_query(term = 'restaurants', categories = cuisine, location = 'New York',
                                             limit = 50, offset = offset)
            # all_restaurants.extend([res for res in response['businesses']])
            for res in response['businesses']:
                if res['id'] not in IDs:
                    all_restaurants.append(res)
                    IDs.add(res['id'])

        return all_restaurants

def store_DynamoDB(restaurants):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    with table.batch_writer(overwrite_by_pkeys=['businessID']) as batch:
        print(f"Wrinting to DynanmoDB")
        for res in json.loads(json.dumps(restaurants),parse_float=Decimal):
            res['businessID'] = res['id']
            del res['id']
            res['insertedAtTimestamp'] = str(datetime.now())
            batch.put_item(Item = res)

def generate_ES_json(restaurants, cuisine, cur_id):
    print('Writing to ES.json')
    with open("ES.json", 'a') as outfile:
        for res in json.loads(json.dumps(restaurants)):
            first = {"index": {"_index": "restaurants", "_id": cur_id}}
            cur_id += 1
            outfile.write(json.dumps(first))
            outfile.write('\n')
            second = {'restaurantID': res['id'], 'cuisine': cuisine}
            outfile.write(json.dumps(second))
            outfile.write('\n')
        return cur_id

if __name__ == '__main__':
    cuisines = ['italian', 'french', 'chinese', 'greek', 'indian', 'mexican', 'japanese', 'korean', 'thai']
    cur_id = 1
    if os.path.exists("ES.json"):
        os.remove("ES.json")
    for cuisine in cuisines:
        result = get_restaurant(cuisine)
        cur_id = generate_ES_json(result, cuisine, cur_id)
        store_DynamoDB(result)
