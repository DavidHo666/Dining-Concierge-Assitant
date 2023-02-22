import json
from decimal import Decimal
from yelpapi import YelpAPI
from pprint import pprint
from datetime import datetime
import os
import boto3

session = boto3.Session(region_name='us-east-1',
                        aws_access_key_id=os.getenv('AWS_KEY_ID'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))

def get_restaurant(cuisine, total_num = 1000):
    with YelpAPI(os.getenv('YELP_API_KEY')) as yelp_api:
        all_restaurants = []
        for offset in range(0, total_num, 50):
            print(f"Getting {cuisine}, offset: {offset}")
            response = yelp_api.search_query(term = 'restaurants', categories = cuisine, location = 'New York',
                                             limit = 50, offset = offset)
            all_restaurants.extend([res for res in response['businesses']])

        return all_restaurants

def store_DynamoDB(restaurants):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    with table.batch_writer(overwrite_by_pkeys=['name']) as batch:
        print(f"Wrinting to DynanmoDB")
        for res in json.loads(json.dumps(restaurants),parse_float=Decimal):
            res['businessId'] = res['id']
            del res['id']
            res['insertedAtTimestamp'] = str(datetime.now())
            batch.put_item(Item = res)

if __name__ == '__main__':
    cuisines = ['italian', 'french', 'chinese', 'greek', 'indian', 'mexican', 'japanese', 'korean', 'thai']
    for cuisine in cuisines:
        result = get_restaurant(cuisine)
        store_DynamoDB(result)
