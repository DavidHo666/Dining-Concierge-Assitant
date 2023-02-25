import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import logging
import time
import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

REGION = 'us-east-1'

sqs = boto3.client('sqs', region_name = REGION,
                       aws_access_key_id = os.getenv('KEY_ID'),
                       aws_secret_access_key = os.getenv('SECRET_KEY'))


session = boto3.Session(region_name='us-east-1',
                        aws_access_key_id=os.getenv('KEY_ID'),
                        aws_secret_access_key=os.getenv('SECRET_KEY'))
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')

ses = boto3.client("ses", region_name=REGION,
                   aws_access_key_id=os.getenv('KEY_ID'),
                   aws_secret_access_key=os.getenv('SECRET_KEY'))


# ref: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
def receive_sqs_message():
    response = sqs.receive_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        AttributeNames=['All'],
        MessageAttributeNames=[
            'cuisine', 'date', 'location', 'party_size', 'email', 'time'
        ],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10
    )
    # [{'MessageId': 'f1285163-d0d9-4d39-97de-a80763a4c1e5', 'ReceiptHandle': 'AQEBYq6q7UFSlcAE+zWM/nZzmk5Kmb0wC9xxXeOhOfarFi/byJfPNNWNOxRCWluwxxH0sEnxtbUhRYOU3uhW27urJbimrdkWllGJr3qJMjtisRuRo5VGokqooHNCpq/s7D5vS1Q9OHmckxpLL6wpEFrMmecFpNhcdtWDsB18T4oRYp6H46Q5xiYRqgQy69kwBYTTr1Cy3twViIFVrW4Xe8hMjRVQRvWoH+4CS8OHs0KvEij+5F36oVqGLMCv2DrijR+pK5+ImXHU3vWHDw+TRnAEeUVzG8m6VNhzupSstq2eX+b0HPXIDKcrB8Ct8OcQJHp3sT2xiVNIKAOVsQLc+V2Q9V1OT482iWGHU0U5QtYRQaM84NRVhhnaXH1iHKnRNfbV5rvCKlCev/4Elkm4kc72ow==', 'MD5OfBody': '323f1e975e4258b28ef2b35350fc1a7c', 'Body': 'slots from the user', 'Attributes': {'SenderId': '267524565890', 'ApproximateFirstReceiveTimestamp': '1677272586781', 'ApproximateReceiveCount': '1', 'SentTimestamp': '1677205066582'}, 'MD5OfMessageAttributes': '229e079bd77d635892b59b82ebd97403', 'MessageAttributes': {'cuisine': {'StringValue': 'japanese', 'DataType': 'String'}, 'date': {'StringValue': '2023-02-22', 'DataType': 'String'}, 'location': {'StringValue': 'manhattan', 'DataType': 'String'}, 'party_size': {'StringValue': '4', 'DataType': 'String'}, 'phone_number': {'StringValue': '6469459688', 'DataType': 'String'}, 'time': {'StringValue': '16:00', 'DataType': 'String'}}}]
    return response.get('Messages', [])

def delete_sqs_message(receipt_handle):
    sqs.delete_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        ReceiptHandle=receipt_handle
    )

# ref: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/search-example.html
def find_res_opensearch(index, cuisine, num_restaurant=5):
    service = 'es'
    awsauth = get_awsauth(REGION, service)
    url = os.getenv('OS_HOST') + '/' + index + '/_search'
    query = {
        # "size": 5,
        "query": {
            "match": {
                "cuisine": cuisine.lower()
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    r = json.loads(r.text)
    random_res = random.choices(r['hits']['hits'], k=num_restaurant)
    response = []
    for res in random_res:
        response.append(res['_source'])

    # [{'restaurantID': 'axqp3pGJXnTLgq2QrPyDyQ', 'cuisine': 'japanese'}, {'restaurantID': 'kesYSgOJW5krU6L8n9qQ4Q', 'cuisine': 'japanese'}, {'restaurantID': '9QK3vhI04Q8ylqk49C3JcQ', 'cuisine': 'japanese'}, {'restaurantID': 'i8ejDDR4COtukAAA1Ls5fw', 'cuisine': 'japanese'}, {'restaurantID': 'ipAsHykRvgJzpx5hD8vTJw', 'cuisine': 'japanese'}]
    return response



def search_dynamoDB(key):
    return table.get_item(Key={'businessID':key})

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)

# https://www.learnaws.org/2020/12/18/aws-ses-boto3-guide/
def verify_email_identity(email):
    response = ses.verify_email_identity(
        EmailAddress=email
    )
    print(response)

def send_email(email, message):
    if email not in ses.list_verified_email_addresses()['VerifiedEmailAddresses']:
        verify_email_identity(email)
        while email not in ses.list_verified_email_addresses()['VerifiedEmailAddresses']:
            time.sleep(10)

    ses.send_email(
        Destination={
            "ToAddresses": [
                email,
            ],
        },
        Message={
            "Subject": {
                "Charset": "UTF-8",
                "Data": "Restaurant suggestions",
            },
            "Body": {
                "Text": {
                    "Charset": "UTF-8",
                    "Data": message,
                }
            },
        },
        Source='dh3027@columbia.edu'
    )

def build_message(all_details, cuisine, party_size, date, time, location):
    message = 'Welcome to use Dining Concierge Assistant!\n'
    message += 'Your requirements are: \n'
    message += f"Cuisine: {cuisine}\n"
    message += f"Location: {location}\n"
    message += f"Party Size: {party_size}\n"
    message += f"Date: {date}\n"
    message += f"Time: {time}\n"
    message += '\n'
    message += f"We have {len(all_details)} restaurant suggestions for you: \n"
    for i, res in enumerate(all_details):
        message += f"({i+1}) {res['name']}: {' '.join(res['location'])}\n"
    return message


def lambda_handler(event, context):
    messages = receive_sqs_message()
    for msg in messages:
        cuisine = msg['MessageAttributes']['cuisine']['StringValue']
        party_size = msg['MessageAttributes']['party_size']['StringValue']
        date = msg['MessageAttributes']['date']['StringValue']
        time = msg['MessageAttributes']['time']['StringValue']
        location = msg['MessageAttributes']['location']['StringValue']
        email = msg['MessageAttributes']['email']['StringValue']

        results = find_res_opensearch('restaurants',
                                      cuisine,
                                      5)

        all_details = []
        for res in results:
            res_details = search_dynamoDB(res['restaurantID'])
            res_details = res_details['Item']
            res_dict = {'name': res_details['name'], 'location': res_details['location']['display_address']}
            all_details.append(res_dict)

        response_message = build_message(all_details, cuisine,party_size,date,time,location)
        send_email(email, response_message)

        delete_sqs_message(msg['ReceiptHandle'])



# if __name__ == '__main__':
#     lambda_handler(None, None)