import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

REGION = 'us-east-1'

sqs = boto3.client('sqs', region_name = 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamodb.Table('yelp-restaurants')


# ref: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
def receive_sqs_message():
    response = sqs.receive_message(
        QueueUrl=os.getenv('SQS_URL'),
        AttributeNames=['All'],
        MessageAttributeNames=[
            'cuisine', 'date', 'location', 'party_size', 'phone_number', 'time'
        ],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10
    )
    # [{'MessageId': 'f1285163-d0d9-4d39-97de-a80763a4c1e5', 'ReceiptHandle': 'AQEBYq6q7UFSlcAE+zWM/nZzmk5Kmb0wC9xxXeOhOfarFi/byJfPNNWNOxRCWluwxxH0sEnxtbUhRYOU3uhW27urJbimrdkWllGJr3qJMjtisRuRo5VGokqooHNCpq/s7D5vS1Q9OHmckxpLL6wpEFrMmecFpNhcdtWDsB18T4oRYp6H46Q5xiYRqgQy69kwBYTTr1Cy3twViIFVrW4Xe8hMjRVQRvWoH+4CS8OHs0KvEij+5F36oVqGLMCv2DrijR+pK5+ImXHU3vWHDw+TRnAEeUVzG8m6VNhzupSstq2eX+b0HPXIDKcrB8Ct8OcQJHp3sT2xiVNIKAOVsQLc+V2Q9V1OT482iWGHU0U5QtYRQaM84NRVhhnaXH1iHKnRNfbV5rvCKlCev/4Elkm4kc72ow==', 'MD5OfBody': '323f1e975e4258b28ef2b35350fc1a7c', 'Body': 'slots from the user', 'Attributes': {'SenderId': '267524565890', 'ApproximateFirstReceiveTimestamp': '1677272586781', 'ApproximateReceiveCount': '1', 'SentTimestamp': '1677205066582'}, 'MD5OfMessageAttributes': '229e079bd77d635892b59b82ebd97403', 'MessageAttributes': {'cuisine': {'StringValue': 'japanese', 'DataType': 'String'}, 'date': {'StringValue': '2023-02-22', 'DataType': 'String'}, 'location': {'StringValue': 'manhattan', 'DataType': 'String'}, 'party_size': {'StringValue': '4', 'DataType': 'String'}, 'phone_number': {'StringValue': '6469459688', 'DataType': 'String'}, 'time': {'StringValue': '16:00', 'DataType': 'String'}}}]
    return response.get('Messages', [])

def delete_sqs_message(receipt_handle):
    sqs.delete_message(
        QueueUrl=os.getenv('SQS_URL'),
        ReceiptHandle=receipt_handle
    )

# ref: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/search-example.html
def find_res_opensearch(index, cuisine, num_restaurant=5):
    region = 'us-east-1'
    service = 'es'
    # credentials = boto3.Session(aws_access_key_id='AKIAT4SNU5OBHEBI36UH',
    #                       aws_secret_access_key="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",
    #                       region_name="us-east-1").get_credentials()
    awsauth = get_awsauth(region, service)
    url = os.getenv('OS_HOST') + '/' + index + '/_search'
    query = {
        "size": num_restaurant,
        "query": {
            "match": {
                "cuisine": cuisine
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

    r = json.loads(r.text)
    response = []
    for res in r['hits']['hits']:
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


def lambda_handler(event, context):
    messages = receive_sqs_message()
    for mesg in messages:
        results = find_res_opensearch('restaurants',
                                      mesg['MessageAttributes']['cuisine'],
                                      5)
        for res in results:
            details = search_dynamoDB(res['restaurantID'])
        delete_sqs_message(mesg['ReceiptHandle'])

    return results

