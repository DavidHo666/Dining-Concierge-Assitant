import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import re


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """

def get_session_attributes(intent_request):
    return intent_request['sessionState'].get('sessionAttributes', {})

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']

def get_slot(intent_request, slot_name):
    slots = get_slots(intent_request)
    if slots[slot_name]:
        values = slots[slot_name]['value']
        if values['resolvedValues']:
            return values['resolvedValues'][0]
        else:
            return values["originalValue"]
    else:
        return None

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        },
        'messages': [message],
    }

# ref: https://forum.rasa.com/t/want-to-integrate-amazon-connect-ivr-to-rasa-open-source/46962
def elicit_intent(session_attributes, message):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'ElicitIntent',
            }
        },
        'messages': [message]
    }


def close(session_attributes, fulfilled_intent, messages):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': fulfilled_intent
        },
        'messages': messages
    }


def delegate(session_attributes, slots):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent':{
                "name": "DiningSuggestionsIntent",
                'slots':slots
            }
        }
    }


""" --- Helper Functions --- """


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def validate_dining_suggestions(location, cuisine, party_size, date, time, email):
    location_types = ['manhattan', 'nyc']
    if location and location.lower() not in location_types:
        return build_validation_result(False,
                                       'location',
                                       'We do not have suggestions in {}, '
                                       'you can choose Manhattan or NYC'.format(location))


    cuisine_types = ['american', 'italian', 'french', 'spanish', 'chinese', 'mexican', 'japanese',
                     'korean', 'thai']
    if cuisine and cuisine.lower() not in cuisine_types:
        return build_validation_result(False,
                                       'cuisine',
                                       'We do not have suggestions in {}, '
                                       'please try another one such as Japanese'.format(cuisine))



    if party_size:
        if not party_size.isnumeric():
            return build_validation_result(False,
                                           'party_size',
                                           'Party Size is invalid. Please input a valid number.')

        if party_size.isnumeric() and (int(party_size) < 1 or int(party_size) > 12):
            return build_validation_result(False,
                                           'party_size',
                                           'We can only give suggestions for a party with 1-12 people, please try again.')



    if date:
        if not isvalid_date(date):
            return build_validation_result(False,
                                           'date',
                                           'I did not understand that, you can try today or tomorrow')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False,
                                           'date',
                                           'The date can not be earlier than today, please try again.')


    if time:
        if not isvalid_date(date+' '+time):
            return build_validation_result(False,
                                           'time',
                                           'I did not understand that, please input a valid time.')
        if datetime.datetime.strptime(date+' '+time, '%Y-%m-%d %H:%M') < datetime.datetime.now():
            return build_validation_result(False,
                                           'time',
                                           'The time can not be earlier than now, '
                                           'please try again.')

        if len(time) != 5:
            return build_validation_result(False, 'time', None)



    if email:
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        if not (re.fullmatch(regex, email)):
            return build_validation_result(False,
                                           'email',
                                           'The email is invalid, please try again.')


    return build_validation_result(True, None, None)


# ref: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
def push_SQS(location, cuisine, party_size, date, time, email):
    sqs = boto3.client('sqs', region_name = 'us-east-1',
                       aws_access_key_id = os.getenv('KEY_ID'),
                       aws_secret_access_key = os.getenv('SECRET_KEY'))
    url = os.getenv('QUEUE_URL')
    message_attributes = {
            'location': {
                'DataType': 'String',
                'StringValue': location
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': cuisine
            },
            'party_size': {
                'DataType': 'String',
                'StringValue': party_size
            },
            'date': {
                'DataType': 'String',
                'StringValue': date
            },
            'time': {
                'DataType': 'String',
                'StringValue': time
            },
            'email': {
                'DataType': 'String',
                'StringValue': email
            }
        }
    message_body = 'slots from the user'
    response = sqs.send_message(QueueUrl=url, MessageAttributes=message_attributes, MessageBody = message_body)
    print(response)
    logger.debug(response)



""" --- Functions that control the bot's behavior --- """

def greeting_intent(intent_request):
    session_attributes = get_session_attributes(intent_request)
    message =  {
            'contentType': 'PlainText',
            'content': "Hi! How Can I Help you?"
        }
    return elicit_intent(session_attributes, message)




def dining_suggestions_intent(intent_request):
    location = get_slot(intent_request, 'location')
    cuisine = get_slot(intent_request, 'cuisine')
    party_size = get_slot(intent_request, 'party_size',)
    date = get_slot(intent_request, 'date')
    time = get_slot(intent_request, 'time')
    email = get_slot(intent_request, 'email')
    source = intent_request['invocationSource']
    session_attributes = get_session_attributes(intent_request)

    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        validation_result = validate_dining_suggestions(location, cuisine, party_size,
                                                        date, time, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None

            return elicit_slot(session_attributes,
                               intent_request['sessionState']['intent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        return delegate(session_attributes, get_slots(intent_request))

    push_SQS(location, cuisine, party_size, date, time, email)

    fulfilled_intent = intent_request['sessionState']['intent']
    fulfilled_intent['state'] = 'Fulfilled'
    messages = [{'contentType': 'PlainText',
               'content': "You're all set. Expect my suggestions to {} shortly!".format(email)}]
    return close(session_attributes, fulfilled_intent, messages)


def thank_you_intent(intent_request):
    session_attributes = get_session_attributes(intent_request)
    fulfilled_intent = intent_request['sessionState']['intent']
    fulfilled_intent['state'] = 'Fulfilled'
    messages = [{'contentType': 'PlainText',
                'content': "You're welcome!"}]
    return close(session_attributes, fulfilled_intent, messages)

""" --- Intents --- """




def dispatch(intent_request):

    intent_name = intent_request['sessionState']['intent']['name']

    if intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug(event)
    return dispatch(event)


