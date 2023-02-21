import math
import dateutil.parser
import datetime
import time
import os
import logging


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
        if values['interpretedValue']:
            return values['interpretedValue']
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

def validate_dining_suggestions(location, cuisine, party_size, date, time, phone_number):
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
                                       'We do not have suggestions in {}, please try another one'.format(cuisine))

    if party_size:
        if party_size.isnumeric() and (int(party_size) < 1 or int(party_size) > 12):
            return build_validation_result(False,
                                           'party_size',
                                           'We can only give suggestions for a party with 1-12 people, please try again.')

    if date:
        if not isvalid_date(date):
            return build_validation_result(False,
                                           'date',
                                           'I did not understand that, you can try today or tomorrow')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() <= datetime.date.today():
            return build_validation_result(False,
                                           'date',
                                           'The date can not be earlier than today.')

    if time:
        if len(time) != 5:
            return build_validation_result(False, 'time', None)

    if phone_number:
        if len(phone_number) != 10:
            return build_validation_result(False, 'phone_number', 'The phone number is invalid, please try again.')

    return build_validation_result(True, None, None)


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
    phone_number = get_slot(intent_request, 'phone_number')
    source = intent_request['invocationSource']
    session_attributes = get_session_attributes(intent_request)

    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        validation_result = validate_dining_suggestions(location, cuisine, party_size,
                                                        date, time, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None

            return elicit_slot(session_attributes,
                               intent_request['sessionState']['intent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        return delegate(session_attributes, get_slots(intent_request))

    messages = [{'contentType': 'PlainText',
               'content': 'You’re all set. Expect my suggestions to {} shortly!'.format(phone_number)}]
    fulfilled_intent = intent_request['sessionState']['intent']
    fulfilled_intent['state'] = 'Fulfilled'
    return close(session_attributes, fulfilled_intent, messages)


""" --- Intents --- """


def dispatch(intent_request):

    intent_name = intent_request['sessionState']['intent']['name']

    if intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug(event)
    return dispatch(event)


if __name__ == "__main__":
    event = {'sessionId': '267524565890316', 'inputTranscript': 'tomorrow', 'interpretations': [{'intent': {'slots': {'date': {'shape': 'Scalar', 'value': {'originalValue': 'tomorrow', 'resolvedValues': ['2023-02-22'], 'interpretedValue': '2023-02-22'}}, 'cuisine': {'shape': 'Scalar', 'value': {'originalValue': 'japanese', 'resolvedValues': ['Japanese'], 'interpretedValue': 'japanese'}}, 'party_size': {'shape': 'Scalar', 'value': {'originalValue': '12', 'resolvedValues': [], 'interpretedValue': '12'}}, 'location': {'shape': 'Scalar', 'value': {'originalValue': 'manhattan', 'resolvedValues': ['Manhattan'], 'interpretedValue': 'manhattan'}}, 'phone_number': None, 'time': None}, 'confirmationState': 'None', 'name': 'DiningSuggestionsIntent', 'state': 'InProgress'}, 'nluConfidence': 1.0}, {'intent': {'slots': {}, 'confirmationState': 'None', 'name': 'FallbackIntent', 'state': 'InProgress'}}, {'intent': {'slots': {}, 'confirmationState': 'None', 'name': 'GreetingIntent', 'state': 'InProgress'}, 'nluConfidence': 0.44}], 'proposedNextState': {'intent': {'slots': {'date': {'shape': 'Scalar', 'value': {'originalValue': 'tomorrow', 'resolvedValues': ['2023-02-22'], 'interpretedValue': '2023-02-22'}}, 'cuisine': {'shape': 'Scalar', 'value': {'originalValue': 'japanese', 'resolvedValues': ['Japanese'], 'interpretedValue': 'japanese'}}, 'party_size': {'shape': 'Scalar', 'value': {'originalValue': '12', 'resolvedValues': [], 'interpretedValue': '12'}}, 'location': {'shape': 'Scalar', 'value': {'originalValue': 'manhattan', 'resolvedValues': ['Manhattan'], 'interpretedValue': 'manhattan'}}, 'phone_number': None, 'time': None}, 'confirmationState': 'None', 'name': 'DiningSuggestionsIntent', 'state': 'InProgress'}, 'dialogAction': {'slotToElicit': 'time', 'type': 'ElicitSlot'}, 'prompt': {'attempt': 'Initial'}}, 'sessionState': {'sessionAttributes': {}, 'activeContexts': [], 'intent': {'slots': {'date': {'shape': 'Scalar', 'value': {'originalValue': 'tomorrow', 'resolvedValues': ['2023-02-22'], 'interpretedValue': '2023-02-22'}}, 'cuisine': {'shape': 'Scalar', 'value': {'originalValue': 'japanese', 'resolvedValues': ['Japanese'], 'interpretedValue': 'japanese'}}, 'party_size': {'shape': 'Scalar', 'value': {'originalValue': '12', 'resolvedValues': [], 'interpretedValue': '12'}}, 'location': {'shape': 'Scalar', 'value': {'originalValue': 'manhattan', 'resolvedValues': ['Manhattan'], 'interpretedValue': 'manhattan'}}, 'phone_number': None, 'time': None}, 'confirmationState': 'None', 'name': 'DiningSuggestionsIntent', 'state': 'InProgress'}, 'originatingRequestId': '76c8b664-ec1e-4c52-9b75-e404314abfb9'}, 'responseContentType': 'text/plain; charset=utf-8', 'invocationSource': 'DialogCodeHook', 'messageVersion': '1.0', 'transcriptions': [{'transcription': 'tomorrow', 'resolvedSlots': {'date': {'shape': 'Scalar', 'value': {'originalValue': 'tomorrow', 'resolvedValues': ['2023-02-22']}}}, 'transcriptionConfidence': 1.0, 'resolvedContext': {'intent': 'DiningSuggestionsIntent'}}], 'inputMode': 'Text', 'bot': {'aliasId': 'TSTALIASID', 'aliasName': 'TestBotAlias', 'name': 'DiningConcierge', 'version': 'DRAFT', 'localeId': 'en_US', 'id': 'XYWRSPCNFB'}}

    response = lambda_handler(event, None)
    tmp = 1