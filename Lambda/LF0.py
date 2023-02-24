import boto3
import json

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')


def lambda_handler(event, context):
    # msg_from_user = event['messages'][0]["unstructured"]["text"]

    # change this to the message that user submits on
    # your website using the 'event' variable
    # msg_from_user = "Hello"

    msg_from_user = ''
    for message in event['messages']:
        msg_from_user += message['unstructured']['text'] + ' '

    print(f"Message from frontend: {msg_from_user}")

    # Initiate conversation with Lex
    response = client.recognize_text(
        botId='XYWRSPCNFB',  # MODIFY HERE
        # botAliasId='MEBTCAUIGZ',  # cannot find this botAliasId
        botAliasId='TSTALIASID',  # MODIFY HERE (new policy added)
        localeId='en_US',
        sessionId='test_session',
        text=msg_from_user)

    msg_from_lex = response.get('messages', [])
    session_intent = response.get('interpretations', [])[0]['intent']

    if msg_from_lex:

        response_from_lex = ''
        for message in msg_from_lex:
            response_from_lex += message['content'] + ' '

        print(f"Message from Chatbot: {response_from_lex}")
        print(f"Chatbot's sessionIntent: {session_intent['name']}")

        # if session_intent['slots'] != None:
        #     if session_intent['slots']['cuisine'] != None:
        #         category = session_intent['slots']['cuisine']['value']['resolvedValues'][0]
        #         print(category)
        #     if session_intent['slots']['cuisine'] != None:
        #         location = session_intent['slots']['location']['value']['resolvedValues'][0]
        #         print(location)

        # ref https://docs.aws.amazon.com/lexv2/latest/APIReference/API_runtime_RecognizeText.html#API_runtime_RecognizeText_ResponseSyntax
        print(response)

        resp = {
            'statusCode': 200,
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "text": json.dumps(response_from_lex)
                        # ref: chat.js line 61-62
                    }
                }
            ]
        }

        # modify resp to send back the next question Lex would ask from the user

        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket

        return resp