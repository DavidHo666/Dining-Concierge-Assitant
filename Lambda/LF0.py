import boto3
import json
from botocore.exceptions import ClientError
# from boto3.dynamodb.conditions import Key

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

# Define the client to interact with ASW Lambda
client_LF3 = boto3.client('lambda')

def lookup_data(key, db=None, table='last_search'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        # print(response)
        if 'Item' in response.keys():
            return response['Item']
        else:
            return ""
        


def insert_data(data_list, db=None, table='last_search'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response


def lambda_handler(event, context):
    print(event)
    # msg_from_user = event['messages'][0]["unstructured"]["text"]

    # change this to the message that user submits on 
    # your website using the 'event' variable
    # msg_from_user = "Hello"
    
    msg_from_user=''
    for message in event['messages']:
        msg_from_user += message['unstructured']['text'] + ' '

    print(f"Message from frontend: {msg_from_user}")

    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='XYWRSPCNFB', # MODIFY HERE
            # botAliasId='MAUQZ0D8KP',  # version 2
            botAliasId='TSTALIASID', # draft version
            localeId='en_US',
            sessionId='test_session',
            text=msg_from_user)
    
    
    msg_from_lex = response.get('messages', [])
    session_intent = response.get('interpretations',[])[0]['intent']
    
    if msg_from_lex:
        
        response_from_lex=''
        for message in msg_from_lex:
            response_from_lex +=  message['content'] + ' '
            
        print(f"Message from Chatbot: {response_from_lex}")
        # print(f"Chatbot's sessionIntent: {session_intent['name']}")
        # print(session_intent['slots'])
        
        # extract recognized email, cuisine category and location from lex
        category = ''
        location = ''
        request_id = ''
        request_date = ''
        user_email = ''
        flag = 0
        if session_intent['slots'] != {} and session_intent['slots']['party_size'] == None:
            if session_intent['slots']['email'] != None:
                user_email = session_intent['slots']['email']['value']['resolvedValues'][0]
                
            if session_intent['slots']['email'] != None and session_intent['slots']['location'] == None:
                # check whether this is a new user 
                print(user_email)
        
                tuples = lookup_data({'user_email': user_email})
                old_location = ""
                old_category = ""
                response_from_LF3 = ""
                if(tuples): 
                    old_location = tuples["location"]
                    old_category = tuples["category"]
                    print("old user!")
                    print(old_location)
                    print(old_category)
                    # invoke LF3 here
                    #ref https://www.sqlshack.com/calling-an-aws-lambda-function-from-another-lambda-function/
                    input_event = {
                        "user_email": user_email,
                        "old_location": old_location,
                        "old_category": old_category
                    }
                    
                    # get response from LF3
                    response_LF = client_LF3.invoke(
                        FunctionName = 'arn:aws:lambda:us-east-1:267524565890:function:LF3',
                        InvocationType = 'RequestResponse',
                        Payload = json.dumps(input_event)
                    )
                    
                    response_from_LF3 = json.load(response_LF['Payload'])["body"][1:-1]
                    
                    print(response_from_LF3)
                    
                    # send email again to get correct functionality ############################
                    
                    response = client.recognize_text(
                    botId='XYWRSPCNFB', # MODIFY HERE
                    # botAliasId='MAUQZ0D8KP',  # version 2
                    botAliasId='TSTALIASID', # draft version
                    localeId='en_US',
                    sessionId='test_session',
                    text=" I need some restaurant suggestions.")
                    
                    response = client.recognize_text(
                    botId='XYWRSPCNFB', # MODIFY HERE
                    # botAliasId='MAUQZ0D8KP',  # version 2
                    botAliasId='TSTALIASID', # draft version
                    localeId='en_US',
                    sessionId='test_session',
                    text=user_email)
                    
                    msg_from_lex = response.get('messages', [])
                    session_intent = response.get('interpretations',[])[0]['intent']
    
                    response_from_lex = response_from_LF3 + ' ' + response_from_lex
                    
                else:
                    print("new user!")
                    
            if session_intent['slots']['location'] != None:
                location = session_intent['slots']['location']['value']['resolvedValues'][0]
                
            if session_intent['slots']['cuisine'] != None:
                category = session_intent['slots']['cuisine']['value']['resolvedValues'][0]
                request_date = response['ResponseMetadata']['HTTPHeaders']['date']
                flag = 1
            
        # store search info into DynamoDB
        if flag == 1:
            print("Ready to store last search info")

            print(location)
            print(category)
            print(request_date)
            print(user_email)
            last_search = [{
                'request_date': request_date,
                'location': location,
                'category': category,
                'user_email': user_email
                }]
            insert_data(last_search)
            
        
        #ref https://docs.aws.amazon.com/lexv2/latest/APIReference/API_runtime_RecognizeText.html#API_runtime_RecognizeText_ResponseSyntax
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
