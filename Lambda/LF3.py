import json
import boto3
import time

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

# return the response from lex
def get_response(msg):
    response = client.recognize_text(
    botId='XYWRSPCNFB', # MODIFY HERE
    # botAliasId='MAUQZ0D8KP',  # version 2
    botAliasId='TSTALIASID', # draft version
    localeId='en_US',
    sessionId='test_session',
    text=msg)
    return response

def lambda_handler(event, context):
    # TODO implement
    print(event)
    
    # extract information from event
    old_location = event["old_location"]
    old_category = event["old_category"]
    user_email = event["user_email"]
    print(old_location)
    print(old_category)
    print(user_email)
    
    # constructing messages here
    msg_from_lf0 = ["I need some restaurant suggestions."]
    msg_from_lf0.append(user_email)
    msg_from_lf0.append(old_location)
    msg_from_lf0.append(old_category)
    msg_from_lf0.append("Two")
    msg_from_lf0.append("Tomorrow")
    msg_from_lf0.append("7pm")
    
    print(msg_from_lf0)
    # get response from lex
    for i in range(0,7): # set timeout for LF3 to 20sec (default 3sec is not enough)
        # time.sleep(2)
        response = get_response(msg_from_lf0[i])
    
    msg_from_lex = response.get('messages', [])
    session_intent = response.get('interpretations',[])[0]['intent']
    
    if msg_from_lex:
        response_from_lex=''
        for message in msg_from_lex:
            response_from_lex +=  message['content']+ ' '
            
        print(f"Message from Chatbot: {response_from_lex}")
        
        
    return {
        'statusCode': 200,
        'body': json.dumps('A recommendation based on your previous search has been sent to your email!')
        }
