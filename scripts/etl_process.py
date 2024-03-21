import sys, os
from dotenv import dotenv_values, load_dotenv
import boto3
import pandas as pd
load_dotenv()
sys.path.append("../")
region_name= os.getenv('region_name')
aws_access_key_id = os.getenv('aws_access_key_id') 
aws_secret_access_key = os.getenv('aws_secret_access_key')


def create_sns_client(region_name,aws_access_key_id,aws_secret_access_key):
    """ Function to get aws sns client"""
    sns_client = boto3.client('sns',
    region_name= region_name,
    aws_access_key_id = aws_access_key_id ,
    aws_secret_access_key = aws_secret_access_key )
    return sns_client

# Re-create the city_alerts topic using a oneliner
def create_sns_topic(topic_name,sns):
    """ Function to get aws sns topic"""
    topic_arn = sns.create_topic(Name=topic_name)['TopicArn']
    topics = sns.list_topics()['Topics']
    return topic_arn, topics

# Create list of departments
def create_sns_dep_topic(departments:  list, sns: str):
    for dept in departments:
        # For every department, create a general topic
        sns.create_topic(Name="{}_general".format(dept))
        print(f'==== {dept}_general successifully created=====')
        # For every department, create a critical topic
        sns.create_topic(Name="{}_critical".format(dept))
        print(f'==== {dept}_critical successifully created=====')
    # Print all the topics in SNS
    response = sns.list_topics()
    list_topics= response['Topics']
    return list_topics


# delete unwanted topics
def delete_unwanted_top(sns, pattern):
    '''
    - pattern: a string that is present in the 
    topic['TopicArn'] we want to maintain.
    '''
    # Get the current list of topics
    topics = sns.list_topics()['Topics']

    for topic in topics:
    # For each topic, if it is not marked for example critical, delete it
        if pattern not in topic['TopicArn']:
            sns.delete_topic(TopicArn=topic['TopicArn'])
        
    # Print the list of remaining critical topics
    print(sns.list_topics()['Topics'])


def subscribe_to(sns, topic_arn,protocol,endpoint ):
    '''
    - sns: sns client 
    - topic_arn: The ARN of the topic you want to subscribe to 
    - endpoint: actual phone number or email
    '''
    #  Subscribe Elena's phone number to streets_critical topic
    try:
        resp_sms = sns.subscribe(
                TopicArn = topic_arn, 
                Protocol=protocol,
                Endpoint=endpoint)
        resp_sms_arn= resp_sms['SubscriptionArn']
        # Print the SubscriptionArn
        print(f'Successfull subscribed to {endpoint} with response:  {resp_sms_arn} ')
        return 
    except Exception as e:
        print( f"Subscription failed because of {e}")
#  create a bucke(t
def publish_topic(sns, v_count, topic_patten,service_name):
    '''
    - sns: sns client 
    - streets_v_count: the minimun number ot count to have before publishing
    - topic_patten: The string in  the TopicArn you want to publish to
    - service_name: The activities that are concerned
    '''

    topics = sns.list_topics()['Topics']
    # look through the 
    list_topic_arns=[]
    for topic in topics:
       topic_arn = topic['TopicArn']
       list_topic_arns.append(topic_arn)
    #    print(topic_arn)
    # If there are over 100 potholes, create a message
    if v_count > 100:
        # The message should contain the number of potholes.
        message = "There are {} {}! Kindly act on this issue as soon as possible".format(v_count, service_name)
        # The email subject should also contain number of potholes
        subject = "Latest {} count is {}".format(v_count,service_name)
        for topc_arn in list_topic_arns:
            if topic_patten in  topc_arn:
        # Publish the email to for example streets_critical topic
                sns.publish(
                    TopicArn = topc_arn,
                    # Set subject and message
                    Message = message,
                    Subject = subject
                )

def publish_to_phones(sns,contacts: pd.DataFrame):

    # Loop through every row in contacts
    for _, row in contacts.iterrows():
        
        # Publish an ad-hoc sms to the user's phone number
        response = sns.publish(
            # Set the phone number
            PhoneNumber = str(row['Phone']),
            # The message should include the user's name
            Message = 'Hello {}'.format(row['Name'])
        )
        return (response)

if __name__ == "__main__":
    sns_client=create_sns_client(region_name,aws_access_key_id,aws_secret_access_key)
    # List only objects that start with '2018/final_'
    topic, topics =create_sns_topic('city_alerts',sns_client)   
    # print(topic,'\n', topics )  
    departments = ['trash', 'streets', 'water'] 
    topics = sns_client.list_topics()['Topics']
    print(topics)
    for topic in topics:
       topic_arn = topic['TopicArn']
       print(topic_arn)
    # get the topics
    
    # subcripbe to critical topics only
    list_topics= create_sns_dep_topic(departments, sns_client)
    for topic in list_topics:
        topic_arn = topic['TopicArn']
        if "critical" in topic_arn:
        # Subscribe Elena's email to streets_critical topic.
            subscribe_to(sns_client, 
                         topic_arn,
                         protocol='',
                         endpoint=''
                         )
    