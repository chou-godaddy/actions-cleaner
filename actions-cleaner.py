import base64
import boto3
from boto3.dynamodb.types import TypeSerializer
from logger import setup_logger

class ActionsCleaner:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name="YOUR_REGION",
                                       aws_access_key_id='YOUR_ACCESS_KEY',
                                       aws_secret_access_key='YOUR_SECRET_KEY',
                                       aws_session_token='YOUR_SESSION_TOKEN')
        self.table = self.dynamodb.Table('Actions')
        self.serializer = TypeSerializer()
        self.index = 0

    def query_actions(self, last_evaluated_key=None):
        '''
        Query the Actions 25 per page by StatusIndex PENDING_INTERNAL with filter type DATABASE_UPDATE and customerId UUID 0
        '''
        params = {
            'IndexName': 'StatusIndex',
            'ExpressionAttributeNames': {
                '#status': 'status',
                '#type': 'type',
                '#customerId': 'customerId'
            },
            'ExpressionAttributeValues': {
                ':status': 'PENDING_INTERNAL',
                ':type': 'DATABASE_UPDATE',
                ':customerId': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            },
            'KeyConditionExpression': '#status = :status',
            'FilterExpression': '#type = :type AND #customerId = :customerId',
            'Limit': 25
        }
        if last_evaluated_key:
            params['ExclusiveStartKey'] = last_evaluated_key

        response = self.table.query(**params)
        return response

    def clean_actions(self):
        '''
        Query per page and update the status of each action to SUCCESS
        '''
        self.logger.info("Querying actions")
        response = self.query_actions()
        self.logger.info(f"Found {response['Count']} actions")
        items = response['Items']
        for item in items:
            self.update_action(self.index, item)
            self.index += 1
        while 'LastEvaluatedKey' in response:
            response = self.query_actions(response['LastEvaluatedKey'])
            items = response['Items']
            for item in items:
                self.update_action(self.index, item)
                self.index += 1
        self.logger.info(f"Cleaned up {self.index} actions")

    def update_action(self, i, item):
        '''
        Update the status of the action to SUCCESS
        '''
        self.logger.info(f"Index: {i}, Customer ID: {base64.b64encode(bytes(item['customerId'])).decode('utf-8')}, Action ID: {base64.b64encode(bytes(item['actionId'])).decode('utf-8')}")
        self.table.update_item(
            Key={
                'customerId': bytes(item['customerId']),
                'actionId': bytes(item['actionId'])
            },
            UpdateExpression='SET #status = :val1',
            ExpressionAttributeValues={
                ':val1': 'SUCCESS'
            },
            ExpressionAttributeNames={
                '#status': 'status'
            }
        )

if __name__ == '__main__':
    try:
        cleaner = ActionsCleaner()
        cleaner.clean_actions()
    except Exception as e:
        print(f"An error occurred: {str(e)}")