import time
import boto3

from logger import setup_logger

LAMBDA_NAMES = [
    'registry-contacts-api', 
    'registrar-domains-worker', 
    'registrar-contacts-api',
    'registrar-config-api',
    'registry-domains-api'
    # Add more lambda names here
]

class OldLambdaStacksCleaner:
    def __init__(self):
        self.cloudformation = boto3.client('cloudformation',
                                           region_name="YOUR_REGION",
                                           aws_access_key_id='YOUR_ACCESS_KEY',
                                           aws_secret_access_key='YOUR_SECRET_KEY',
                                           aws_session_token='YOUR_SESSION_TOKEN')
        self.num_recent_versions_to_keep = 10
        self.logger = setup_logger(__name__)

    def clean_old_stacks(self):
        '''
        Delete old lambda stacks for given lambda names using pagination
        '''
        lambda_stacks = {name: [] for name in LAMBDA_NAMES}
        deleted_stacks_count = 0
        paginator = self.cloudformation.get_paginator('list_stacks')
        for page in paginator.paginate(StackStatusFilter=['CREATE_COMPLETE']):
            for stack in page["StackSummaries"]:
                for name in LAMBDA_NAMES:
                    if "lambdaVersion-{}".format(name) in stack["StackName"]:
                        lambda_stacks[name].append(stack)
        
        for name, stacks in lambda_stacks.items():
            stacks.sort(key=lambda x: x["CreationTime"], reverse=True)
            for stack in stacks[self.num_recent_versions_to_keep:]:
                self.cloudformation.delete_stack(StackName=stack["StackName"])
                self.logger.info(f"{name} - deleting stack {stack['StackName']} created at {stack['CreationTime']}")
                deleted_stacks_count += 1
                # Sleep for 1 second to avoid throttling
                time.sleep(1)
        
        self.logger.info(f"Total stacks deleted: {deleted_stacks_count}")
        for name, stacks in lambda_stacks.items():
            if len(stacks) > self.num_recent_versions_to_keep:
                self.logger.info(f"Stacks deleted for {name}: {len(stacks) - self.num_recent_versions_to_keep}")
            else:
                self.logger.info(f"No stacks to delete for {name}")

if __name__ == "__main__":
    try:
        cleaner = OldLambdaStacksCleaner()
        cleaner.clean_old_stacks()
    except Exception as e:
        print(f"An error occurred: {str(e)}")