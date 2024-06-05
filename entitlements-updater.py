import boto3
from logger import setup_logger

class EntitlementsUpdater:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name="us-west-2",
                                       aws_access_key_id='',
                                       aws_secret_access_key='',
                                       aws_session_token='')
        self.table = self.dynamodb.Table('entitlements')
        self.index = 0
        self.failed_retrieved_entitlements = []
        self.unmirgrated_entitlements = []
        self.mirgrated_entitlements = []
        self.migratedEntitlementIDs = []
        

    def get_entitlement(self, pk, sk):
        params = {
            'Key': {
                'ID': pk,
                'clockID': sk
            }
        }
        response = self.table.get_item(**params)
        if 'Item' not in response:
            self.failed_retrieved_entitlements.append({'ID': pk, 'clockID': sk})
            return None
        return response

    def is_migrated_domain(self, response):
        raw_json = response['Item']['rawJsonRecord']
        return 'ENTITLEMENT_UPDATE' in raw_json and 'INTERNAL_UPDATE' in raw_json and 'leasedDomain' not in raw_json

    def update_event_state(self, item):
        self.logger.info(f"Entitlement ID: {item['ID']}, Clock ID: {item['clockID']}")
        self.table.update_item(
            Key={
                'ID': item['ID'],
                'clockID': item['clockID']
            },
            UpdateExpression='SET #eventState = :val1',
            ExpressionAttributeValues={
                ':val1': 'COMPLETED'
            },
            ExpressionAttributeNames={
                '#eventState': 'eventState'
            }
        )

    def run(self, keys):
        for key in keys:
            pk = key['ID']
            sk = key['clockID']
            response = self.get_entitlement(pk, sk)
            if response is None:
                continue
            if self.is_migrated_domain(response):
                if response['Item']['eventState'] == 'FAILED':
                    self.update_event_state(response['Item'])
                self.mirgrated_entitlements.append({'ID': pk, 'clockID': sk})
            else:
                self.logger.info(f"Entitlement is not a migrated domain: {pk}, {sk}")
                self.unmirgrated_entitlements.append({'ID': pk, 'clockID': sk})

        
        for failedEntitlement in self.failed_retrieved_entitlements:
            self.logger.error(f"Failed to get entitlement: {failedEntitlement['ID']}, {failedEntitlement['clockID']}")

        print("Other Type Entitlements: {}".format(self.unmirgrated_entitlements))
        print("Migrated Entitlements: {}".format(self.mirgrated_entitlements))
        for migratedEntitlement in self.mirgrated_entitlements:
            self.migratedEntitlementIDs.append(migratedEntitlement['ID'])
        print("Migrated Entitlement IDs: {}".format(self.migratedEntitlementIDs))
    
    # Format the keys from the retry logs
    # from [ID=f7645e3b-d593-4823-9388-c26dd2fba424, clockID=814322262 ID=8030ffae-13a6-4f8e-9725-a0c0c9514142, clockID=814358649 ID=1077ecea-2e3c-47ac-b2fd-ec39fe6c6439, clockID=814377492]
    # to [{'ID': 'f7645e3b-d593-4823-9388-c26dd2fba424', 'clockID': 814322262}, {'ID': '8030ffae-13a6-4f8e-9725-a0c0c9514142', 'clockID': 814358649} 'ID': '1077ecea-2e3c-47ac-b2fd-ec39fe6c6439', 'clockID': 814377492}]
    def format_keys(self, data_string):
        data_string = data_string.strip("[]")

        parts = data_string.split(" ID=")

        if parts[0].startswith("ID="):
            parts[0] = parts[0][3:]

        id_clockID_pairs = []

        for part in parts:
            id_part, clockID_part = part.split(", clockID=")
            id_value = id_part.strip()
            clockID_value = int(clockID_part.strip())
        
            id_clockID_pairs.append({'ID': id_value, 'clockID': clockID_value})

        return id_clockID_pairs

if __name__ == '__main__':
    try:
        # Get list of ID and clockID pairs from retry logs
        data_string = ""
        updater = EntitlementsUpdater()
        keys = updater.format_keys(data_string)
        updater.run(keys)
    except Exception as e:
        print(f"An error occurred: {str(e)}")