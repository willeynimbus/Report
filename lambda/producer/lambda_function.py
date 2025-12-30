import json
import boto3
import os
from datetime import datetime
from decimal import Decimal

# Initialize AWS clients with explicit region
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
sns = boto3.client('sns', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

# Environment variables
TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    """
    Main Lambda handler - orchestrates the network data collection process
    """
    print(f"Starting network data collection process at {datetime.utcnow().isoformat()}")
    
    try:
        # Step 1: Scan DynamoDB for account metadata
        accounts = scan_account_metadata()
        print(f"Retrieved {len(accounts)} total accounts from DynamoDB")
        
        # Step 2: Filter active accounts
        active_accounts = filter_active_accounts(accounts)
        print(f"Filtered to {len(active_accounts)} active accounts")
        
        # Step 3: Expand accounts by regions and publish events
        total_events = 0
        success_count = 0
        failure_count = 0
        
        for account in active_accounts:
            account_region_pairs = expand_account_regions(account)
            
            for account_data, region in account_region_pairs:
                try:
                    payload = create_event_payload(account_data, region)
                    publish_to_sns(payload)
                    success_count += 1
                    total_events += 1
                    print(f"Published event for account {account_data['account_id']} in region {region}")
                except Exception as e:
                    failure_count += 1
                    total_events += 1
                    print(f"Failed to publish event for account {account_data['account_id']} in region {region}: {str(e)}")
        
        # Return summary
        result = {
            'statusCode': 200,
            'body': {
                'message': 'Network data collection process completed',
                'total_accounts': len(accounts),
                'active_accounts': len(active_accounts),
                'total_events_published': success_count,
                'failed_events': failure_count,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        print(f"Process completed: {json.dumps(result['body'])}")
        return result
        
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': {
                'message': 'Error in network data collection process',
                'error': str(e)
            }
        }

def scan_account_metadata():
    """
    Scan DynamoDB table to retrieve all account metadata
    """
    table = dynamodb.Table(TABLE_NAME)
    
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        
        return items
    except Exception as e:
        print(f"Error scanning DynamoDB: {str(e)}")
        raise

def filter_active_accounts(accounts):
    """
    Filter accounts to only include active ones
    """
    return [acc for acc in accounts if acc.get('status', '').lower() == 'active']

def expand_account_regions(account):
    """
    Expand account into (account, region) pairs
    """
    regions = account.get('regions', [])
    
    # Handle both list and set types
    if isinstance(regions, set):
        regions = list(regions)
    
    return [(account, region) for region in regions]

def create_event_payload(account, region):
    """
    Create event payload for SNS message
    """
    return {
        'account_id': account['account_id'],
        'account_name': account.get('account_name', 'Unknown'),
        'business_unit': account.get('business_unit'),
        'region': region,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

def publish_to_sns(payload):
    """
    Publish event to SNS topic
    """
    try:
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(payload, default=str),
            Subject=f"Network Data Collection - {payload['account_id']} - {payload['region']}"
        )
        return response
    except Exception as e:
        print(f"Error publishing to SNS: {str(e)}")
        raise
