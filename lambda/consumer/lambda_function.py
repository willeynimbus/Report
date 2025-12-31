import json
import boto3
import os
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')
sts = boto3.client('sts')

# Environment variables
S3_BUCKET = os.environ['S3_BUCKET_NAME']
S3_PREFIX = os.environ['S3_PREFIX']

def lambda_handler(event, context):
    """
    Consumer Lambda - processes SQS messages and collects network data
    """
    print(f"Received {len(event['Records'])} messages from SQS")
    
    processed_count = 0
    failed_count = 0
    
    for record in event['Records']:
        try:
            # Parse SNS message from SQS
            message_body = json.loads(record['body'])
            sns_message = json.loads(message_body['Message'])
            
            print(f"Processing account: {sns_message['account_id']}, region: {sns_message['region']}")
            
            # Collect network data
            network_data = collect_network_data(sns_message)
            
            # Store in S3
            store_to_s3(network_data)
            
            processed_count += 1
            print(f"Successfully processed account {sns_message['account_id']}")
            
        except Exception as e:
            failed_count += 1
            print(f"Error processing message: {str(e)}")
            import traceback
            traceback.print_exc()
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'failed': failed_count
        })
    }

def collect_network_data(event_payload):
    """
    Collect network data from the target account and region
    """
    account_id = event_payload['account_id']
    region = event_payload['region']
    account_name = event_payload['account_name']
    business_unit = event_payload.get('business_unit')
    
    # Get EC2 client
    ec2_client = get_ec2_client(account_id, region)
    
    # Collect network resources
    network_data = {
        'account_id': account_id,
        'account_name': account_name,
        'business_unit': business_unit,
        'region': region,
        'collection_timestamp': datetime.utcnow().isoformat() + 'Z',
        'network': {
            'vpcs': collect_vpcs(ec2_client),
            'subnets': collect_subnets(ec2_client),
            'security_groups': collect_security_groups(ec2_client)
        }
    }
    
    return network_data

def get_ec2_client(account_id, region):
    """
    Get EC2 client for target account
    """
    current_account = sts.get_caller_identity()['Account']
    
    if account_id == current_account:
        return boto3.client('ec2', region_name=region)
    
    role_arn = f"arn:aws:iam::{account_id}:role/NetworkDataCollectionReadOnly"
    
    try:
        assumed_role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=f"NetworkDataCollection-{account_id}-{region}"
        )
        
        credentials = assumed_role['Credentials']
        
        return boto3.client(
            'ec2',
            region_name=region,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
    except Exception as e:
        print(f"Error assuming role: {str(e)}")
        raise

def collect_vpcs(ec2_client):
    """Collect VPC information"""
    try:
        response = ec2_client.describe_vpcs()
        return [
            {
                'vpc_id': vpc['VpcId'],
                'cidr_block': vpc['CidrBlock'],
                'state': vpc['State'],
                'is_default': vpc.get('IsDefault', False),
                'tags': vpc.get('Tags', [])
            }
            for vpc in response['Vpcs']
        ]
    except Exception as e:
        print(f"Error collecting VPCs: {str(e)}")
        return []

def collect_subnets(ec2_client):
    """Collect subnet information"""
    try:
        response = ec2_client.describe_subnets()
        return [
            {
                'subnet_id': subnet['SubnetId'],
                'vpc_id': subnet['VpcId'],
                'cidr_block': subnet['CidrBlock'],
                'availability_zone': subnet['AvailabilityZone'],
                'available_ip_count': subnet['AvailableIpAddressCount'],
                'tags': subnet.get('Tags', [])
            }
            for subnet in response['Subnets']
        ]
    except Exception as e:
        print(f"Error collecting subnets: {str(e)}")
        return []

def collect_security_groups(ec2_client):
    """Collect Security Group information"""
    try:
        response = ec2_client.describe_security_groups()
        return [
            {
                'sg_id': sg['GroupId'],
                'sg_name': sg['GroupName'],
                'vpc_id': sg.get('VpcId', 'N/A'),
                'description': sg['Description'],
                'ingress_rules_count': len(sg.get('IpPermissions', [])),
                'egress_rules_count': len(sg.get('IpPermissionsEgress', [])),
                'tags': sg.get('Tags', [])
            }
            for sg in response['SecurityGroups']
        ]
    except Exception as e:
        print(f"Error collecting security groups: {str(e)}")
        return []

def store_to_s3(network_data):
    """
    Store collected network data to S3 as flat JSON files
    """
    account_id = network_data['account_id']
    region = network_data['region']
    account_name = network_data['account_name']
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    
    # Base metadata for all records
    base_metadata = {
        'account_id': account_id,
        'account_name': account_name,
        'business_unit': network_data.get('business_unit'),
        'region': region,
        'collection_timestamp': network_data['collection_timestamp']
    }
    
    # Store VPCs
    if network_data['network']['vpcs']:
        vpc_records = []
        for vpc in network_data['network']['vpcs']:
            record = {**base_metadata, **vpc}
            vpc_records.append(record)
        
        store_flat_file(
            records=vpc_records,
            resource_type='vpcs',
            account_id=account_id,
            region=region,
            timestamp=timestamp
        )
    
    # Store Subnets
    if network_data['network']['subnets']:
        subnet_records = []
        for subnet in network_data['network']['subnets']:
            record = {**base_metadata, **subnet}
            subnet_records.append(record)
        
        store_flat_file(
            records=subnet_records,
            resource_type='subnets',
            account_id=account_id,
            region=region,
            timestamp=timestamp
        )
    
    # Store Security Groups
    if network_data['network']['security_groups']:
        sg_records = []
        for sg in network_data['network']['security_groups']:
            record = {**base_metadata, **sg}
            sg_records.append(record)
        
        store_flat_file(
            records=sg_records,
            resource_type='security_groups',
            account_id=account_id,
            region=region,
            timestamp=timestamp
        )
    
    print(f"✓ Stored all network data for account {account_id} in region {region}")

def store_flat_file(records, resource_type, account_id, region, timestamp):
    """
    Store a flat JSON file (one line per record - JSONL format)
    """
    # Create S3 key
    s3_key = f"{S3_PREFIX}{resource_type}/account={account_id}/region={region}/data-{timestamp}.json"
    
    # Convert to JSONL format (one JSON object per line)
    jsonl_data = '\n'.join([json.dumps(record, default=str) for record in records])
    
    # Upload to S3
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=jsonl_data,
            ContentType='application/json'
        )
        print(f"  ✓ Stored {len(records)} {resource_type} records")
    except Exception as e:
        print(f"  ✗ Error storing {resource_type}: {str(e)}")
        raise
