import boto3
import json
import csv
import io
from datetime import datetime, timedelta
import os
from botocore.exceptions import ClientError, NoCredentialsError
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from functools import partial

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Thread-safe counter for progress tracking
class ThreadSafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
    
    @property
    def value(self):
        return self._value

def lambda_handler(event, context):
    """Main Lambda handler with parallel processing"""
    
    logger.info(f"Lambda function started at: {datetime.utcnow()}")
    start_time = datetime.utcnow()
    
    config = get_configuration(event)
    validation_result = validate_configuration(config)
    
    if not validation_result['valid']:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Configuration validation failed',
                'details': validation_result['errors']
            })
        }
    
    try:
        org_client = boto3.client('organizations')
        s3_client = boto3.client('s3')
        
        logger.info(f"Getting accounts for OU: {config['target_ou_id']}")
        account_list = get_accounts_in_ou(org_client, config['target_ou_id'])
        logger.info(f"Found {len(account_list)} accounts in OU")
        
        # Check remaining time
        remaining_time = get_remaining_time(context)
        if remaining_time < 120:  # Less than 2 minutes remaining
            logger.warning("Insufficient time remaining, triggering async processing")
            return trigger_async_processing(account_list, config)
        
        # Process accounts in parallel with optimized batch size
        all_inventory_data = []
        failed_accounts = []
        processed_counter = ThreadSafeCounter()
        
        # Determine optimal batch size based on account count and remaining time
        max_workers = min(10, len(account_list), max(1, remaining_time // 30))
        logger.info(f"Using {max_workers} parallel workers for {len(account_list)} accounts")
        
        # Process accounts in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all account processing tasks
            future_to_account = {
                executor.submit(process_account_optimized, account, config, processed_counter): account 
                for account in account_list
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_account, timeout=remaining_time-60):
                account = future_to_account[future]
                try:
                    account_inventory = future.result(timeout=30)
                    all_inventory_data.extend(account_inventory)
                    logger.info(f"Completed account {account['Id']} - Progress: {processed_counter.value}/{len(account_list)}")
                    
                    # Check remaining time periodically
                    if get_remaining_time(context) < 90:
                        logger.warning("Time running low, breaking early")
                        break
                        
                except Exception as e:
                    logger.error(f"Failed to process account {account['Id']}: {str(e)}")
                    failed_accounts.append({
                        'AccountId': account['Id'],
                        'AccountName': account['Name'],
                        'Error': str(e)
                    })
        
        # Store results
        report_key = store_inventory_to_s3_optimized(s3_client, all_inventory_data, failed_accounts, config)
        summary = generate_inventory_summary(all_inventory_data, failed_accounts, account_list)
        
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Execution completed in {execution_time:.2f} seconds")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'AWS inventory report generated successfully',
                'report_location': f"s3://{config['s3_bucket']}/{report_key}",
                'summary': summary,
                'execution_time_seconds': execution_time,
                'accounts_processed': processed_counter.value,
                'accounts_failed': len(failed_accounts)
            })
        }
        
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            })
        }

def get_remaining_time(context):
    """Get remaining execution time in seconds"""
    return (context.get_remaining_time_in_millis() / 1000) if context else 900

def trigger_async_processing(account_list, config):
    """Trigger Step Functions for async processing of large account lists"""
    try:
        stepfunctions_client = boto3.client('stepfunctions')
        
        # Split accounts into batches for parallel processing
        batch_size = 5  # Process 5 accounts per batch
        batches = [account_list[i:i + batch_size] for i in range(0, len(account_list), batch_size)]
        
        execution_name = f"inventory-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        
        # Start Step Function execution
        response = stepfunctions_client.start_execution(
            stateMachineArn=os.environ.get('STEP_FUNCTION_ARN'),
            name=execution_name,
            input=json.dumps({
                'batches': batches,
                'config': config,
                'total_accounts': len(account_list)
            })
        )
        
        return {
            'statusCode': 202,
            'body': json.dumps({
                'message': 'Large inventory processing started asynchronously',
                'execution_arn': response['executionArn'],
                'total_accounts': len(account_list),
                'total_batches': len(batches)
            })
        }
    except Exception as e:
        logger.error(f"Error triggering async processing: {str(e)}")
        raise

def process_account_optimized(account_info, config, counter):
    """Optimized account processing with selective service collection"""
    
    account_inventory = []
    
    try:
        # Assume role in target account
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account_info['Id']}:role/{config['cross_account_role_name']}",
            RoleSessionName=f"FastInventory-{account_info['Id']}",
            ExternalId=config['external_id']
        )
        
        credentials = assumed_role['Credentials']
        
        # Get priority regions (focus on main regions first)
        priority_regions = get_priority_regions(credentials, config)
        
        # Process regions in parallel
        with ThreadPoolExecutor(max_workers=3) as region_executor:
            region_futures = {
                region_executor.submit(collect_region_inventory_fast, credentials, region, account_info, config): region 
                for region in priority_regions
            }
            
            for future in as_completed(region_futures, timeout=120):
                try:
                    region_inventory = future.result(timeout=30)
                    account_inventory.extend(region_inventory)
                except Exception as e:
                    region = region_futures[future]
                    logger.error(f"Error processing region {region} for account {account_info['Id']}: {str(e)}")
        
        counter.increment()
        return account_inventory
        
    except Exception as e:
        logger.error(f"Error processing account {account_info['Id']}: {str(e)}")
        counter.increment()
        raise

def get_priority_regions(credentials, config):
    """Get priority regions for faster processing"""
    
    if config['regions_to_scan'] != 'all':
        return [r.strip() for r in config['regions_to_scan'].split(',') if r.strip()]
    
    # Priority regions (most commonly used)
    priority_regions = [
        'us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1', 
        'us-east-2', 'eu-central-1', 'ap-northeast-1'
    ]
    
    try:
        # Get all available regions
        ec2_client = boto3.client(
            'ec2', region_name='us-east-1',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )
        all_regions = [region['RegionName'] for region in ec2_client.describe_regions()['Regions']]
        
        # Return priority regions first, then others
        other_regions = [r for r in all_regions if r not in priority_regions]
        return priority_regions + other_regions[:5]  # Limit to top 12 regions total
        
    except Exception:
        return priority_regions

def collect_region_inventory_fast(credentials, region, account_info, config):
    """Fast region inventory collection focusing on key services"""
    
    region_inventory = []
    
    try:
        # Create clients for high-priority services only
        clients = create_priority_clients(credentials, region)
        
        # Fast collection functions for key services
        fast_collectors = [
            collect_ec2_fast,
            collect_s3_fast,
            collect_rds_fast,
            collect_lambda_fast,
            collect_ecs_fast,
            collect_elb_fast
        ]
        
        # Process services in parallel
        with ThreadPoolExecutor(max_workers=3) as service_executor:
            service_futures = {
                service_executor.submit(collector, clients, region, account_info, config): collector.__name__
                for collector in fast_collectors
            }
            
            for future in as_completed(service_futures, timeout=60):
                try:
                    service_inventory = future.result(timeout=15)
                    region_inventory.extend(service_inventory)
                except Exception as e:
                    service_name = service_futures[future]
                    logger.error(f"Error in {service_name} for region {region}: {str(e)}")
        
        return region_inventory
        
    except Exception as e:
        logger.error(f"Error collecting inventory from region {region}: {str(e)}")
        return []

def create_priority_clients(credentials, region):
    """Create clients for priority services only"""
    
    client_config = {
        'region_name': region,
        'aws_access_key_id': credentials['AccessKeyId'],
        'aws_secret_access_key': credentials['SecretAccessKey'],
        'aws_session_token': credentials['SessionToken']
    }
    
    return {
        'ec2': boto3.client('ec2', **client_config),
        's3': boto3.client('s3', **client_config),
        'rds': boto3.client('rds', **client_config),
        'lambda': boto3.client('lambda', **client_config),
        'ecs': boto3.client('ecs', **client_config),
        'elbv2': boto3.client('elbv2', **client_config)
    }

def collect_ec2_fast(clients, region, account_info, config):
    """Fast EC2 collection with pagination limits"""
    inventory = []
    try:
        paginator = clients['ec2'].get_paginator('describe_instances')
        page_count = 0
        
        for page in paginator.paginate():
            if page_count >= 10:  # Limit pages for speed
                break
                
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    # Skip terminated instances for speed
                    if instance['State']['Name'] == 'terminated':
                        continue
                        
                    tags = {}
                    if config['include_tags'] and instance.get('Tags'):
                        tags = {tag['Key']: tag['Value'] for tag in instance['Tags'][:5]}  # Limit tags
                    
                    inventory.append({
                        'AccountId': account_info['Id'],
                        'AccountName': account_info['Name'],
                        'Region': region,
                        'Service': 'EC2',
                        'ResourceType': 'Instance',
                        'ResourceId': instance['InstanceId'],
                        'ResourceName': tags.get('Name', instance['InstanceId']),
                        'State': instance['State']['Name'],
                        'InstanceType': instance['InstanceType'],
                        'Platform': instance.get('Platform', 'Linux/Unix'),
                        'VpcId': instance.get('VpcId', 'N/A'),
                        'PrivateIpAddress': instance.get('PrivateIpAddress', 'N/A'),
                        'PublicIpAddress': instance.get('PublicIpAddress', 'N/A'),
                        'LaunchTime': instance.get('LaunchTime', '').isoformat() if instance.get('LaunchTime') else 'N/A',
                        'Tags': json.dumps(tags) if tags else 'N/A'
                    })
            page_count += 1
            
    except Exception as e:
        logger.error(f"Error in EC2 fast collection: {str(e)}")
    
    return inventory

def collect_s3_fast(clients, region, account_info, config):
    """Fast S3 collection (global service, collect only from us-east-1)"""
    inventory = []
    
    if region != 'us-east-1':
        return inventory
        
    try:
        response = clients['s3'].list_buckets()
        
        # Limit to first 100 buckets for speed
        for bucket in response['Buckets'][:100]:
            try:
                inventory.append({
                    'AccountId': account_info['Id'],
                    'AccountName': account_info['Name'],
                    'Region': 'Global',
                    'Service': 'S3',
                    'ResourceType': 'Bucket',
                    'ResourceId': bucket['Name'],
                    'ResourceName': bucket['Name'],
                    'State': 'Active',
                    'InstanceType': 'N/A',
                    'Platform': 'N/A',
                    'VpcId': 'N/A',
                    'PrivateIpAddress': 'N/A',
                    'PublicIpAddress': 'N/A',
                    'LaunchTime': bucket['CreationDate'].isoformat(),
                    'Tags': 'N/A'
                })
            except Exception:
                continue
                
    except Exception as e:
        logger.error(f"Error in S3 fast collection: {str(e)}")
    
    return inventory

def collect_rds_fast(clients, region, account_info, config):
    """Fast RDS collection"""
    inventory = []
    try:
        response = clients['rds'].describe_db_instances(MaxRecords=50)  # Limit for speed
        
        for db_instance in response['DBInstances']:
            inventory.append({
                'AccountId': account_info['Id'],
                'AccountName': account_info['Name'],
                'Region': region,
                'Service': 'RDS',
                'ResourceType': 'DB Instance',
                'ResourceId': db_instance['DBInstanceIdentifier'],
                'ResourceName': db_instance['DBInstanceIdentifier'],
                'State': db_instance['DBInstanceStatus'],
                'InstanceType': db_instance['DBInstanceClass'],
                'Platform': db_instance['Engine'],
                'VpcId': db_instance.get('DBSubnetGroup', {}).get('VpcId', 'N/A'),
                'PrivateIpAddress': db_instance.get('Endpoint', {}).get('Address', 'N/A'),
                'PublicIpAddress': 'N/A',
                'LaunchTime': db_instance.get('InstanceCreateTime', '').isoformat() if db_instance.get('InstanceCreateTime') else 'N/A',
                'Tags': 'N/A'
            })
            
    except Exception as e:
        logger.error(f"Error in RDS fast collection: {str(e)}")
    
    return inventory

def collect_lambda_fast(clients, region, account_info, config):
    """Fast Lambda collection"""
    inventory = []
    try:
        response = clients['lambda'].list_functions(MaxItems=50)  # Limit for speed
        
        for function in response['Functions']:
            inventory.append({
                'AccountId': account_info['Id'],
                'AccountName': account_info['Name'],
                'Region': region,
                'Service': 'Lambda',
                'ResourceType': 'Function',
                'ResourceId': function['FunctionName'],
                'ResourceName': function['FunctionName'],
                'State': function['State'],
                'InstanceType': 'N/A',
                'Platform': function['Runtime'],
                'VpcId': function.get('VpcConfig', {}).get('VpcId', 'N/A'),
                'PrivateIpAddress': 'N/A',
                'PublicIpAddress': 'N/A',
                'LaunchTime': function.get('LastModified', 'N/A'),
                'Tags': 'N/A'
            })
            
    except Exception as e:
        logger.error(f"Error in Lambda fast collection: {str(e)}")
    
    return inventory

def collect_ecs_fast(clients, region, account_info, config):
    """Fast ECS collection"""
    inventory = []
    try:
        clusters = clients['ecs'].list_clusters(maxResults=20)  # Limit for speed
        
        if clusters['clusterArns']:
            cluster_details = clients['ecs'].describe_clusters(clusters=clusters['clusterArns'])
            for cluster in cluster_details['clusters']:
                inventory.append({
                    'AccountId': account_info['Id'],
                    'AccountName': account_info['Name'],
                    'Region': region,
                    'Service': 'ECS',
                    'ResourceType': 'Cluster',
                    'ResourceId': cluster['clusterName'],
                    'ResourceName': cluster['clusterName'],
                    'State': cluster['status'],
                    'InstanceType': 'N/A',
                    'Platform': 'N/A',
                    'VpcId': 'N/A',
                    'PrivateIpAddress': 'N/A',
                    'PublicIpAddress': 'N/A',
                    'LaunchTime': 'N/A',
                    'Tags': 'N/A'
                })
                
    except Exception as e:
        logger.error(f"Error in ECS fast collection: {str(e)}")
    
    return inventory

def collect_elb_fast(clients, region, account_info, config):
    """Fast ELB collection"""
    inventory = []
    try:
        response = clients['elbv2'].describe_load_balancers(PageSize=20)  # Limit for speed
        
        for lb in response['LoadBalancers']:
            inventory.append({
                'AccountId': account_info['Id'],
                'AccountName': account_info['Name'],
                'Region': region,
                'Service': 'ELB',
                'ResourceType': lb['Type'].upper() + ' Load Balancer',
                'ResourceId': lb['LoadBalancerName'],
                'ResourceName': lb['LoadBalancerName'],
                'State': lb['State']['Code'],
                'InstanceType': 'N/A',
                'Platform': 'N/A',
                'VpcId': lb.get('VpcId', 'N/A'),
                'PrivateIpAddress': 'N/A',
                'PublicIpAddress': lb.get('DNSName', 'N/A'),
                'LaunchTime': lb.get('CreatedTime', '').isoformat() if lb.get('CreatedTime') else 'N/A',
                'Tags': 'N/A'
            })
            
    except Exception as e:
        logger.error(f"Error in ELB fast collection: {str(e)}")
    
    return inventory

def store_inventory_to_s3_optimized(s3_client, all_inventory_data, failed_accounts, config):
    """Optimized S3 storage with streaming"""
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    report_key = f"aws-inventory-reports/{timestamp}/aws_inventory_report.csv"
    
    try:
        if all_inventory_data:
            # Stream CSV to S3 for large datasets
            csv_buffer = io.StringIO()
            fieldnames = [
                'AccountId', 'AccountName', 'Region', 'Service', 'ResourceType', 
                'ResourceId', 'ResourceName', 'State', 'InstanceType', 'Platform', 
                'VpcId', 'PrivateIpAddress', 'PublicIpAddress', 'LaunchTime', 'Tags'
            ]
            
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write in chunks to manage memory
            chunk_size = 1000
            for i in range(0, len(all_inventory_data), chunk_size):
                chunk = all_inventory_data[i:i + chunk_size]
                writer.writerows(chunk)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=config['s3_bucket'],
                Key=report_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Inventory report uploaded: s3://{config['s3_bucket']}/{report_key}")
        
        # Store failed accounts
        if failed_accounts:
            error_key = f"aws-inventory-reports/{timestamp}/failed_accounts.json"
            s3_client.put_object(
                Bucket=config['s3_bucket'],
                Key=error_key,
                Body=json.dumps(failed_accounts, indent=2),
                ContentType='application/json'
            )
        
        return report_key
        
    except Exception as e:
        logger.error(f"Error storing inventory to S3: {str(e)}")
        raise

# Keep the existing helper functions
def get_configuration(event):
    return {
        'target_ou_id': event.get('ou_id') or os.environ.get('TARGET_OU_ID'),
        's3_bucket': os.environ.get('S3_BUCKET_NAME'),
        'cross_account_role_name': os.environ.get('CROSS_ACCOUNT_ROLE_NAME', 'AWSInventoryCrossAccountRole'),
        'external_id': os.environ.get('EXTERNAL_ID', 'EC2ReportingAccess'),
        'regions_to_scan': os.environ.get('REGIONS_TO_SCAN', 'all'),
        'include_tags': os.environ.get('INCLUDE_TAGS', 'true').lower() == 'true',
        'report_format': os.environ.get('REPORT_FORMAT', 'csv')
    }

def validate_configuration(config):
    errors = []
    if not config['target_ou_id']:
        errors.append('TARGET_OU_ID is required')
    if not config['s3_bucket']:
        errors.append('S3_BUCKET_NAME is required')
    return {'valid': len(errors) == 0, 'errors': errors}

def get_accounts_in_ou(org_client, ou_id):
    accounts = []
    try:
        paginator = org_client.get_paginator('list_accounts_for_parent')
        for page in paginator.paginate(ParentId=ou_id):
            for account in page['Accounts']:
                if account['Status'] == 'ACTIVE':
                    accounts.append({
                        'Id': account['Id'],
                        'Name': account['Name'],
                        'Email': account['Email']
                    })
    except Exception as e:
        logger.error(f"Error getting accounts for OU {ou_id}: {str(e)}")
        raise
    return accounts

def generate_inventory_summary(all_inventory_data, failed_accounts, account_list):
    summary = {
        'total_accounts_processed': len(account_list),
        'total_accounts_failed': len(failed_accounts),
        'total_resources_found': len(all_inventory_data),
        'report_generated_at': datetime.utcnow().isoformat()
    }
    
    if all_inventory_data:
        services = {}
        regions = {}
        
        for resource in all_inventory_data:
            service = resource['Service']
            region = resource['Region']
            services[service] = services.get(service, 0) + 1
            regions[region] = regions.get(region, 0) + 1
        
        summary['resources_by_service'] = services
        summary['resources_by_region'] = regions
    
    return summary
