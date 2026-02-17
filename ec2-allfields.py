import boto3
import json
import csv
import io
from datetime import datetime, timedelta
import os
from botocore.exceptions import ClientError, NoCredentialsError
 
def lambda_handler(event, context):
    print(f"Lambda function started at: {datetime.utcnow()}")
    config = get_configuration(event)
    validation_result = validate_configuration(config)
    if not validation_result['valid']:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Configuration validation failed', 'details': validation_result['errors']})}
   
    try:
        org_client = boto3.client('organizations')
        s3_client = boto3.client('s3')
        account_list = get_accounts_in_ou(org_client, config['target_ou_id'])
        all_ec2_data = []
        failed_accounts = []
       
        for account in account_list:
            try:
                account_data = collect_account_ec2_data(account, config)
                all_ec2_data.extend(account_data)
            except Exception as e:
                failed_accounts.append({'AccountId': account['Id'], 'AccountName': account['Name'], 'Error': str(e)})
       
        report_key = store_report_to_s3(s3_client, all_ec2_data, failed_accounts, config)
        summary = generate_summary(all_ec2_data, failed_accounts, account_list)
        return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'report_location': f"s3://{config['s3_bucket']}/{report_key}", 'summary': summary})}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
 
def get_configuration(event):
    return {
        'target_ou_id': event.get('ou_id') or os.environ.get('TARGET_OU_ID'),
        's3_bucket': event.get('s3_bucket') or os.environ.get('S3_BUCKET_NAME'),
        'cross_account_role_name': os.environ.get('CROSS_ACCOUNT_ROLE_NAME', 'EC2CrossAccountReportingRole'),
        'external_id': os.environ.get('EXTERNAL_ID', 'EC2ReportingAccess'),
        'include_stopped_instances': os.environ.get('INCLUDE_STOPPED_INSTANCES', 'true').lower() == 'true',
        'regions_to_scan': os.environ.get('REGIONS_TO_SCAN', 'all'),
        'collect_metrics': os.environ.get('COLLECT_METRICS', 'true').lower() == 'true',
        'metrics_days': int(os.environ.get('METRICS_DAYS', '7')),
        'report_format': os.environ.get('REPORT_FORMAT', 'csv'),
        'include_tags': os.environ.get('INCLUDE_TAGS', 'true').lower() == 'true'
    }
 
def validate_configuration(config):
    errors = []
    if not config['target_ou_id']: errors.append('TARGET_OU_ID is required')
    if not config['s3_bucket']: errors.append('S3_BUCKET_NAME is required')
    return {'valid': len(errors) == 0, 'errors': errors}
def get_accounts_in_ou(org_client, ou_id):
    accounts = []
    paginator = org_client.get_paginator('list_accounts_for_parent')
    for page in paginator.paginate(ParentId=ou_id):
        for account in page['Accounts']:
            if account['Status'] == 'ACTIVE':
                accounts.append({'Id': account['Id'], 'Name': account['Name'], 'Email': account['Email']})
    return accounts
 
def collect_account_ec2_data(account_info, config):
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn=f"arn:aws:iam::{account_info['Id']}:role/{config['cross_account_role_name']}",
        RoleSessionName=f"EC2Reporting-{account_info['Id']}",
        ExternalId=config['external_id']
    )
    credentials = assumed_role['Credentials']
    regions = get_regions_to_scan(credentials, config)
    account_instances = []
    for region in regions:
        try:
            region_instances = collect_region_ec2_data(credentials, region, account_info, config)
            account_instances.extend(region_instances)
        except Exception as e:
            print(f"Error in region {region}: {str(e)}")
    return account_instances
 
def get_regions_to_scan(credentials, config):
    if config['regions_to_scan'] == 'all':
        ec2_client = boto3.client('ec2', region_name='us-east-1', aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
        return [region['RegionName'] for region in ec2_client.describe_regions()['Regions']]
    return [r.strip() for r in config['regions_to_scan'].split(',') if r.strip()]
 
def collect_region_ec2_data(credentials, region, account_info, config):
    ec2_client = boto3.client('ec2', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    cloudwatch_client = boto3.client('cloudwatch', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken']) if config['collect_metrics'] else None
   
    filters = [{'Name': 'instance-state-name', 'Values': ['running', 'pending', 'shutting-down', 'stopping']}] if not config['include_stopped_instances'] else []
    paginator = ec2_client.get_paginator('describe_instances')
    region_instances = []
   
    if filters:
        page_iterator = paginator.paginate(Filters=filters)
    else:
        page_iterator = paginator.paginate()
 
    for page in page_iterator:
        for reservation in page['Reservations']:
            for instance in reservation['Instances']:
                instance_data = extract_instance_details(instance, account_info, region, ec2_client, cloudwatch_client, config)
                region_instances.append(instance_data)
    return region_instances

def extract_instance_details(instance, account_info, region, ec2_client, cloudwatch_client, config):

    instance_id = instance['InstanceId']

    # ------------------------
    # AMI DETAILS
    # ------------------------
    image_name = "N/A"
    image_owner = "N/A"

    try:
        img = ec2_client.describe_images(ImageIds=[instance['ImageId']])['Images']
        if img:
            image_name = img[0].get('Name', 'N/A')
            image_owner = img[0].get('OwnerId', 'N/A')
    except:
        pass


    # ------------------------
    # VOLUMES
    # ------------------------
    volume_ids = []
    volume_types = []
    total_volume_size = 0

    try:
        vols = ec2_client.describe_volumes(
            Filters=[{'Name': 'attachment.instance-id', 'Values': [instance_id]}]
        )['Volumes']

        for v in vols:
            volume_ids.append(v['VolumeId'])
            volume_types.append(v['VolumeType'])
            total_volume_size += v['Size']
    except:
        pass


    # ------------------------
    # NETWORK INTERFACES
    # ------------------------
    eni_ids = []
    mac_addresses = []

    for eni in instance.get('NetworkInterfaces', []):
        eni_ids.append(eni['NetworkInterfaceId'])
        mac_addresses.append(eni.get('MacAddress', 'N/A'))


    # ------------------------
    # SECURITY GROUPS
    # ------------------------
    sg_ids = []
    sg_names = []

    for sg in instance.get('SecurityGroups', []):
        sg_ids.append(sg['GroupId'])
        sg_names.append(sg['GroupName'])


    # ------------------------
    # CPU CREDITS
    # ------------------------
    cpu_credits = "standard"

    try:
        credit = ec2_client.describe_instance_credit_specifications(
            InstanceIds=[instance_id]
        )['InstanceCreditSpecifications']

        if credit:
            cpu_credits = credit[0].get('CpuCredits', 'standard')
    except:
        pass


    # ------------------------
    # CORE DATA
    # ------------------------
    instance_data = {

        # Account
        'AccountId': account_info['Id'],
        'AccountName': account_info['Name'],
        'Region': region,

        # Instance
        'InstanceId': instance_id,
        'InstanceType': instance['InstanceType'],
        'State': instance['State']['Name'],
        'LaunchTime': instance.get('LaunchTime', '').isoformat(),
        'Architecture': instance.get('Architecture', 'N/A'),
        'Hypervisor': instance.get('Hypervisor', 'N/A'),
        'VirtualizationType': instance.get('VirtualizationType', 'N/A'),
        'EbsOptimized': instance.get('EbsOptimized', False),
        'EnaSupport': instance.get('EnaSupport', False),
        'RootDeviceType': instance.get('RootDeviceType', 'N/A'),

        # AMI
        'ImageId': instance['ImageId'],
        'ImageName': image_name,
        'ImageOwner': image_owner,

        # Network
        'VpcId': instance.get('VpcId', 'N/A'),
        'SubnetId': instance.get('SubnetId', 'N/A'),
        'PrivateIp': instance.get('PrivateIpAddress', 'N/A'),
        'PublicIp': instance.get('PublicIpAddress', 'N/A'),
        'PrivateDns': instance.get('PrivateDnsName', 'N/A'),
        'PublicDns': instance.get('PublicDnsName', 'N/A'),
        'NetworkInterfaceIds': ",".join(eni_ids),
        'MacAddresses': ",".join(mac_addresses),

        # Storage
        'VolumeIds': ",".join(volume_ids),
        'VolumeTypes': ",".join(volume_types),
        'TotalVolumeSizeGB': total_volume_size,

        # Security
        'SecurityGroupIds': ",".join(sg_ids),
        'SecurityGroupNames': ",".join(sg_names),

        # Placement
        'AvailabilityZone': instance['Placement']['AvailabilityZone'],
        'Tenancy': instance['Placement'].get('Tenancy', 'default'),
        'PlacementGroup': instance['Placement'].get('GroupName', 'N/A'),

        # IAM
        'IamInstanceProfile': instance.get('IamInstanceProfile', {}).get('Arn', 'N/A'),

        # Monitoring
        'Monitoring': instance.get('Monitoring', {}).get('State', 'disabled'),

        # Credits
        'CpuCredits': cpu_credits
    }


    # ------------------------
    # TAGS
    # ------------------------
    if config['include_tags']:
        tags = {t['Key']: t['Value'] for t in instance.get('Tags', [])}
        instance_data['Tags'] = json.dumps(tags)


    # ------------------------
    # METRICS
    # ------------------------
    if config['collect_metrics'] and cloudwatch_client and instance['State']['Name'] == 'running':
        try:
            metrics = get_instance_metrics(
                cloudwatch_client,
                instance_id,
                config['metrics_days']
            )
            instance_data.update(metrics)

        except:
            instance_data.update({
                'AvgCpuUtilization': 'N/A',
                'MaxCpuUtilization': 'N/A'
            })

    else:
        instance_data.update({
            'AvgCpuUtilization': 'N/A',
            'MaxCpuUtilization': 'N/A'
        })


    return instance_data
 
def get_instance_metrics(cloudwatch_client, instance_id, days):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
   
    cpu_response = cloudwatch_client.get_metric_statistics(
        Namespace='AWS/EC2', MetricName='CPUUtilization',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=start_time, EndTime=end_time, Period=3600, Statistics=['Average', 'Maximum']
    )
   
    if cpu_response['Datapoints']:
        avg_cpu = sum(dp['Average'] for dp in cpu_response['Datapoints']) / len(cpu_response['Datapoints'])
        max_cpu = max(dp['Maximum'] for dp in cpu_response['Datapoints'])
        return {'AvgCpuUtilization': round(avg_cpu, 2), 'MaxCpuUtilization': round(max_cpu, 2)}
    return {'AvgCpuUtilization': 0, 'MaxCpuUtilization': 0}
def store_report_to_s3(s3_client, all_ec2_data, failed_accounts, config):
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    report_key = f"ec2-usage-reports/{timestamp}/ec2_usage_report.{config['report_format']}"
   
    if config['report_format'] == 'csv':
        csv_buffer = io.StringIO()
        if all_ec2_data:
            fieldnames = all_ec2_data[0].keys()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_ec2_data)
        content = csv_buffer.getvalue()
    else:
        content = json.dumps({'instances': all_ec2_data, 'failed_accounts': failed_accounts}, indent=2, default=str)
   
    s3_client.put_object(Bucket=config['s3_bucket'], Key=report_key, Body=content, ContentType='text/csv' if config['report_format'] == 'csv' else 'application/json')
   
    if failed_accounts:
        error_key = f"ec2-usage-reports/{timestamp}/failed_accounts.json"
        s3_client.put_object(Bucket=config['s3_bucket'], Key=error_key, Body=json.dumps(failed_accounts, indent=2), ContentType='application/json')
   
    return report_key
 
def generate_summary(all_ec2_data, failed_accounts, account_list):
    return {
        'total_accounts_processed': len(account_list),
        'total_accounts_failed': len(failed_accounts),
        'total_instances_found': len(all_ec2_data),
        'instances_by_state': {state: len([i for i in all_ec2_data if i['State'] == state]) for state in set(i['State'] for i in all_ec2_data)} if all_ec2_data else {},
        'report_generated_at': datetime.utcnow().isoformat()
    }
 