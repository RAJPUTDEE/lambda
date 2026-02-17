import boto3
import json
import csv
import io
from datetime import datetime, timedelta
import os
from botocore.exceptions import ClientError, NoCredentialsError
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
 
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
       
        report_key, csv_content = store_report_to_s3(
            s3_client,
            all_ec2_data,
            failed_accounts,
            config
        )

        print("Report uploaded to S3:", report_key)

        email_message_id = send_report_via_email(
            csv_content,
            all_ec2_data,
            failed_accounts
        )

        print("Email sent. Message ID:", email_message_id)

        summary = generate_summary(
            all_ec2_data,
            failed_accounts,
            account_list
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Report stored in S3 and sent by email",
                "s3_location": f"s3://{config['s3_bucket']}/{report_key}",
                "ses_message_id": email_message_id,
                "summary": summary
            })
        }

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
    instance_data = {
        'AccountId': account_info['Id'], 'AccountName': account_info['Name'], 'Region': region,
        'InstanceId': instance['InstanceId'], 'InstanceType': instance['InstanceType'],
        'State': instance['State']['Name'], 'LaunchTime': instance.get('LaunchTime', '').isoformat() if instance.get('LaunchTime') else 'N/A',
        'Platform': instance.get('Platform', 'Linux/Unix'), 'PrivateIpAddress': instance.get('PrivateIpAddress', 'N/A'),
        'PublicIpAddress': instance.get('PublicIpAddress', 'N/A'), 'VpcId': instance.get('VpcId', 'N/A'),
        'AvailabilityZone': instance['Placement']['AvailabilityZone'], 'KeyName': instance.get('KeyName', 'N/A'),
        'SecurityGroups': json.dumps([sg['GroupName'] for sg in instance.get('SecurityGroups', [])]),
        'Monitoring': instance.get('Monitoring', {}).get('State', 'disabled')
    }
   
    if config['include_tags']:
        tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
        instance_data['Tags'] = json.dumps(tags)
   
    if config['collect_metrics'] and cloudwatch_client and instance['State']['Name'] == 'running':
        try:
            metrics = get_instance_metrics(cloudwatch_client, instance['InstanceId'], config['metrics_days'])
            instance_data.update(metrics)
        except:
            instance_data.update({'AvgCpuUtilization': 'N/A', 'MaxCpuUtilization': 'N/A'})
    else:
        instance_data.update({'AvgCpuUtilization': 'N/A', 'MaxCpuUtilization': 'N/A'})
   
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

    report_key = f"ec2-usage-reports/{timestamp}/ec2_usage_report.csv"

    # Create CSV in memory
    csv_buffer = io.StringIO()

    if all_ec2_data:

        fieldnames = all_ec2_data[0].keys()

        writer = csv.DictWriter(
            csv_buffer,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(all_ec2_data)

    csv_content = csv_buffer.getvalue()

    # Upload to S3
    s3_client.put_object(
        Bucket=config["s3_bucket"],
        Key=report_key,
        Body=csv_content,
        ContentType="text/csv"
    )

    # Save failed accounts (optional)
    if failed_accounts:

        error_key = f"ec2-usage-reports/{timestamp}/failed_accounts.json"

        s3_client.put_object(
            Bucket=config["s3_bucket"],
            Key=error_key,
            Body=json.dumps(failed_accounts, indent=2),
            ContentType="application/json"
        )

    return report_key, csv_content

def send_report_via_email(csv_content, all_ec2_data, failed_accounts):

    ses = boto3.client(
        "ses",
        region_name=os.environ.get("AWS_REGION", "us-east-1")
    )

    sender = os.environ.get("EMAIL_SENDER")
    receiver = os.environ.get("EMAIL_RECEIVER")

    if not sender or not receiver:
        raise Exception("EMAIL_SENDER / EMAIL_RECEIVER not configured")

    # Create email
    msg = MIMEMultipart()

    msg["Subject"] = "EC2 Usage Report"
    msg["From"] = sender
    msg["To"] = receiver

    body = f"""
Hello,

Please find attached the EC2 Usage Report.

Total Instances: {len(all_ec2_data)}
Failed Accounts: {len(failed_accounts)}
Generated At: {datetime.utcnow().isoformat()}

Regards,
AWS Lambda
"""

    msg.attach(MIMEText(body, "plain"))

    # Attach CSV
    attachment = MIMEApplication(
        csv_content.encode("utf-8")
    )

    attachment.add_header(
        "Content-Disposition",
        "attachment",
        filename="ec2_usage_report.csv"
    )

    msg.attach(attachment)

    # Send
    response = ses.send_raw_email(
        Source=sender,
        Destinations=[receiver],
        RawMessage={"Data": msg.as_string()}
    )

    return response["MessageId"]
 
def generate_summary(all_ec2_data, failed_accounts, account_list):
    return {
        'total_accounts_processed': len(account_list),
        'total_accounts_failed': len(failed_accounts),
        'total_instances_found': len(all_ec2_data),
        'instances_by_state': {state: len([i for i in all_ec2_data if i['State'] == state]) for state in set(i['State'] for i in all_ec2_data)} if all_ec2_data else {},
        'report_generated_at': datetime.utcnow().isoformat()
    }
 