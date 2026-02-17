"""
Expanded inventory Lambda
- Assumes `CROSS_ACCOUNT_ROLE_NAME` in each account inside the target OU
- Collects resources using ResourceGroups Tagging API + a set of service-specific collectors
- Produces a normalized CSV with columns:
  Timestamp, AccountId, AccountName, Region, Service, ResourceType, ResourceId, ResourceArn, ResourceName, ResourceTags, AdditionalInfo

Notes:
- Scanning every service/region for many accounts may exceed a short Lambda timeout. Consider running by OU in batches or use Step Functions for very large environments.
"""
import boto3
import csv
import io
import json
import os
import logging
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# ---------------------- Lambda entry ---------------------------------

def lambda_handler(event, context):
    LOG.info("Inventory Lambda started at %s", datetime.utcnow().isoformat())
    config = get_configuration(event)
    valid = validate_configuration(config)
    if not valid['valid']:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Configuration validation failed', 'details': valid['errors']})}

    org_client = boto3.client('organizations')
    s3_client = boto3.client('s3')

    accounts = get_accounts_in_ou(org_client, config['target_ou_id'])
    inventory_rows = []
    failed_accounts = []

    for acct in accounts:
        try:
            rows = collect_account_inventory(acct, config)
            inventory_rows.extend(rows)
        except Exception as e:
            LOG.exception("Failed to collect inventory for account %s", acct['Id'])
            failed_accounts.append({'AccountId': acct['Id'], 'AccountName': acct['Name'], 'Error': str(e)})

    report_key = store_inventory_to_s3(s3_client, inventory_rows, failed_accounts, config)
    summary = generate_summary(inventory_rows, failed_accounts, accounts)

    return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'report_location': f"s3://{config['s3_bucket']}/{report_key}", 'summary': summary})}


# ---------------------- Configuration / validation --------------------

def get_configuration(event):
    # SERVICES_TO_COLLECT: comma-separated list or "all-resourcegroups" to use ResourceGroups Tagging API
    return {
        'target_ou_id': event.get('ou_id') or os.environ.get('TARGET_OU_ID'),
        's3_bucket': event.get('s3_bucket') or os.environ.get('S3_BUCKET_NAME'),
        'cross_account_role_name': os.environ.get('CROSS_ACCOUNT_ROLE_NAME', 'EC2CrossAccountReportingRole'),
        'external_id': os.environ.get('EXTERNAL_ID'),
        'regions_to_scan': os.environ.get('REGIONS_TO_SCAN', 'all'),
        'services_to_collect': os.environ.get('SERVICES_TO_COLLECT', 'resourcegroupstaggingapi,ec2,s3,rds,lambda,iam,elb,ecr,dynamodb,cloudformation,cloudwatch,ssm,secretsmanager,route53'),
        'include_tags': os.environ.get('INCLUDE_TAGS', 'true').lower() == 'true',
        'collect_metrics': os.environ.get('COLLECT_METRICS', 'false').lower() == 'true',
        'metrics_days': int(os.environ.get('METRICS_DAYS', '7')),
        'report_prefix': os.environ.get('REPORT_PREFIX', 'inventory-reports'),
        'report_format': os.environ.get('REPORT_FORMAT', 'csv')
    }


def validate_configuration(cfg):
    errors = []
    if not cfg['target_ou_id']:
        errors.append('TARGET_OU_ID is required')
    if not cfg['s3_bucket']:
        errors.append('S3_BUCKET_NAME is required')
    return {'valid': len(errors) == 0, 'errors': errors}


# ---------------------- Organizations / accounts ----------------------

def get_accounts_in_ou(org_client, ou_id):
    accounts = []
    paginator = org_client.get_paginator('list_accounts_for_parent')
    for page in paginator.paginate(ParentId=ou_id):
        for a in page['Accounts']:
            if a['Status'] == 'ACTIVE':
                accounts.append({'Id': a['Id'], 'Name': a['Name'], 'Email': a.get('Email')})
    LOG.info('Found %d active accounts in OU %s', len(accounts), ou_id)
    return accounts


# ---------------------- Account / region orchestration ----------------

def collect_account_inventory(account_info, config):
    sts = boto3.client('sts')
    role_arn = f"arn:aws:iam::{account_info['Id']}:role/{config['cross_account_role_name']}"
    assumed = sts.assume_role(RoleArn=role_arn, RoleSessionName=f"Inventory-{account_info['Id']}", ExternalId=config.get('external_id'))
    creds = assumed['Credentials']

    regions = get_regions_to_scan(creds, config)
    LOG.info('Collecting inventory for account %s (%s) across %d regions', account_info['Id'], account_info['Name'], len(regions))

    services = [s.strip().lower() for s in config['services_to_collect'].split(',') if s.strip()]
    rows = []

    # per-region collectors
    for region in regions:
        try:
            rows.extend(collect_region_inventory(creds, region, account_info, services, config))
        except Exception as e:
            LOG.exception('Region collection failed for %s %s in %s', account_info['Id'], region, str(e))

    # global (non-regional) collectors
    try:
        rows.extend(collect_global_inventory(creds, account_info, services, config))
    except Exception as e:
        LOG.exception('Global collection failed for %s: %s', account_info['Id'], str(e))

    return rows


def get_regions_to_scan(credentials, config):
    if config['regions_to_scan'] == 'all':
        ec2 = boto3.client('ec2', region_name='us-east-1', aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
        return [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    return [r.strip() for r in config['regions_to_scan'].split(',') if r.strip()]


# ---------------------- Region-level collection ------------------------

def collect_region_inventory(creds, region, account_info, services, config):
    rows = []

    # Resource Groups Tagging API (broad, taggable resources)
    if 'resourcegroupstaggingapi' in services or 'all-resourcegroups' in services:
        try:
            rows.extend(collect_resourcegroup_resources(creds, region, account_info, config))
        except Exception:
            LOG.exception('resourcegroupstaggingapi failed for %s %s', account_info['Id'], region)

    # EC2 (instances + volumes)
    if 'ec2' in services:
        try:
            rows.extend(collect_ec2_instances(creds, region, account_info, config))
        except Exception:
            LOG.exception('EC2 collection failed for %s %s', account_info['Id'], region)

    # Add targeted service collectors (each returns standardized rows)
    if 's3' in services:
        try:
            # S3 is global but we call per-region only for convenience; collector will normalize region
            rows.extend(collect_s3_buckets(creds, account_info, config))
        except Exception:
            LOG.exception('S3 collection failed for %s', account_info['Id'])

    if 'lambda' in services:
        try:
            rows.extend(collect_lambda_functions(creds, region, account_info, config))
        except Exception:
            LOG.exception('Lambda collection failed for %s %s', account_info['Id'], region)

    if 'rds' in services:
        try:
            rows.extend(collect_rds_instances(creds, region, account_info, config))
        except Exception:
            LOG.exception('RDS collection failed for %s %s', account_info['Id'], region)

    if 'ecr' in services:
        try:
            rows.extend(collect_ecr_repos(creds, region, account_info, config))
        except Exception:
            LOG.exception('ECR collection failed for %s %s', account_info['Id'], region)

    if 'dynamodb' in services:
        try:
            rows.extend(collect_dynamodb_tables(creds, region, account_info, config))
        except Exception:
            LOG.exception('DynamoDB collection failed for %s %s', account_info['Id'], region)

    if 'elb' in services or 'alb' in services:
        try:
            rows.extend(collect_elbv2(creds, region, account_info, config))
        except Exception:
            LOG.exception('ELBv2 collection failed for %s %s', account_info['Id'], region)

    # Other service collectors can be added here similarly

    return rows


# ---------------------- Global collectors ------------------------------

def collect_global_inventory(creds, account_info, services, config):
    rows = []
    if 'iam' in services:
        try:
            rows.extend(collect_iam_roles_and_users(creds, account_info, config))
        except Exception:
            LOG.exception('IAM collection failed for %s', account_info['Id'])

    if 'route53' in services:
        try:
            rows.extend(collect_route53_zones(creds, account_info, config))
        except Exception:
            LOG.exception('Route53 collection failed for %s', account_info['Id'])

    # Add more global collectors as needed
    return rows


# ---------------------- Standard row factory ---------------------------

def make_row(account_info, region, service, resource_type, resource_id, resource_arn, resource_name=None, tags=None, additional=None):
    return {
        'Timestamp': datetime.utcnow().isoformat(),
        'AccountId': account_info['Id'],
        'AccountName': account_info.get('Name'),
        'Region': region or 'global',
        'Service': service,
        'ResourceType': resource_type,
        'ResourceId': resource_id,
        'ResourceArn': resource_arn,
        'ResourceName': resource_name or '',
        'ResourceTags': json.dumps(tags or {}),
        'AdditionalInfo': json.dumps(additional or {})
    }


# ---------------------- ResourceGroup Tagging API ----------------------

def collect_resourcegroup_resources(creds, region, account_info, config):
    client = boto3.client('resourcegroupstaggingapi', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    paginator = client.get_paginator('get_resources')
    rows = []
    for page in paginator.paginate(PaginationConfig={'PageSize': 100}):
        for mapping in page.get('ResourceTagMappingList', []):
            arn = mapping.get('ResourceARN')
            tags = {t['Key']: t['Value'] for t in mapping.get('Tags', [])}
            service, res_type, res_id = parse_arn(arn)
            name = tags.get('Name') or ''
            rows.append(make_row(account_info, region, service or 'unknown', res_type or 'resource', res_id or arn, arn, name, tags, {}))
    return rows


def parse_arn(arn: str):
    # Return (service, resource_type, resource_id) - best-effort parsing
    try:
        # arn:partition:service:region:account:resource
        parts = arn.split(':', 5)
        service = parts[2]
        resource = parts[5]
        if '/' in resource:
            rtype, rid = resource.split('/', 1)
        elif ':' in resource:
            rtype, rid = resource.split(':', 1)
        else:
            # resource may be e.g. bucket_name
            rtype, rid = resource, resource
        return service, rtype, rid
    except Exception:
        return None, None, arn


# ---------------------- EC2 collector (instances) ---------------------

def collect_ec2_instances(creds, region, account_info, config):
    ec2 = boto3.client('ec2', region_name=region,
                       aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    cw = boto3.client('cloudwatch', region_name=region,
                      aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken']) if config.get('collect_metrics') else None
    paginator = ec2.get_paginator('describe_instances')
    rows = []
    filters = []  # keep as-is from original config (could be extended)
    for page in paginator.paginate(Filters=filters) if filters else paginator.paginate():
        for r in page.get('Reservations', []):
            for inst in r.get('Instances', []):
                instance_id = inst['InstanceId']
                tags = {t['Key']: t['Value'] for t in inst.get('Tags', [])} if config.get('include_tags') else {}
                name = tags.get('Name', '')
                arn = f"arn:aws:ec2:{region}:{account_info['Id']}:instance/{instance_id}"
                additional = {
                    'InstanceType': inst.get('InstanceType'),
                    'State': inst.get('State', {}).get('Name'),
                    'VpcId': inst.get('VpcId'),
                    'SubnetId': inst.get('SubnetId'),
                    'PrivateIp': inst.get('PrivateIpAddress'),
                    'PublicIp': inst.get('PublicIpAddress'),
                    'LaunchTime': inst.get('LaunchTime').isoformat() if inst.get('LaunchTime') else None
                }
                if cw and additional.get('State') == 'running':
                    try:
                        m = get_instance_metrics(cw, instance_id, config.get('metrics_days', 7))
                        additional.update(m)
                    except Exception:
                        pass
                rows.append(make_row(account_info, region, 'ec2', 'instance', instance_id, arn, name, tags, additional))
    return rows


# ---------------------- S3 collector ---------------------------------

def collect_s3_buckets(creds, account_info, config):
    # S3 is global; list_buckets returns all buckets for the account
    s3 = boto3.client('s3', region_name='us-east-1',
                      aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    try:
        for b in s3.list_buckets().get('Buckets', []):
            name = b['Name']
            arn = f"arn:aws:s3:::{name}"
            # bucket location
            try:
                loc = s3.get_bucket_location(Bucket=name).get('LocationConstraint') or 'us-east-1'
            except ClientError:
                loc = 'unknown'
            tags = {}
            if config.get('include_tags'):
                try:
                    resp = s3.get_bucket_tagging(Bucket=name)
                    tags = {t['Key']: t['Value'] for t in resp.get('TagSet', [])}
                except ClientError:
                    tags = {}
            rows.append(make_row(account_info, loc, 's3', 'bucket', name, arn, name, tags, {}))
    except ClientError:
        LOG.exception('Failed listing S3 buckets for %s', account_info['Id'])
    return rows


# ---------------------- Lambda collector ------------------------------

def collect_lambda_functions(creds, region, account_info, config):
    client = boto3.client('lambda', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    paginator = client.get_paginator('list_functions')
    for page in paginator.paginate():
        for fn in page.get('Functions', []):
            arn = fn['FunctionArn']
            name = fn.get('FunctionName')
            tags = {}
            if config.get('include_tags'):
                try:
                    tags = client.list_tags(Resource=arn).get('Tags', {})
                except ClientError:
                    tags = {}
            additional = {'Runtime': fn.get('Runtime'), 'LastModified': fn.get('LastModified')}
            rows.append(make_row(account_info, region, 'lambda', 'function', name, arn, name, tags, additional))
    return rows


# ---------------------- RDS collector --------------------------------

def collect_rds_instances(creds, region, account_info, config):
    client = boto3.client('rds', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    paginator = client.get_paginator('describe_db_instances')
    for page in paginator.paginate():
        for db in page.get('DBInstances', []):
            arn = db.get('DBInstanceArn')
            identifier = db.get('DBInstanceIdentifier')
            tags = {}
            if config.get('include_tags'):
                try:
                    tags = {t['Key']: t['Value'] for t in client.list_tags_for_resource(ResourceName=arn).get('TagList', [])}
                except ClientError:
                    tags = {}
            additional = {'Engine': db.get('Engine'), 'DBInstanceClass': db.get('DBInstanceClass'), 'Status': db.get('DBInstanceStatus')}
            rows.append(make_row(account_info, region, 'rds', 'db-instance', identifier, arn, identifier, tags, additional))
    return rows


# ---------------------- ECR collector --------------------------------

def collect_ecr_repos(creds, region, account_info, config):
    client = boto3.client('ecr', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    paginator = client.get_paginator('describe_repositories')
    for page in paginator.paginate():
        for repo in page.get('repositories', []):
            arn = repo.get('repositoryArn')
            name = repo.get('repositoryName')
            tags = {}
            if config.get('include_tags'):
                try:
                    tags = {t['Key']: t['Value'] for t in client.list_tags_for_resource(resourceArn=arn).get('tags', [])}
                except ClientError:
                    tags = {}
            rows.append(make_row(account_info, region, 'ecr', 'repository', name, arn, name, tags, {}))
    return rows


# ---------------------- DynamoDB collector ----------------------------

def collect_dynamodb_tables(creds, region, account_info, config):
    client = boto3.client('dynamodb', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    paginator = client.get_paginator('list_tables')
    for page in paginator.paginate():
        for name in page.get('TableNames', []):
            arn = f"arn:aws:dynamodb:{region}:{account_info['Id']}:table/{name}"
            try:
                desc = client.describe_table(TableName=name).get('Table', {})
                tags = {}
                if config.get('include_tags'):
                    try:
                        tags = {t['Key']: t['Value'] for t in client.list_tags_of_resource(ResourceArn=arn).get('Tags', [])}
                    except ClientError:
                        tags = {}
                additional = {'TableStatus': desc.get('TableStatus'), 'ItemCount': desc.get('ItemCount')}
            except ClientError:
                tags = {}
                additional = {}
            rows.append(make_row(account_info, region, 'dynamodb', 'table', name, arn, name, tags, additional))
    return rows


# ---------------------- ELBv2 collector -------------------------------

def collect_elbv2(creds, region, account_info, config):
    client = boto3.client('elbv2', region_name=region,
                          aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    paginator = client.get_paginator('describe_load_balancers')
    for page in paginator.paginate():
        for lb in page.get('LoadBalancers', []):
            arn = lb.get('LoadBalancerArn')
            name = lb.get('LoadBalancerName')
            rows.append(make_row(account_info, region, 'elbv2', lb.get('Type', 'loadbalancer'), name, arn, name, {}, {'State': lb.get('State')}))
    return rows


# ---------------------- IAM collector --------------------------------

def collect_iam_roles_and_users(creds, account_info, config):
    client = boto3.client('iam', aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    # Roles
    paginator = client.get_paginator('list_roles')
    for page in paginator.paginate():
        for r in page.get('Roles', []):
            name = r['RoleName']
            arn = r['Arn']
            tags = {}
            if config.get('include_tags'):
                try:
                    tags = {t['Key']: t['Value'] for t in client.list_role_tags(RoleName=name).get('Tags', [])}
                except ClientError:
                    tags = {}
            rows.append(make_row(account_info, 'global', 'iam', 'role', name, arn, name, tags, {'CreateDate': r.get('CreateDate').isoformat() if r.get('CreateDate') else None}))
    # Users
    paginator = client.get_paginator('list_users')
    for page in paginator.paginate():
        for u in page.get('Users', []):
            name = u['UserName']
            arn = u['Arn']
            tags = {}
            if config.get('include_tags'):
                try:
                    tags = {t['Key']: t['Value'] for t in client.list_user_tags(UserName=name).get('Tags', [])}
                except ClientError:
                    tags = {}
            rows.append(make_row(account_info, 'global', 'iam', 'user', name, arn, name, tags, {'CreateDate': u.get('CreateDate').isoformat() if u.get('CreateDate') else None}))
    return rows


# ---------------------- Route53 collector -----------------------------

def collect_route53_zones(creds, account_info, config):
    client = boto3.client('route53', aws_access_key_id=creds['AccessKeyId'], aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'])
    rows = []
    for z in client.list_hosted_zones().get('HostedZones', []):
        arn = f"arn:aws:route53:::{z['Id'].lstrip('/hostedzone/')}"
        name = z.get('Name')
        rows.append(make_row(account_info, 'global', 'route53', 'hosted-zone', z['Id'], arn, name, {}, {'PrivateZone': z.get('Config', {}).get('PrivateZone')}))
    return rows


# ---------------------- Storage / reporting ---------------------------

def store_inventory_to_s3(s3_client, rows, failed_accounts, config):
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    prefix = config.get('report_prefix', 'inventory-reports')
    key = f"{prefix}/{timestamp}/inventory.{config.get('report_format','csv')}"

    if config.get('report_format', 'csv') == 'csv':
        csv_buf = io.StringIO()
        fieldnames = ['Timestamp', 'AccountId', 'AccountName', 'Region', 'Service', 'ResourceType', 'ResourceId', 'ResourceArn', 'ResourceName', 'ResourceTags', 'AdditionalInfo']
        writer = csv.DictWriter(csv_buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            # ensure deterministic field order
            writer.writerow({k: r.get(k, '') for k in fieldnames})
        body = csv_buf.getvalue()
        content_type = 'text/csv'
    else:
        body = json.dumps({'rows': rows, 'failed_accounts': failed_accounts}, default=str, indent=2)
        content_type = 'application/json'

    s3_client.put_object(Bucket=config['s3_bucket'], Key=key, Body=body, ContentType=content_type)
    if failed_accounts:
        s3_client.put_object(Bucket=config['s3_bucket'], Key=f"{prefix}/{timestamp}/failed_accounts.json", Body=json.dumps(failed_accounts, indent=2), ContentType='application/json')
    LOG.info('Wrote report to s3://%s/%s (rows=%d)', config['s3_bucket'], key, len(rows))
    return key


# ---------------------- Summary --------------------------------------

def generate_summary(rows, failed_accounts, accounts):
    by_service = {}
    for r in rows:
        by_service[r['Service']] = by_service.get(r['Service'], 0) + 1
    return {
        'total_accounts_processed': len(accounts),
        'total_accounts_failed': len(failed_accounts),
        'total_resources_found': len(rows),
        'resources_by_service': by_service,
        'report_generated_at': datetime.utcnow().isoformat()
    }


# ---------------------- Utilities / metrics --------------------------

def get_instance_metrics(cloudwatch_client, instance_id, days):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    resp = cloudwatch_client.get_metric_statistics(Namespace='AWS/EC2', MetricName='CPUUtilization', Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}], StartTime=start_time, EndTime=end_time, Period=3600, Statistics=['Average', 'Maximum'])
    if resp.get('Datapoints'):
        avg = sum(d['Average'] for d in resp['Datapoints']) / len(resp['Datapoints'])
        mx = max(d['Maximum'] for d in resp['Datapoints'])
        return {'AvgCpuUtilization': round(avg, 2), 'MaxCpuUtilization': round(mx, 2)}
    return {'AvgCpuUtilization': None, 'MaxCpuUtilization': None}


# End of file
 