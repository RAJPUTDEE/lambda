"""
Expanded inventory Lambda with parallel execution
- Assumes `CROSS_ACCOUNT_ROLE_NAME` in each account inside the target OU
- Collects resources using ResourceGroups Tagging API + a set of service-specific collectors
- Produces a normalized CSV with columns:
  Timestamp, AccountId, AccountName, Region, Service, ResourceType, ResourceId, ResourceArn, ResourceName, ResourceTags, AdditionalInfo

ARCHITECTURE:
- Accounts are processed in parallel (up to 5 concurrent accounts)
- Regions within each account are processed in parallel (up to 10 concurrent regions per account)
- GLOBAL resources (S3, IAM, Route53) are collected ONCE per account, NOT per-region to avoid duplicates
- REGIONAL resources (EC2, EBS, VPC, RDS, etc.) are collected per-region in parallel
- Global inventory collection happens AFTER regional scanning completes for each account

KEY FIXES:
- S3 buckets moved to collect_global_inventory() (was causing major duplication when called per-region)
- Regional APIs (EC2, RDS, Lambda, etc.) are correctly scoped to their region
- Proper error handling with debug-level logging for regional failures
- Thread-safe row collection using as_completed()
"""
import boto3
import csv
import io
import json
import os
import logging
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# Thread-safe list lock for parallel collection
ROWS_LOCK = Lock()

# ---------------------- Lambda entry ---------------------------------

def lambda_handler(event, context):
    LOG.info("Inventory Lambda started at %s", datetime.utcnow().isoformat())
    config = get_configuration(event)
    valid = validate_configuration(config)
    if not valid['valid']:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Configuration validation failed', 'details': valid['errors']})}

    org_client = make_boto_client('organizations')
    s3_client = make_boto_client('s3')

    accounts = get_accounts_in_ou(org_client, config['target_ou_id'])
    inventory_rows = []
    failed_accounts = []
    
    # Parallel account collection using ThreadPoolExecutor
    max_workers = min(len(accounts), 5)  # limit to 5 parallel account threads to avoid throttling
    LOG.info('Processing %d accounts with %d parallel workers', len(accounts), max_workers)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(collect_account_inventory, acct, config): acct for acct in accounts}
        for future in as_completed(futures):
            acct = futures[future]
            try:
                rows = future.result()
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
    # Default includes core compute, storage, networking, and control-plane services requested by the user
    return {
        'target_ou_id': event.get('ou_id') or os.environ.get('TARGET_OU_ID'),
        's3_bucket': event.get('s3_bucket') or os.environ.get('S3_BUCKET_NAME'),
        # Default cross-account role name expected in target accounts
        'cross_account_role_name': event.get('cross_account_role_name') or os.environ.get('CROSS_ACCOUNT_ROLE_NAME', 'AWSInventoryCrossAccountRole'),
        # ExternalId used by the cross-account trust (default to the value used in your cross-account role)
        'external_id': event.get('external_id') or os.environ.get('EXTERNAL_ID', 'AWSInventoryAccess'),
        'regions_to_scan': os.environ.get('REGIONS_TO_SCAN', 'all'),
        'services_to_collect': os.environ.get('SERVICES_TO_COLLECT', 'resourcegroupstaggingapi,ec2,ebs,vpc,subnet,securitygroup,elasticip,internet-gateway,transit-gateway,transit-gateway-attachment,vpc-peering,network-acl,vpn,eks,efs,alb,elb,s3,rds,lambda,iam,ecr,dynamodb,cloudformation,cloudwatch,ssm,secretsmanager,route53'),
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
    sts = make_boto_client('sts')
    role_arn = f"arn:aws:iam::{account_info['Id']}:role/{config['cross_account_role_name']}"
    assume_kwargs = {'RoleArn': role_arn, 'RoleSessionName': f"Inventory-{account_info['Id']}"}
    ext_id = config.get('external_id')
    if ext_id:
        assume_kwargs['ExternalId'] = ext_id
        LOG.info('Assuming role %s for account %s with ExternalId configured', role_arn, account_info['Id'])
    else:
        LOG.info('Assuming role %s for account %s without ExternalId', role_arn, account_info['Id'])
    assumed = sts.assume_role(**assume_kwargs)
    creds = assumed['Credentials']

    regions = get_regions_to_scan(creds, config)
    LOG.info('Collecting inventory for account %s (%s) across %d regions', account_info['Id'], account_info['Name'], len(regions))

    services = [s.strip().lower() for s in config['services_to_collect'].split(',') if s.strip()]
    rows = []

    # Parallel region collection using ThreadPoolExecutor
    max_region_workers = min(len(regions), 10)  # limit to 10 parallel region threads
    with ThreadPoolExecutor(max_workers=max_region_workers) as executor:
        futures = {executor.submit(collect_region_inventory, creds, region, account_info, services, config): region for region in regions}
        for future in as_completed(futures):
            region = futures[future]
            try:
                region_rows = future.result()
                rows.extend(region_rows)
            except Exception as e:
                LOG.exception('Region collection failed for %s %s: %s', account_info['Id'], region, str(e))

    # global (non-regional) collectors
    try:
        rows.extend(collect_global_inventory(creds, account_info, services, config))
    except Exception as e:
        LOG.exception('Global collection failed for %s: %s', account_info['Id'], str(e))

    return rows


def get_regions_to_scan(credentials, config):
    if config['regions_to_scan'] == 'all':
        ec2 = make_boto_client('ec2', region='us-east-1', creds=credentials)
        return [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    return [r.strip() for r in config['regions_to_scan'].split(',') if r.strip()]


# ---------------------- Region-level collection ------------------------

def collect_region_inventory(creds, region, account_info, services, config):
    rows = []

    # Resource Groups Tagging API (broad, taggable resources)
    if 'resourcegroupstaggingapi' in services or 'all-resourcegroups' in services:
        try:
            rows.extend(collect_resourcegroup_resources(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('resourcegroupstaggingapi failed for %s %s: %s', account_info['Id'], region, str(e))

    # EC2 (instances + volumes)
    if 'ec2' in services:
        try:
            rows.extend(collect_ec2_instances(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('EC2 collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'lambda' in services:
        try:
            rows.extend(collect_lambda_functions(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Lambda collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'rds' in services:
        try:
            rows.extend(collect_rds_instances(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('RDS collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'ecr' in services:
        try:
            rows.extend(collect_ecr_repos(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('ECR collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'dynamodb' in services:
        try:
            rows.extend(collect_dynamodb_tables(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('DynamoDB collection failed for %s %s: %s', account_info['Id'], region, str(e))

    # EC2-related and networking collectors
    if 'ebs' in services:
        try:
            rows.extend(collect_ebs_volumes(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('EBS collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'vpc' in services:
        try:
            rows.extend(collect_vpcs(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('VPC collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'subnet' in services or 'subnets' in services:
        try:
            rows.extend(collect_subnets(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Subnets collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'securitygroup' in services or 'security-groups' in services:
        try:
            rows.extend(collect_security_groups(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('SecurityGroups collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'elasticip' in services or 'eip' in services:
        try:
            rows.extend(collect_elastic_ips(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Elastic IPs collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'internet-gateway' in services or 'igw' in services:
        try:
            rows.extend(collect_internet_gateways(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Internet Gateways collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'transit-gateway' in services:
        try:
            rows.extend(collect_transit_gateways(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Transit Gateways collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'transit-gateway-attachment' in services:
        try:
            rows.extend(collect_transit_gateway_attachments(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('TGW Attachments collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'vpc-peering' in services or 'peering' in services:
        try:
            rows.extend(collect_vpc_peering_connections(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('VPC Peering collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'network-acl' in services or 'nacl' in services:
        try:
            rows.extend(collect_network_acls(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Network ACLs collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'vpn' in services:
        try:
            rows.extend(collect_vpn_connections(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('VPN connections collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'eks' in services:
        try:
            rows.extend(collect_eks_clusters(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('EKS collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'efs' in services:
        try:
            rows.extend(collect_efs_file_systems(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('EFS collection failed for %s %s: %s', account_info['Id'], region, str(e))

    # Load balancers: ALBs (elbv2) and classic ELBs
    if 'alb' in services or 'elbv2' in services:
        try:
            rows.extend(collect_elbv2(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('ELBv2 collection failed for %s %s: %s', account_info['Id'], region, str(e))

    if 'elb' in services:
        try:
            rows.extend(collect_classic_elb(creds, region, account_info, config))
        except Exception as e:
            LOG.debug('Classic ELB collection failed for %s %s: %s', account_info['Id'], region, str(e))

    # Other service collectors can be added here similarly

    return rows


# ---------------------- Global collectors ------------------------------

def collect_global_inventory(creds, account_info, services, config):
    rows = []
    
    # S3 is global - collect once per account, not per region
    if 's3' in services:
        try:
            rows.extend(collect_s3_buckets(creds, account_info, config))
        except Exception as e:
            LOG.exception('S3 collection failed for %s: %s', account_info['Id'], str(e))
    
    if 'iam' in services:
        try:
            rows.extend(collect_iam_roles_and_users(creds, account_info, config))
        except Exception as e:
            LOG.exception('IAM collection failed for %s: %s', account_info['Id'], str(e))

    if 'route53' in services:
        try:
            rows.extend(collect_route53_zones(creds, account_info, config))
        except Exception as e:
            LOG.exception('Route53 collection failed for %s: %s', account_info['Id'], str(e))

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


def make_boto_client(service, region=None, creds=None):
    """Create a boto3 client with conservative retry settings.
    If creds is provided, use cross-account temporary credentials.
    """
    cfg = Config(retries={'max_attempts': 8, 'mode': 'standard'})
    if creds:
        return boto3.client(service, region_name=region, aws_access_key_id=creds['AccessKeyId'],
                            aws_secret_access_key=creds['SecretAccessKey'], aws_session_token=creds['SessionToken'], config=cfg)
    return boto3.client(service, region_name=region, config=cfg)


# ---------------------- ResourceGroup Tagging API ----------------------

def collect_resourcegroup_resources(creds, region, account_info, config):
    client = make_boto_client('resourcegroupstaggingapi', region=region, creds=creds)
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
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    cw = make_boto_client('cloudwatch', region=region, creds=creds) if config.get('collect_metrics') else None
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


# ---------------------- Additional network & compute collectors ------

def collect_ebs_volumes(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    paginator = ec2.get_paginator('describe_volumes')
    for page in paginator.paginate():
        for vol in page.get('Volumes', []):
            vid = vol.get('VolumeId')
            arn = f"arn:aws:ec2:{region}:{account_info['Id']}:volume/{vid}"
            tags = {t['Key']: t['Value'] for t in vol.get('Tags', [])} if config.get('include_tags') else {}
            additional = {'SizeGiB': vol.get('Size'), 'State': vol.get('State'), 'VolumeType': vol.get('VolumeType'), 'Encrypted': vol.get('Encrypted'), 'SnapshotId': vol.get('SnapshotId'), 'AvailabilityZone': vol.get('AvailabilityZone'), 'Iops': vol.get('Iops'), 'Attachments': vol.get('Attachments')}
            rows.append(make_row(account_info, region, 'ebs', 'volume', vid, arn, vid, tags, additional))
    return rows


def collect_vpcs(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for vpc in ec2.describe_vpcs().get('Vpcs', []):
        vid = vpc.get('VpcId')
        arn = f"arn:aws:ec2:{region}:{account_info['Id']}:vpc/{vid}"
        tags = {t['Key']: t['Value'] for t in vpc.get('Tags', [])} if config.get('include_tags') else {}
        additional = {'CidrBlock': vpc.get('CidrBlock'), 'IsDefault': vpc.get('IsDefault'), 'DhcpOptionsId': vpc.get('DhcpOptionsId'), 'InstanceTenancy': vpc.get('InstanceTenancy')}
        rows.append(make_row(account_info, region, 'vpc', 'vpc', vid, arn, vid, tags, additional))
    return rows


def collect_subnets(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for s in ec2.describe_subnets().get('Subnets', []):
        sid = s.get('SubnetId')
        arn = f"arn:aws:ec2:{region}:{account_info['Id']}:subnet/{sid}"
        tags = {t['Key']: t['Value'] for t in s.get('Tags', [])} if config.get('include_tags') else {}
        additional = {'VpcId': s.get('VpcId'), 'CidrBlock': s.get('CidrBlock'), 'AvailabilityZone': s.get('AvailabilityZone'), 'AvailableIpAddressCount': s.get('AvailableIpAddressCount'), 'MapPublicIpOnLaunch': s.get('MapPublicIpOnLaunch')}
        rows.append(make_row(account_info, region, 'subnet', 'subnet', sid, arn, sid, tags, additional))
    return rows


def collect_security_groups(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for sg in ec2.describe_security_groups().get('SecurityGroups', []):
        gid = sg.get('GroupId')
        arn = f"arn:aws:ec2:{region}:{account_info['Id']}:security-group/{gid}"
        tags = {t['Key']: t['Value'] for t in sg.get('Tags', [])} if config.get('include_tags') else {}
        additional = {'GroupName': sg.get('GroupName'), 'Description': sg.get('Description'), 'VpcId': sg.get('VpcId'), 'IpPermissions': sg.get('IpPermissions'), 'IpPermissionsEgress': sg.get('IpPermissionsEgress')}
        rows.append(make_row(account_info, region, 'securitygroup', 'security-group', gid, arn, sg.get('GroupName'), tags, additional))
    return rows


def collect_elastic_ips(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for addr in ec2.describe_addresses().get('Addresses', []):
        pub = addr.get('PublicIp')
        alloc = addr.get('AllocationId') or ''
        arn = f"arn:aws:ec2:{region}:{account_info['Id']}:elastic-ip/{alloc or pub}"
        tags = {t['Key']: t['Value'] for t in addr.get('Tags', [])} if config.get('include_tags') else {}
        additional = {'AllocationId': alloc, 'AssociationId': addr.get('AssociationId'), 'InstanceId': addr.get('InstanceId'), 'NetworkInterfaceId': addr.get('NetworkInterfaceId'), 'Domain': addr.get('Domain')}
        rows.append(make_row(account_info, region, 'elasticip', 'elastic-ip', alloc or pub, arn, pub, tags, additional))
    return rows


def collect_internet_gateways(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for igw in ec2.describe_internet_gateways().get('InternetGateways', []):
        igw_id = igw.get('InternetGatewayId')
        arn = f"arn:aws:ec2:{region}:{account_info['Id']}:internet-gateway/{igw_id}"
        tags = {t['Key']: t['Value'] for t in igw.get('Tags', [])} if config.get('include_tags') else {}
        attachments = [a.get('VpcId') for a in igw.get('Attachments', [])]
        rows.append(make_row(account_info, region, 'internet-gateway', 'internet-gateway', igw_id, arn, igw_id, tags, {'Attachments': attachments}))
    return rows


def collect_transit_gateways(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for tgw in ec2.describe_transit_gateways().get('TransitGateways', []):
        tid = tgw.get('TransitGatewayId')
        arn = tgw.get('TransitGatewayArn') or f"arn:aws:ec2:{region}:{account_info['Id']}:transit-gateway/{tid}"
        rows.append(make_row(account_info, region, 'transit-gateway', 'transit-gateway', tid, arn, tid, {}, {'State': tgw.get('State'), 'OwnerId': tgw.get('OwnerId')}))
    return rows


def collect_transit_gateway_attachments(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for att in ec2.describe_transit_gateway_attachments().get('TransitGatewayAttachments', []):
        aid = att.get('TransitGatewayAttachmentId')
        rows.append(make_row(account_info, region, 'transit-gateway-attachment', 'attachment', aid, att.get('TransitGatewayAttachmentArn'), aid, {}, att))
    return rows


def collect_vpc_peering_connections(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for p in ec2.describe_vpc_peering_connections().get('VpcPeeringConnections', []):
        pid = p.get('VpcPeeringConnectionId')
        rows.append(make_row(account_info, region, 'vpc-peering', 'peering-connection', pid, pid, pid, {}, p))
    return rows


def collect_network_acls(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for acl in ec2.describe_network_acls().get('NetworkAcls', []):
        nid = acl.get('NetworkAclId')
        rows.append(make_row(account_info, region, 'network-acl', 'network-acl', nid, nid, nid, {}, {'Entries': acl.get('Entries'), 'Associations': acl.get('Associations')}))
    return rows


def collect_vpn_connections(creds, region, account_info, config):
    ec2 = make_boto_client('ec2', region=region, creds=creds)
    rows = []
    for v in ec2.describe_vpn_connections().get('VpnConnections', []):
        vid = v.get('VpnConnectionId')
        rows.append(make_row(account_info, region, 'vpn', 'vpn-connection', vid, vid, vid, {}, {'State': v.get('State'), 'Type': v.get('Type')}))
    return rows


def collect_eks_clusters(creds, region, account_info, config):
    eks = make_boto_client('eks', region=region, creds=creds)
    rows = []
    for name in eks.list_clusters().get('clusters', []):
        try:
            cluster = eks.describe_cluster(name=name).get('cluster', {})
            arn = cluster.get('arn')
            tags = cluster.get('tags', {}) if config.get('include_tags') else {}
            additional = {'Status': cluster.get('status'), 'Version': cluster.get('version'), 'Endpoint': cluster.get('endpoint'), 'PlatformVersion': cluster.get('platformVersion'), 'VpcConfig': cluster.get('resourcesVpcConfig')}
            rows.append(make_row(account_info, region, 'eks', 'cluster', name, arn or name, name, tags, additional))
        except Exception:
            LOG.exception('Failed to describe EKS cluster %s in %s', name, region)
    return rows


def collect_efs_file_systems(creds, region, account_info, config):
    efs = make_boto_client('efs', region=region, creds=creds)
    rows = []
    for fs in efs.describe_file_systems().get('FileSystems', []):
        fid = fs.get('FileSystemId')
        arn = fs.get('FileSystemArn') or f"arn:aws:elasticfilesystem:{region}:{account_info['Id']}:file-system/{fid}"
        tags = {}
        if config.get('include_tags'):
            try:
                tags = {t['Key']: t['Value'] for t in efs.list_tags_for_resource(FileSystemId=fid).get('Tags', [])}
            except Exception:
                tags = {}
        additional = {'LifeCycleState': fs.get('LifeCycleState'), 'NumberOfMountTargets': fs.get('NumberOfMountTargets'), 'Encrypted': fs.get('Encrypted'), 'PerformanceMode': fs.get('PerformanceMode'), 'ThroughputMode': fs.get('ThroughputMode')}
        rows.append(make_row(account_info, region, 'efs', 'file-system', fid, arn, fid, tags, additional))
    return rows


def collect_classic_elb(creds, region, account_info, config):
    client = make_boto_client('elb', region=region, creds=creds)
    rows = []
    paginator = client.get_paginator('describe_load_balancers')
    for page in paginator.paginate():
        for lb in page.get('LoadBalancerDescriptions', []):
            name = lb.get('LoadBalancerName')
            arn_placeholder = f"arn:aws:elasticloadbalancing:{region}:{account_info['Id']}:loadbalancer/{name}"
            additional = {'DNSName': lb.get('DNSName'), 'Scheme': lb.get('Scheme'), 'VPCId': lb.get('VPCId'), 'Subnets': lb.get('Subnets'), 'SecurityGroups': lb.get('SecurityGroups'), 'Instances': lb.get('Instances')}
            rows.append(make_row(account_info, region, 'elb', 'classic-load-balancer', name, arn_placeholder, name, {}, additional))
    return rows


# ---------------------- S3 collector ---------------------------------

def collect_s3_buckets(creds, account_info, config):
    # S3 is global; list_buckets returns all buckets for the account
    s3 = make_boto_client('s3', region='us-east-1', creds=creds)
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
    client = make_boto_client('lambda', region=region, creds=creds)
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
    client = make_boto_client('rds', region=region, creds=creds)
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
    client = make_boto_client('ecr', region=region, creds=creds)
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
    client = make_boto_client('dynamodb', region=region, creds=creds)
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
    client = make_boto_client('elbv2', region=region, creds=creds)
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
    client = make_boto_client('iam', creds=creds)
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
    client = make_boto_client('route53', creds=creds)
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
    folder = f"{prefix}/{timestamp}/"

    # group rows by service
    groups = {}
    for r in rows:
        svc = r.get('Service', 'unknown')
        groups.setdefault(svc, []).append(r)

    written = []
    if config.get('report_format', 'csv') == 'csv':
        for svc, items in groups.items():
            base_cols = ['Timestamp','AccountId','AccountName','Region','Service','ResourceType','ResourceId','ResourceArn','ResourceName','ResourceTags']
            add_keys = set()
            for it in items:
                try:
                    ai = json.loads(it.get('AdditionalInfo') or '{}')
                except Exception:
                    ai = {}
                add_keys.update(ai.keys())
            add_keys = sorted(add_keys)
            fieldnames = base_cols + add_keys

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            for it in items:
                row = {k: it.get(k, '') for k in base_cols}
                try:
                    ai = json.loads(it.get('AdditionalInfo') or '{}')
                except Exception:
                    ai = {}
                for k in add_keys:
                    v = ai.get(k, '')
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v)
                    row[k] = v
                writer.writerow(row)

            key = f"{folder}{svc}.csv"
            s3_client.put_object(Bucket=config['s3_bucket'], Key=key, Body=buf.getvalue(), ContentType='text/csv')
            written.append(key)
    else:
        key = f"{folder}inventory.json"
        s3_client.put_object(Bucket=config['s3_bucket'], Key=key, Body=json.dumps({'rows': rows, 'failed_accounts': failed_accounts}, default=str, indent=2), ContentType='application/json')
        written.append(key)

    if failed_accounts:
        s3_client.put_object(Bucket=config['s3_bucket'], Key=f"{folder}failed_accounts.json", Body=json.dumps(failed_accounts, indent=2), ContentType='application/json')

    LOG.info('Wrote %d files to s3://%s/%s', len(written), config['s3_bucket'], folder)
    return folder


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