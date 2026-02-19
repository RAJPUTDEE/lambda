# AWS Lambda Inventory & Reporting Scripts

This collection contains Lambda functions for AWS resource inventory collection, EC2 reporting, and email notification capabilities. These scripts support cross-account AWS organization scanning with parallel execution for performance optimization.

---

## 📋 Project Overview

This project provides automated AWS resource discovery and reporting solutions:
- **Comprehensive inventory collection** across multiple AWS services
- **EC2-specific reporting** with detailed instance and resource information
- **Email delivery** via AWS SES for reports
- **Cross-account support** using AWS Organizations and STS role assumption
- **Parallel processing** for scalability across accounts and regions

---

## 📁 File Inventory

### Core Inventory Scripts

#### [`inventory.py`](inventory.py) ⭐ **PRIMARY INVENTORY**
- **Status**: ✅ Production-ready
- **Lines**: 865
- **Description**: Comprehensive expanded inventory Lambda function leveraging parallel execution
- **Purpose**: Collects AWS resources across all services using ResourceGroups Tagging API combined with service-specific collectors
- **Key Features**:
  - Parallel account processing (up to 5 concurrent accounts)
  - Parallel region processing (up to 10 concurrent regions per account)
  - Global resource collection (S3, IAM, Route53) to avoid duplicates
  - Regional resource collection (EC2, EBS, VPC, RDS, Lambda, etc.)
  - Comprehensive error handling with debug logging
  - Thread-safe row collection with locks
- **Output**: Normalized CSV with columns: Timestamp, AccountId, AccountName, Region, Service, ResourceType, ResourceId, ResourceArn, ResourceName, ResourceTags, AdditionalInfo
- **Supported Services**: EC2 instances, EBS volumes, VPC resources, subnets, security groups, elastic IPs, internet gateways, transit gateways, VPC peering, network ACLs, VPN, EKS, EFS, ALB, ELB, S3 buckets, RDS instances, Lambda functions, ECR repositories, DynamoDB tables, IAM roles/users, Route53 hosted zones, CloudWatch metrics, and more

#### [`aq-inventory.py`](aq-inventory.py) 🔄 **IN DEVELOPMENT**
- **Status**: ⚠️ 80% Complete (needs additional services)
- **Lines**: 616
- **Author**: AWS AmazonQ (AI-generated)
- **Description**: Alternative inventory collection script with similar architecture to inventory.py
- **Purpose**: Parallel AWS resource collection across organizations and accounts
- **TODO**: 
  - Include additional AWS services not yet implemented
  - Integration testing with full service suite
  - Cross-validation with inventory.py implementation

---

### EC2 Reporting Scripts

#### [`ec2-reporting.py`](ec2-reporting.py) ✅ **TESTED**
- **Status**: ✅ Tested and functional
- **Lines**: 180
- **Description**: EC2-specific reporting Lambda function
- **Purpose**: Collects EC2 instance data across accounts and generates S3-stored reports
- **Key Features**:
  - Cross-account EC2 inventory collection
  - Support for stopped/running instances filtering
  - Multi-region support
  - CloudWatch metrics collection (configurable days)
  - CSV or JSON report format
  - Tag collection and storage
- **Output**: S3-stored reports in CSV or JSON format
- **Environment Variables**: TARGET_OU_ID, S3_BUCKET_NAME, CROSS_ACCOUNT_ROLE_NAME, EXTERNAL_ID, REGIONS_TO_SCAN, COLLECT_METRICS, METRICS_DAYS, REPORT_FORMAT, INCLUDE_TAGS

#### [`ec2-allfields.py`](ec2-allfields.py)
- **Status**: 📝 Undocumented
- **Description**: Extended EC2 reporting variant capturing all available EC2 fields
- **Purpose**: Detailed EC2 resource inventory with comprehensive field extraction
- **Note**: Complements ec2-reporting.py with additional data fields

---

### Email Reporting Scripts

#### [`gauri-ses.py`](gauri-ses.py) 🔧 **NEEDS TESTING**
- **Status**: ⚠️ Awaiting testing
- **Lines**: 390
- **Author**: Gauri
- **Description**: EC2 inventory collection with SES email delivery
- **Purpose**: Generates EC2 reports and sends them via AWS SES (Simple Email Service)
- **Key Features**:
  - Cross-account EC2 data collection
  - MIME multipart email support
  - Base64 encoding for attachments
  - S3 report storage
  - Email delivery via SES
- **TODO**: 
  - Unit testing
  - Integration testing with SES endpoint
  - Email template validation
  - Error handling verification

#### [`reporting-ses-s3.py`](reporting-ses-s3.py) 📋 **NOT YET TESTED**
- **Status**: ❌ Testing required
- **Lines**: 294
- **Description**: Advanced reporting solution combining SES email and S3 storage
- **Purpose**: Generates EC2 reports, stores in S3, and emails results with attachment
- **Key Features**:
  - S3 report generation and storage
  - CSV content extraction for email
  - MIME multipart email construction
  - MIMEApplication for binary attachments
  - Cross-account resource collection
- **TODO**:
  - Functional testing
  - SES delivery verification
  - Email attachment validation
  - End-to-end workflow testing

#### [`gauri-gmail.py`](gauri-gmail.py) ❌ **MULTIPLE ERRORS**
- **Status**: 🚫 Contains errors (requires fixes)
- **Lines**: Empty file
- **Author**: Gauri
- **Description**: Gmail integration for inventory reports (placeholder/not yet implemented)
- **Purpose**: Intended for report delivery via Gmail API
- **TODO**:
  - Full implementation
  - Gmail API integration
  - OAuth2 authentication setup
  - Testing and validation

#### [`mintu-gmail.py`](mintu-gmail.py) ❌ **MULTIPLE ERRORS**
- **Status**: 🚫 Contains errors (requires fixes)
- **Author**: Mintu
- **Description**: Gmail-based reporting script
- **Purpose**: Alternative Gmail integration for report distribution
- **TODO**:
  - Error identification and fixes
  - Gmail API integration review
  - Authentication mechanism validation
  - Testing suite creation

---

### Configuration Files

#### [`EC2ReportingLambdaRole.json`](EC2ReportingLambdaRole.json)
- **Type**: IAM Role Policy Document
- **Purpose**: Defines permissions required for EC2 reporting Lambda function
- **Contains**: Policies for S3, EC2, Organizations, STS, and CloudWatch access
- **Usage**: Attach to Lambda execution IAM role

#### [`EC2CrossAccountReportingRole.json`](EC2CrossAccountReportingRole.json)
- **Type**: Cross-Account IAM Role Trust Policy
- **Purpose**: Enables cross-account role assumption for multi-account inventory
- **Contains**: Trust relationship configuration for cross-account access
- **Usage**: Deploy in target accounts within organization

---

## 🚀 Quick Start

### Prerequisites
- AWS Lambda execution role with appropriate permissions
- S3 bucket for report storage
- AWS Organizations setup (for multi-account deployments)
- Cross-account roles deployed in target accounts (if using cross-account mode)

### Environment Variables (Common)
```
TARGET_OU_ID          # AWS Organizations OU ID to scan
S3_BUCKET_NAME        # S3 bucket for report storage
CROSS_ACCOUNT_ROLE_NAME # IAM role name in target accounts (default: AWSInventoryCrossAccountRole)
EXTERNAL_ID           # External ID for cross-account role assumption (if configured)
REGIONS_TO_SCAN       # Comma-separated regions or 'all' (default: all)
SERVICES_TO_COLLECT   # Comma-separated services or 'all-resourcegroups'
REPORT_PREFIX         # S3 prefix for reports (default: inventory-reports)
REPORT_FORMAT         # CSV or JSON (default: csv)
INCLUDE_TAGS          # Include resource tags in inventory (true/false)
COLLECT_METRICS       # Collect CloudWatch metrics (true/false)
METRICS_DAYS          # Number of days for metric collection (default: 7)
```

### Deployment
1. Choose appropriate script based on needs:
   - Use `inventory.py` for comprehensive multi-service inventory
   - Use `ec2-reporting.py` for EC2-only reporting
   - Use `gauri-ses.py` or `reporting-ses-s3.py` for email-enabled reports

2. Deploy as Lambda function with:
   - Timeout: 15-30 minutes (for large organizations)
   - Memory: 512 MB - 3 GB (depends on organization size)
   - Execution role with attached policy from `EC2ReportingLambdaRole.json`

3. For cross-account scanning:
   - Deploy `EC2CrossAccountReportingRole.json` in each target account
   - Set CROSS_ACCOUNT_ROLE_NAME environment variable

---

## 📊 Service Coverage

### Fully Supported Services (inventory.py)
- **Compute**: EC2, ECS, EKS, Lambda, Elastic Beanstalk
- **Storage**: S3, EBS volumes, EFS
- **Database**: RDS, DynamoDB, ElastiCache
- **Networking**: VPC, Subnets, Security Groups, Route Tables, NACs, Transit Gateways, VPN
- **Container**: ECR, ECS, EKS
- **IAM**: Roles, Users, Policies
- **DNS**: Route53 Hosted Zones
- **Monitoring**: CloudWatch alarms, logs
- **Security**: Secrets Manager, Systems Manager
- **Load Balancing**: ALB, NLB, Classic ELB

---

## 🔧 Testing Status Summary

| Script | Status | Author | Notes |
|--------|--------|--------|-------|
| inventory.py | ✅ Production | Internal | Comprehensive, fully featured |
| aq-inventory.py | ⚠️ 80% Complete | AmazonQ | Needs additional services |
| ec2-reporting.py | ✅ Tested | Internal | Stable, EC2-focused |
| ec2-allfields.py | 📝 Unknown | Internal | Extended EC2 fields |
| gauri-ses.py | ⚠️ Untested | Gauri | Needs testing |
| reporting-ses-s3.py | ❌ Not Tested | Internal | Requires validation |
| gauri-gmail.py | ❌ Errors | Gauri | Empty/not implemented |
| mintu-gmail.py | ❌ Errors | Mintu | Contains errors, needs fixes |

---

## 🐛 Known Issues & TODOs

### High Priority
- [ ] **gauri-gmail.py** - Implement Gmail API integration
- [ ] **mintu-gmail.py** - Fix existing errors
- [ ] **reporting-ses-s3.py** - Complete end-to-end testing
- [ ] **gauri-ses.py** - Run full test suite

### Medium Priority
- [ ] **aq-inventory.py** - Add remaining AWS services
- [ ] Add support for more services (CloudFormation stacks, API Gateway, AppSync, etc.)
- [ ] Implement cost/billing data collection
- [ ] Add compliance checking capabilities

### Enhancement Ideas
- [ ] Add filtering by resource tags
- [ ] Implement incremental reporting (changes only)
- [ ] Add Slack integration for notifications
- [ ] Create CloudFormation stack templates
- [ ] Add interactive dashboard via QuickSight

---

## 📝 Architecture Notes

### Parallel Processing Model
- **Account Level**: 5 concurrent account threads to avoid throttling
- **Region Level**: 10 concurrent region threads per account
- **Global Resources**: Collected once per account (no duplication)
- **Regional Resources**: Collected per-region with regional filtering

### Thread Safety
- Lock-based synchronization using `threading.Lock()`
- Thread-safe counters for progress tracking
- Proper exception handling in thread executors

### Error Handling
- Comprehensive exception catching at multiple levels
- Failed accounts logged separately for follow-up
- Debug-level logging for regional failures
- Graceful degradation when optional services unavailable

---

## 📞 Support & Authors

| Component | Author | Status |
|-----------|--------|--------|
| inventory.py, ec2-reporting.py, reporting-ses-s3.py | Internal Team | ✅ Active |
| gauri-ses.py, gauri-gmail.py | Gauri | ⚠️ In Development |
| mintu-gmail.py | Mintu | 🔧 Needs Fixes |
| aq-inventory.py | AWS AmazonQ | ⚠️ 80% Complete |

---

## 🔐 Security Considerations

1. **IAM Policies**: Follow least-privilege principle
2. **External IDs**: Use external IDs for cross-account trust relationships
3. **S3 Encryption**: Enable encryption for S3 buckets storing reports
4. **Credential Management**: Never hardcode credentials; use environment variables
5. **SES Sender Verification**: Verify sender email addresses in SES
6. **Email Security**: Encrypt sensitive data in email attachments

---

## 📄 License & Usage

These scripts are provided for AWS deployment within your organization. Modify as needed for your specific requirements.

---

**Last Updated**: February 2026  
**Workspace**: d:\lambda
