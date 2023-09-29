# invoiceAnalysis.py

*invoiceAnalysis.py* collects IBM Cloud Classic Infrastructure NEW, RECURRING, ONE-TIME-CHARGES and CREDIT invoices between invoice months
specified, then consolidates the data into an Excel worksheet for billing analysis and reconciliation.  For accounts with SLIC billing all charges
are aligned to the IBM Invoice Online invoice cycle which consolidated IBM Classic Infrastructure invoices from the 20th of the previous month to 19th
of the next month.   In addition to consolidation of the detailed data and formatting consistent with IBM Online Invoices, additional pivot tables are
created to aid in understanding IBM Cloud usage charges and month to month comparisons.

## Table of Contents
1. [Identity and Access Management Requirements](#identity-&-access-management-requirements)
2. [Output Description](#output-description)
3. [Script Execution](#script-execution-instructions)
4. [Code Engine: Configuring Invoice Analysis Report to automatically produce output each month](#running-invoice-analysis-report-as-a-code-engine-job)


## Identity & Access Management Requirements
| APIKEY                                        | Description                                                                | Min Access Permissions                                                        
|-----------------------------------------------|----------------------------------------------------------------------------|-------------------------------------------------------------------------------
| IBM Cloud API Key                             | API Key used access classic invoices.                                      | IAM Billing Viewer Role; if --storage specified, required classic infrastructure viewer permission for net storage.                                                     
| COS API Key                                   | (optional) API Key used to write output to specified bucket (if specified) | (optional) COS Bucket Write access to Bucket at specified Object Storage CRN. 
| Internal employee ims_username & ims_password | (optional) Credentials for internal IBM IMS access.                        | IMS Access to Account                                                 |


## Output Description
The output generated provides several tables for facilitating reconciliation of billing that correspond to the monthly invoice cycle for SLIC accounts.   

### Detail Tabs
| Tab Name      | Default | flag to change default| Description of Tab 
|---------------|---------|----------------------|-------------------
| Detail | True | --no-detail | Detailed list of every invoice line item (including chidlren line items) from all invoices types between months specified.

### Monthly TopSheet Tabs
One tab is created for each month in range specified and used for reconciliation against IBM Invoices Online invoices.

| Tab Name         | Default | flag to change default| Description of Tab 
|------------------|--------|----------------------|-------------------
| TopSheet_YYYY-MM | True | --no-reconcilliation | Table matching the IaaS, PaaS and Credits from the matching invoices in IBM Invoices Online.  Each row provides the corresponding detail for the charge, such as the portal invoice number, invoice type, invoice date, service dates, product code, product description, and the amount.  This view is used to reconcile against the invoice. 


### Summary Tabs
Tabs are created to summarize usage data based on SLIC invoice month.   If a range of months is specified, months are displayed as columns in each tab and can be used to compare month to month changes

| Tab Name                | Default | flag to change default | Description of Tab 
|-------------------------|---------|------------------------|-------------------
| CategoryGroupSummary    | True    | --no-summary          | A pivot table of all charges shown by Invoice Type and Category Groups by month. 
| CategoryDetail          | True    | --no-summary          | A pivot table of all charges by Invoice Type, Category Group, Category and specific service Detail by month.
| Classic_COS_Detail      | False   | --cosdetail            | is a table of detailed usage from Classic Cloud Object Storage.  Detail is provided for awareness, but will not appear on invoice.
| StoragePivot            | False   | --storage              | A Table of all Block and File Storage allocations by location with custom notes (if used)

### BSS Tabs
These tabs are created with the --bss flag, and require an IBM Cloud API key with access to billing and usage.  These services are non IBM Cloud Classic Infrastructure services (ie. VPC Virtual Servers) and appear on the 
RECURRING invoice each month as a line item, but are for usage two months prior.   If --bss flag is specified the correct corresponding months will be included that match the specified IBM Invoices Online month.

| Tab Name                 | Default | flag to change default | Description of Tab 
|--------------------------|---------|------------------------|-------------------
| Cloud_Usage              | False   | --bss                  | A table for all BSS service usage for the corresponding usage month.
| YYYY-MM_VPC_Server_list  | False   | --bss                  | A table of all VPC virtual servers deployed and associated charges.   Servers are grouped by Resource Group and Role tag. (tag servers role:xyz, with xyz being role or org)
| YYYY-MM_VPC_Volume_list  | False   | --bss                  | A table of all virtual server block volumes and associated charges.   Volumes are grouped by Resource Group and Role tag. (tag servers role:xyz, with xyz being role or org)
| YYYY-MM_VPC_VirtualCores | False   | --bss                  | A table of Virtual Cores deployed in VPC for the current month or if range specified, the last month specified.
| YYYY-MM_BareMetalCores   | False   | --bss                  | A table of Bare Metal Cores and Sockets in VPC for the current month or if range specified, the last month specified.
| YYYY-MM_Volume_Summary   | False   | --bss                  | A table of VPC Block Volumes Deployed by volume characteristics.

***example:*** to provide the 3 latest months of detail
   ```bazaar
   $ export IC_API_KEY=<ibm cloud apikey>
   $ python invoiceAnalysis.py -m 3 
   ```

## Script Execution Instructions

```bazaar
python invoiceAnalysis.py --help
usage: invoiceAnalysis.py [-h] [-k IC_API_KEY] [-u username] [-p password] [-a account] [-s STARTDATE] [-e ENDDATE] [--debug | --no-debug] [--load | --no-load] [--save | --no-save] [--months MONTHS] [--COS_APIKEY COS_APIKEY] [--COS_ENDPOINT COS_ENDPOINT] [--COS_INSTANCE_CRN COS_INSTANCE_CRN]
                          [--COS_BUCKET COS_BUCKET] [--sendGridApi SENDGRIDAPI] [--sendGridTo SENDGRIDTO] [--sendGridFrom SENDGRIDFROM] [--sendGridSubject SENDGRIDSUBJECT] [--output OUTPUT] [--SL_PRIVATE | --no-SL_PRIVATE] [--oldFormat | --no-oldFormat] [--storage | --no-storage]
                          [--detail | --no-detail] [--summary | --no-summary] [--reconciliation | --no-reconciliation] [--serverdetail | --no-serverdetail] [--cosdetail | --no-cosdetail] [--bss | --no-bss]
```

### Command Line Parameters
| Parameter                   | Environment Variable | Default               | Description                   
|-----------------------------|----------------------|-----------------------|-------------------------------
| --IC_API_KEY, -k            | IC_API_KEY           | None                  | IBM Cloud API Key to be used to retrieve invoices and usage. 
| --username                  | ims_username         | None                  | Required only if using internal authorization (used instead of IC_API_KEY) 
| --password                  | ims_password         | None                  | Required only if using internal authorization (used instead of IC_API_KEY) 
| --account                   | ims_account          | None                  | Required only if using internal authorization to specify IMS account to pull. 
| --STARTDATE, -s             | startdate            | None                  | Start Month in YYYY-MM format 
| --ENDDATE, -e               | enddate              | None                  | End Month in YYYY-MM format   
| --months                    | months               | 1                     | Number of months including last full month to include in report. (use instead of -s/-e) 
| --COS_APIKEY                | COS_APIKEY           | None                  | COS API to be used to write output file to object storage, if not specified file written locally. 
| --COS_BUCKET                | COS_BUCKET           | None                  | COS Bucket to be used to write output file to. 
| --COS_ENDPOINT              | COS_ENDPOINT         | None                  | COS Endpoint (with https://) to be used to write output file to. 
| --COS_INSTANCE_CRN          | COS_INSTANCE_CRN     | None                  | COS Instance CRN to be used to write output file to. 
| --sendGridApi               | sendGridApi          | None                  | SendGrid API key to use to send Email. 
| --sendGridTo                | sendGridTo           | None                  | SendGrid comma delimited list of email addresses to send output report to. 
| --sendGridFrom              | sendGridFrom         | None                  | SendGrid from email addresss to send output report from. 
| --sendGridSubject           | sendGridSubject      | None                  | SendGrid email subject.       
| --output                    | output               | invoice-analysis.xlsx | Output file name used.        
| --SL_PRIVATE                |                      | --no_SL_PRIVATE       | Whether to use Public or Private Endpoint. 
| [--oldFormat](oldFormat.md) |                      | --no_oldFormat        | Specify old Format of output.
| --storage                   |                      | --no_storage          | Whether to write additional level of classic Block & File storage analysis to worksheet (default: False) 
| --no-summary                |                      | --summary             | Whether to write summary detail tabs to worksheet. (default: True)
| --no-detail                 |                      | --detail              | Whether to Write detail tabs to worksheet. (default: True)
| --no-reconciliation         |                      | --reconciliation      | Whether to write invoice reconciliation tabs to worksheet. (default: True)
| --no-serverdetail           |                      | --serverdetail        | Whether to write server detail tabs to worksheet (default: True)
| --cosdetail                 |                      | --no-cosdetail        | Whether to write Classic OBject Storage tab to worksheet (default: False)
| --bss                       |                      | --no-bss              | Include IBM Cloud BSS Metered Service detail tabs

### Examples

To analyze invoices between two months.
```bazaar
$ export IC_API_KEY=<ibm cloud apikey>
$ python invoiceAnalysis.py -s 2021-01 -e 2021-06
```
To analyze last 3 invoices.
```bazaar
$ export IC_API_KEY=<ibm cloud apikey>
$ python inboiceAnalysis.py -m 3
```

## Running Invoice Analysis Report as a Code Engine Job
Requirements
* Creation of an Object Storage Bucket to store the script output in at execution time. 
* Creation of an IBM Cloud Object Storage Service API Key with read/write access to bucket above
* Creation of an IBM Cloud API Key with Billing Service (View access))

### Setting up IBM Code Engine to run report from IBM Cloud Portal
1. Open IBM Cloud Code Engine Console from IBM Cloud Portal (left Navigation)
2. Create project, build job and job.
   - Select Start creating from Start from source code.  
   - Select Job  
   - Enter a name for the job such as invoiceanalysis. Use a name for your job that is unique within the project.  
   - Select a project from the list of available projects of if this is the first one, create a new one. Note that you must have a selected project to deploy an app.  
   - Enter the URL for this GitHub repository and click specify build details. Make adjustments if needed to URL and Branch name. Click Next.  
   - Select Dockerfile for Strategy, Dockerfile for Dockerfile, 10m for Timeout, and Medium for Build resources. Click Next.  
   - Select a container registry location, such as IBM Registry, Dallas.  
   - Select Automatic for Registry access.  
   - Select an existing namespace or enter a name for a new one, for example, newnamespace. 
   - Enter a name for your image and optionally a tag.  
   - Click Done.  
   - Click Create.  
2. Create ***configmaps*** and ***secrets***.  
    - From project list, choose newly created project.  
    - Select secrets and configmaps  
    - Click create, choose config map, and give it a name. Add the following key value pairs    
      - ***COS_BUCKET*** = Bucket within COS instance to write report file to.  
      - ***COS_ENDPOINT*** = Public COS Endpoint (including https://) for bucket to write report file to  
      - ***COS_INSTANCE_CRN*** = COS Service Instance CRN in which bucket is located.<br>
	- Select secrets and configmaps (again)
    - Click create, choose secrets, and give it a name. Add the following key value pairs
      - ***IC_API_KEY*** = an IBM Cloud API Key with Billing access to IBM Cloud Account  
      - ***COS_APIKEY*** = your COS Api Key with writter access to appropriate bucket  
3. Choose the job previously created.  
   - Click on the Environment variables tab.   
   - Click add, choose reference to full configmap, and choose configmap created in previous step and click add.  
   - Click add, choose reference to full secret, and choose secrets created in previous step and click add.  
   - Click add, choose literal value (click add after each, and repeat to set required environment variables.)
     - ***months*** = number of months to include if more than 1.<br>
     - ***output*** = report filename (including extension of XLSX to be written to COS bucket)<br>  
4. Specify Any command line parameters using Command Overrides.<br>
   - Click Command Overrides (see tables above) <br>
   - Under Arguments section specify command line arguments with one per line.
    ```azure
    --no-detail
    --no-reconcilliation
    ```
5. To configure the report to run at a specified date and time configure an Event Subscription.
   - From Project, Choose Event Subscription
   - Click Create
   - Choose Event type of Periodic timer
   - Name subscription; click Next
   - Select cron pattern or type your own.  
   - Recommend monthly on the 20th, as this is the SLIC/CFTS cutoff.  The following pattern will run the job at 07 UTC (2am CDT) on the 20th of every month. 
    ```
    00 07  20 * *
    ```
   - Click Next
   - Leave Custom event data blank, click Next.
   - Choose Event Consumer.  Choose Component Type of Job, Choose The Job Name for the job you created in Step 1.   Click Next.
   - Review configuration Summary; click create.
6. To Run report "On Demand" click ***Submit job***
7. Logging for job can be found from job screen, by clicking Actions, Logging