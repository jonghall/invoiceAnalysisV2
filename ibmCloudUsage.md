# IBM Cloud Usage Reporting

### Identity & Access Management Requirements
| APIKEY                                     | Description                                                     | Min Access Permissions
|--------------------------------------------|-----------------------------------------------------------------|----------------------
| IBM Cloud API Key                          | API Key used access classic invoices & IBM Cloud Usage          | IAM Billing Viewer Role


### Viewing IBM Cloud Usage between range of dates (including current month)

```bazaar
usage: ibmCloudUsage.py [-h] [--apikey apikey] [--output OUTPUT] [--load | --no-load] [--save | --no-save] [--months MONTHS] [--vpc | --no-vpc] [-s STARTDATE] [-e ENDDATE] [--cos | --no-cos | --COS | --no-COS] [--COS_APIKEY COS_APIKEY]
                        [--COS_ENDPOINT COS_ENDPOINT] [--COS_INSTANCE_CRN COS_INSTANCE_CRN] [--COS_BUCKET COS_BUCKET] [--sendgrid | no-sendgrid] [--sendGridApi SENDGRIDAPI] [--sendGridTo SENDGRIDTO] [--sendGridFrom SENDGRIDFROM]
                        [--sendGridSubject SENDGRIDSUBJECT]

Calculate IBM Cloud Usage.

options:
  -h, --help            show this help message and exit
  --apikey apikey       IBM Cloud API Key
  --output OUTPUT       Filename Excel output file. (including extension of .xlsx)
  --load, --no-load     load dataframes from pkl files for testing purposes.
  --save, --no-save     Store dataframes to pkl files for testing purposes.
  --months MONTHS       Number of months including current month to include in report.
  --vpc, --no-vpc       Include additional VPC analysis tabs (server and stroage detail).
  -s STARTDATE, --startdate STARTDATE
                        Start Year & Month in format YYYY-MM
  -e ENDDATE, --enddate ENDDATE
                        End Year & Month in format YYYY-MM
  --cos, --no-cos, --COS, --no-COS
                        Write output to COS bucket destination specified.
  --COS_APIKEY COS_APIKEY
                        COS apikey to use for Object Storage.
  --COS_ENDPOINT COS_ENDPOINT
                        COS endpoint to use for Object Storage.
  --COS_INSTANCE_CRN COS_INSTANCE_CRN
                        COS Instance CRN to use for file upload.
  --COS_BUCKET COS_BUCKET
                        COS Bucket name to use for file upload.
  --sendgrid, --no-sendgrid
                        Send file to email distribution list using SendGrid. 
  --sendGridApi SENDGRIDAPI
                        SendGrid ApiKey used to email output.
  --sendGridTo SENDGRIDTO
                        SendGrid comma deliminated list of emails to send output to.
  --sendGridFrom SENDGRIDFROM
                        Sendgrid from email to send output from.
  --sendGridSubject SENDGRIDSUBJECT
                        SendGrid email subject for output email


```
### Output Description for ibmCloudUsage.py
Note : If current month included this will be month to date.  For SLIC/CFTS invoices, this the actual usage from IBM Cloud will be consolidated onto the classic RECURRING invoice
one month later, and be invoiced via the SLIC/CFTS invoice at the end of that month.  (i.e. April Usage, appears on the June 1st RECURRING invoice, and will
appear on the end of June SLIC/CFTS invoice)  In most cases, for SLIC accounts, discounts are applied at the time the data is fed into the RECURRING
invoice and the resulting cost is shown on the invoice, therefore the discounted usage is not shown in the data.  This can be determined if the discount field empty or zero for
any services where you are receiving a discount.   If discount field is populated, the rated_cost will contain the list price cost, and the cost field will contain the resulting
cost after discounts are applied.   In some rate occasions discounts are applied in both locations and incremental.

*Excel Tab Explanation*
   - ***ServiceUsageDetail*** is a table for the range of months specified for billable metrics for each platform service.   Each row contains a unique service, resource, and metric with the rated usage and cost, and the resulting cost for discounted items.
   - ***Instance_detail*** is a table for the range of months specified for each unique service instance (ie in the case of VPC Virtual Servers each virtual server is listed).  Each row contains details on that instance included rated usage and cost.
   - ***Usage_Summary*** is a pivot table showing each month estimated cost for each service.  Note if current month included amount is month to date.
   - ***MetricPlanSummary*** is a pivot table showing each month estimated cost for each service and related usage metric.  Note if current month included amount is month to date.

*Tabs included if --vpc specified*
   - ***YYYY-MM_VPC_Server_list*** is a list of all virtual servers deployed and associated charges.   Servers are grouped by Resource Group and Role tag. (tag servers role:xyz, with xyz being role or org)
   - ***YYYY-MM_VPC_Volume_list*** is a list of all virtual server block volumes and associated charges.   Volumes are grouped by Resource Group and Role tag. (tag servers role:xyz, with xyz being role or org)
   - ***YYYY-MM_VPC_VirtualCores*** is a pivot table of Virtual Cores deployed in VPC for the current month or if range specified, the last month specified.
   - ***YYYY-MM_BareMetalCores*** is a pivot table of Bare Metal Cores and Sockets in VPC for the current month or if range specified, the last month specified.
   - ***YYYY-MM_Volume_Summary*** is a pivot table of VPC Block Volumes Deployed by volume characteristics.
