# Other IBM Cloud Included Scripts

## Table of Contents
1. Identity and Access Management Requirements
3. ibmCloudUsage.py
4. classicConfigAnalysis.py
5. classicConfigReport.py

### Identity & Access Management Requirements
| APIKEY                                     | Description                                                     | Min Access Permissions
|--------------------------------------------|-----------------------------------------------------------------|----------------------
| IBM Cloud API Key                          | API Key used access classic invoices & IBM Cloud Usage          | IAM Billing Viewer Role


### Estimating IBM Cloud Usage Month To Date

```bazaar
usage: ibmCloudUsage.py [-h] [--apikey apikey] [--output OUTPUT] [--load | --no-load] [--save | --no-save] [--start START] [--end END]

Calculate IBM Cloud Usage.

options:
  -h, --help         show this help message and exit
  --apikey apikey    IBM Cloud API Key
  --output OUTPUT    Filename Excel output file. (including extension of .xlsx)
  --load, --no-load  load dataframes from pkl files.
  --save, --no-save  Store dataframes to pkl files.
  --start START      Start Month YYYY-MM.
  --end END          End Month YYYY-MM.

```
### Output Description for estimateCloudUsage.py
Note : If current month included this will be month to date.  For SLIC/CFTS invoices, this actual usage from IBM Cloud will be consolidated onto the classic RECURRING invoice
one month later, and be invoiced on the SLIC/CFTS invoice at the end of that month.  (i.e. April Usage, appears on the June 1st RECURRING invoice, and will
appear on the end of June SLIC/CFTS invoice)  Additionally in most cases for SLIC accounts, discounts are applied at the time the data is fed to the RECURRING
invoice.  Other words the USAGE charges are generally list price, but eppear on the Portal RECURRING invoice at their discounted rate.
*Excel Tab Explanation*
   - ***ServiceUsageDetail*** is a table for the range of months specified for billable metrics for each platform service.   Each row contains a unique service, resource, and metric with the rated usage and cost, and the resulting cost for discounted items.
   - ***Instance_delete*** is a table for the range of months specified for each unique service instance (ie in the case of VPC Virtual Servers each virtual server is listed).  Each row contains details on that instance included rated usage and cost.
   - ***Usage_Summary*** is a pivot table showing each month estimated cost for each service.  Note if current month included amount is month to date.
   - ***MetricPlanSummary*** is a pivot table showing each month estimated cost for each service and related usage metric.  Note if current month included amount is month to date.
