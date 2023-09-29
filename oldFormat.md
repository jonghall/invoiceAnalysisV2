# Old Format

## Output Description
invoiceAnalysis produces an Excel worksheet with multiple tabs (based on the parameters specified) from the consolidated IBM Cloud invoice data for SLIC accounts.  This data includes all classic Infrastructure usage (hourly & monthly) as well as
IBM Cloud/PaaS usage billed through SLIC for the purpose of reconciliation of invoice charges. In general the SLIC Invoice Month contains all RECURRING, NEW, ONE-TIME-CHARGE, and CREDIT invoices between the 20th of the previous month and the 19th of
the current month.   IBM Cloud PaaS Usage appears on the monthly RECURRING invoice 2 months in arrears.   (i.e April PaaS Usage, will appear on the June 1st, RECURRING invoice a which will be on the SLIC/CFTS invoice received at the end of June.  All
invoice and line item data is normalized to match the SLIC invoice billing dates.

### Detail Tabs
| Tab Name      | Included by Default | flag to change default| Description of Tab 
|---------------|---------------------|----------------------|-------------------
| Detail | True                | --no-detail | Detailed list of every invoice line item (including chidlren line items) from all invoices types between date ranges specified.

### Monthly Invoice Tabs
One tab is created for each month in range specified and used for reconciliation against invoices.   Only required tabs are created.  (ie if no credit in a month, then no credit tab will be created)

| Tab Name      | Included by Default | flag to change default| Description of Tab 
|---------------|---------------------|----------------------|-------------------
| IaaS_YYYY-MM  | True                | --no-reconcilliation | Table matching each portal invoice's IaaS Charges to the IBM SLIC/CFTS invoice.  IaaS Charges are split into three categories VMware License Charges, Classic COS Charges, and All other Classic IaaS Charges for each portal invoice, these amounts should match the SLIC/CFTS invoice amounts and aid in reconciliation. 
| IaaS_Detail_YYYY-MM | True                | --no-reconcilliation | Table provides a more detailed breakdown of the IaaS charges, and particular helps understand Other Classic IaaS Charges from the IaaS-YYYY-MM tab.   This is for information only, and detail will not match the IBM SLIC/CFTS invoice detail. 
| PaaS_YYYY-MM  | True                | --no-reconcilliation | Table matching portal PaaS charges on RECURRING invoice PaaS for that month, which are included in that months SLIC/CFTS invoice.  PaaS Charges are typically consolidated into one amount for type1, though the detail is provided at a service level on this tab to faciliate reconcillation.  PaaS charges are for usage 2 months in arrears. 
| Credit-YYYY-MM | True                | --no-reconcilliation | Table of Credit Invoics to their corresponding IBM SLIC/CFTS invoice(s). 

### Summary Tabs
Tabs are created to summarize usage data based on SLIC invoice month.   If a range of months is specified, months are displayed as columns in each tab and can be used to compare month to month changes

| Tab Name      | Incuded by Default | flag to change default | Description of Tab 
|---------------|--------------------|-----------------------|-------------------
| CategoryGroupSummary | True               | --no-summary          | A pivot table of all charges shown by Invoice Type and Category Groups by month. 
| CategoryDetail | True               | --no-summary          | A pivot table of all charges by Invoice Type, Category Group, Category and specific service Detail by month. 
| Classic_COS_Detail | False              | --cosdetail           | A table of all Classic Cloud Object Storage Usage (if used)
| HrlyVirtualServerPivot | True               | --no-serverdetail     | A table of Hourly Classic VSI's if they exist 
| MnthlyVirtualServerPivot | True               | --no-serverdetail     | A table of monthly Classic VSI's if they exist 
| HrlyBareMetalServerPivot | True               | --no-serverdetail     | A table of Hourly Bare Metal Servers if they exist 
| MnthlyBareMetalServerPivot | True               | --no-serverdetail     | A table of monthly Bare Metal Server if they exist 
| StoragePivot | False              | --storage             | A Table of all Block and File Storage allocations by location with custom notes (if used)
