# Other Included Scripts

## Table of Contents
1. Identity and Access Management Requirements
2. [classicConfigAnalysis.py](#classicconfiganalysis)
3. [classicConfigReport.py](#classicconfigreport)

### Identity & Access Management Requirements
| APIKEY                                     | Description                                                     | Min Access Permissions
|--------------------------------------------|-----------------------------------------------------------------|----------------------
| IBM Cloud API Key                          | API Key used access classic invoices & IBM Cloud Usage          | IAM Billing Viewer Role


#### classicConfigAnalysis

ClassicConfigAnalysis provides a detailed report in Excel format of all BareMetal server configurations in an account.  Including Public and Private network VLANs.

```azure
usage: classicConfigAnalysis.py [-h] [-u username] [-p password] [-a account] [-k apikey] [--output OUTPUT] [--load | --no-load] [--save | --no-save]

Configuration Report prints details of BareMetal Servers such as Network, VLAN, and hardware configuration

options:
  -h, --help            show this help message and exit
  -u username, --username username
                        IMS Userid
  -p password, --password password
                        IMS Password
  -a account, --account account
                        IMS Account
  -k apikey, --IC_API_KEY apikey
                        IBM Cloud API Key
  --output OUTPUT       Excel filename for output file. (including extension of .xlsx)
```

#### classicConfigReport

ClassicConfigAnalysis provides a detailed text based report all BareMetal server configurations in an account.  Including Public and Private network VLANs.  This report provide
more internal configuration data including Serial Numbers of components.

```azure
usage: classicConfigAnalysis.py [-h] [-u username] [-p password] [-a account] [-k apikey] [--output OUTPUT] [--load | --no-load] [--save | --no-save]

Configuration Report prints details of BareMetal Servers such as Network, VLAN, and hardware configuration

options:
  -h, --help            show this help message and exit
  -u username, --username username
                        IMS Userid
  -p password, --password password
                        IMS Password
  -a account, --account account
                        IMS Account
  -k apikey, --IC_API_KEY apikey
                        IBM Cloud API Key
  --output OUTPUT       Excel filename for output file. (including extension of .xlsx)
```