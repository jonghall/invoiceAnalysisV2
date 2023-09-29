# IBM Cloud Classic Infrastructure Invoice Analysis Reports
This repository is a set of usage and billing scripts for IBM Cloud Classic Infrastructure and IBM Cloud that enable users to pull current and historical usage data and organize it in a meaningful way.

## Table of Contents
1. [Package Installation](#package-installation-instructions)
2. [Classic Invoice Analysis](invoiceAnalyss.md)
3. [IBM Cloud Usage Analysis](ibmCloudUsage.md)
4. [Other useful Scripts](other-scripts.md)


## Package Installation Instructions

1. Install Python 3.9+
2. Install required Python packages. 
````
$ pip install -r requirements.txt
````
3. For *Internal IBM IMS users* (employees) who wish to use internal credentials (with Yubikey 2FA) you must first manually uninstall the Public SoftLayer SDK and manually 
build the internal SDK for this script to function properly with internal credentials.  While running script you must be connected securely to IBM network via Global Protect VPN.
You will be prompted for your 2FA yubikey at each script execution.  Note you must invclude IMS account number in environment variable or command line.   [Internal SDK & Instructions](https://github.ibm.com/SoftLayer/internal-softlayer-cli)
```azure
$ pip uninstall SoftLayer
$ git clone https://github.ibm.com/SoftLayer/internal-softlayer-cli
$ cd internal-softlayer-cli
$ python setup.py install
$ ./islcli login

```
