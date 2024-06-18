#!/usr/bin/env python3
# Author: Jon Hall
# Copyright (c) 2023
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


__author__ = 'jonhall'
import SoftLayer, os, logging, logging.config, json, calendar, os.path, argparse, base64, re, urllib, yaml, strip_markdown
import pandas as pd
import numpy as np
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Personalization, Email, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId)
from datetime import datetime, tzinfo, timezone
from dateutil import tz
from calendar import monthrange
from dateutil.relativedelta import relativedelta
import ibm_boto3
from ibm_botocore.client import Config, ClientError
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalTaggingV1, GlobalSearchV2
from ibm_platform_services.resource_controller_v2 import *
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from dotenv import load_dotenv
from yaml import Loader
def setup_logging(default_path='logging.json', default_level=logging.info, env_key='LOG_CFG'):
    # read logging.json for log parameters to be ued by script
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

def getDescription(categoryCode, detail):
    # retrieve additional description detail for child records
    for item in detail:
        if 'categoryCode' in item:
            if item['categoryCode'] == categoryCode:
                return item['product']['description'].strip()
    return ""

def getStorageServiceUsage(categoryCode, detail):
    # retrieve storage details for description text
    for item in detail:
        if 'categoryCode' in item:
            if item['categoryCode'] == categoryCode:
                return item['description'].strip()
    return ""

def getCFTSInvoiceDate(invoiceDate):
    # Determine CFTS Invoice Month (20th of prev month - 19th of current month) are on current month CFTS invoice.
    if invoiceDate.day > 19:
        invoiceDate = invoiceDate + relativedelta(months=1)
    return invoiceDate.strftime('%Y-%m')

def getInvoiceDates(startdate,enddate):
    # Adjust start and dates to match CFTS Invoice cutoffs of 20th to end of day 19th 00:00 Dallas time on the 20th
    dallas = tz.gettz('US/Central')
    startdate = datetime(int(startdate[0:4]),int(startdate[5:7]),20,0,0,0,tzinfo=dallas) - relativedelta(months=1)
    enddate = datetime(int(enddate[0:4]),int(enddate[5:7]),20,0,0,0,tzinfo=dallas)
    return startdate, enddate

def createEmployeeClient(end_point_employee, employee_user, passw, token):
    """Creates a softlayer-python client that can make API requests for a given employee_user"""
    client_noauth = SoftLayer.Client(endpoint_url=end_point_employee)
    client_noauth.auth = None
    employee = client_noauth['SoftLayer_User_Employee']
    result = employee.performExternalAuthentication(employee_user, passw, token)
    # Save result['hash'] somewhere to not have to login for every API request
    client_employee = SoftLayer.employee_client(username=employee_user, access_token=result['hash'], endpoint_url=end_point_employee)
    return client_employee

def getAccountDetail():
    """
    retreive active users
    :return:
    """
    logging.info("Getting IMS Account {} Detail.".format(ims_account))
    try:
        account = client['Account'].getObject(id=ims_account, mask="id, companyName, country, email, accountStatus, billingInfo, bluemixAccountId, brand, datacentersWithSubnetAllocations, bluemixAccountLink, internalNotes,masterUser, proofOfConceptAccountFlag")
    except SoftLayer.SoftLayerAPIError as e:
        logging.error("Account::getObject: %s, %s" % (e.faultCode, e.faultString))
        quit(1)

    masteremail = ""
    masteriamId = ""
    if 'masterUser' in account:
        if 'email' in account['masterUser']:
            masteremail = account['masterUser']['email']

        if 'iamId' in account['masterUser']:
            masteriamId = account['masterUser']['iamId']


    if 'internalNotes' in account:
        notes = ""
        for note in account['internalNotes']:
            notes = notes + strip_markdown.strip_markdown(note['note'])
    else:
        notes = ""


    row = {
        'id': account['id'],
        'companyName': account['companyName'],
        'email': account['email'],
        'bluemixAccountId': account['bluemixAccountId'],
        'proofOfConceptAccountFlag': account['proofOfConceptAccountFlag'],
        'accountStatus': account['accountStatus']['name'],
        'billingInfoCreateDate': account['billingInfo']["createDate"],
        'billingInfoCurrency': account['billingInfo']["currency"]['name'],
        'brand': account['brand']["name"],
        'internalNotes': notes,
        'masterUserEmail': masteremail,
        'masterUserIamId': masteriamId
    }

    """ create dataframe of account detail """

    df = pd.DataFrame([row], columns=list(row.keys()))
    return df

def getUsers():
    """
    retreive active users
    :return:
    """
    logging.info("Getting IMS account {} users.".format(ims_account))
    try:
        userList = client['Account'].getUsers(id=ims_account, mask='id,accountId, companyName, createDate, displayName, firstName, lastName, email, iamId, isMasterUserFlag, managedByOpenIdConnectFlag, modifyDate,'
                                                                   ' openIdConnectUserName, sslVpnAllowedFlag, statusDate, username, userStatus, loginAttempts')
    except SoftLayer.SoftLayerAPIError as e:
        logging.error("Account::getUsers: %s, %s" % (e.faultCode, e.faultString))
        quit(1)

    data = []
    for user in userList:
        if 'companyName' in user:
            companyName = user['companyName']
        else:
            companyName = ""

        if 'email' in user:
            email = user['email']
        else:
            email = ""

        if 'sslVpnAllowedFlag' in user:
            sslVpnAllowedFlag = user['sslVpnAllowedFlag']
        else:
            sslVpnAllowedFlag = ""

        row = {
            'accountId': user['accountId'],
            'companyName': companyName,
            'id': user['id'],
            'displayName': user['displayName'],
            'fistName': user['firstName'],
            'LastName': user['lastName'],
            'createDate': user['createDate'],
            'modifyDate': user['modifyDate'],
            'statusDate': user['statusDate'],
            'iamId': user['iamId'],
            'email': email,
            'username': user['username'],
            'userStatus': user['userStatus']['name'],
            'isMasterUserFlag': user['isMasterUserFlag'],
            'managedByOpenConnectFlag': user['managedByOpenIdConnectFlag'],
            'sslVpnAllowedFlag': sslVpnAllowedFlag
        }

        if len(user['loginAttempts']) > 0:
            row['lastLoginAttempt'] = user['loginAttempts'][0]['createDate']
            row['lastLoginAttemptIpAddress'] = user['loginAttempts'][0]['ipAddress']
            if 'successFlag' in user['loginAttempts'][0]:
                row['lastLoginAttemptSuccessFlag'] = user['loginAttempts'][0]['successFlag']
            else:
                row['lastLoginAttemptSuccessFlag'] = ""

        data.append(row.copy())

    """ create dataframe of users """
    columns = ['accountId','companyName','id','displayName','fistName','LastName','createDate','modifyDate','statusDate','iamId','email','username','userStatus',
                'isMasterUserFlag','managedByOpenConnectFlag','sslVpnAllowedFlag','lastLoginAttempt','lastLoginAttemptIpAddress', 'lastLoginAttemptSuccessFlag']
    df = pd.DataFrame(data, columns=columns)
    return df

def getInvoiceList(startdate, enddate):
    # GET LIST OF PORTAL INVOICES BETWEEN DATES USING CENTRAL (DALLAS) TIME
    dallas=tz.gettz('US/Central')
    logging.info("Looking up invoices from {} to {}.".format(startdate.strftime("%m/%d/%Y %H:%M:%S%z"), enddate.strftime("%m/%d/%Y %H:%M:%S%z")))
    # filter invoices based on local dallas time that correspond to CFTS UTC cutoff
    logging.debug("invoiceList startDate: {}".format(startdate.astimezone(dallas).strftime("%m/%d/%Y %H:%M:%S")))
    logging.debug("invoiceList endDate: {}".format(enddate.astimezone(dallas).strftime("%m/%d/%Y %H:%M:%S")))
    try:
        invoiceList = client['Account'].getInvoices(id=ims_account, mask='id,accountId,createDate,typeCode,invoiceTotalAmount,invoiceTotalRecurringAmount,invoiceTopLevelItemCount', filter={
                'invoices': {
                    'createDate': {
                        'operation': 'betweenDate',
                        'options': [
                             {'name': 'startDate', 'value': [startdate.astimezone(dallas).strftime("%m/%d/%Y %H:%M:%S")]},
                             {'name': 'endDate', 'value': [enddate.astimezone(dallas).strftime("%m/%d/%Y %H:%M:%S")]}
                        ]
                    }
                }
        })
    except SoftLayer.SoftLayerAPIError as e:
        logging.error("Account::getInvoices: %s, %s" % (e.faultCode, e.faultString))
        quit(1)
    logging.debug("getInvoiceList account {}: {}".format(ims_account,invoiceList))
    if len(invoiceList) > 0:
        logging.info("IBM Cloud account {}".format(invoiceList[0]["accountId"]))
    return invoiceList

def parseChildren(row, parentCategory, parentDescription, children):
    """
    Parse Children Record if requested
    """
    global data

    for child in children:
        logging.debug(child)
        if float(child["recurringFee"]) > 0:
            row['RecordType'] = "Child"
            row["childBillingItemId"] = child["billingItemId"]
            row['childParentCategory'] = parentCategory
            row['childParentProduct'] = parentDescription
            if "dPart" in child:
                row["dPart"] = child["dPart"]
            else:
                row["dPart"] = ""
            if "itemCategory" in child["product"]:
                row["Category"] = child["product"]["itemCategory"]["name"]
            else:
                row["Category"] = "Unknown"
            if "group" in child["category"]:
                row["Category_Group"] = child["category"]["group"]["name"]
            else:
                row["Category_group"] = child['category']['name']
            if row["Category_Group"] == "StorageLayer":
                desc = child["description"].find(":")
                if desc == -1:
                    row["Description"] = child["description"]
                    row["childUsage"] = ""
                else:
                    # Parse usage details from child description for StorageLayer
                    row["Description"] = child["description"][0:desc]
                    if child["description"].find("API Requests") != -1:
                        row['childUsage'] = float(re.search("\d+", child["description"][desc:]).group())
                    elif child["description"].find("Snapshot Space") != -1:
                            row['childUsage'] = float(re.search("\d+", child["description"][desc:]).group())
                    elif child["description"].find("Replication for tier") != -1:
                            row['childUsage'] = 0
                    else:
                            row['childUsage'] = float(re.search("\d+([\.,]\d+)", child["description"][desc:]).group())
            else:
                desc = child["description"].find("- $")
                if desc == -1:
                    row["Description"] = child["description"]
                    row["childUsage"] = ""
                else:
                    # Parse usage details from child description
                    row["Description"] = child["description"][0:desc]
                    row['childUsage'] = re.search("([\d.]+)\s+(\S+)", child["description"][desc:]).group()
                    row['childUsage'] = float(row['childUsage'][0:row['childUsage'].find("Usage") - 3])
            row["totalRecurringCharge"] = 0
            row["childTotalRecurringCharge"] = round(float(child["recurringFee"]), 3)

            # Get product attributes for PaaS Product Code and DIV
            row["INV_PRODID"] = ""
            row["INV_DIV"] = ""
            row["PLAN_ID"] = ""
            row["FEATURE_ID"] = ""
            row["IS_PRIVATE_NETWORK_ONLY"] = ""
            row["DUAL_PATH_NETWORK"] = ""
            row["INELIGIBLE_FOR_ACCOUNT_DISCOUNT"] = ""
            if "attributes" in child["product"]:
                for attr in child["product"]["attributes"]:
                    if attr["attributeType"]["keyName"] == "BLUEMIX_PART_NUMBER":
                        row["INV_PRODID"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "BLUEMIX_SERVICE_PLAN_DIVISION":
                        row["INV_DIV"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "BLUEMIX_SERVICE_PLAN_ID":
                        row["PLAN_ID"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "BLUEMIX_SERVICE_PLAN_FEATURE_ID":
                        row["FEATURE_ID"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "IS_PRIVATE_NETWORK_ONLY":
                        row["IS_PRIVATE_NETWORK_ONLY"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "DUAL_PATH_NETWORK":
                        row["DUAL_PATH_NETWORK"] = attr["value"]
                    if attr["attributeType"]["keyName"] == "INELIGIBLE_FOR_ACCOUNT_DISCOUNT":
                        row["INELIGIBLE_FOR_ACCOUNT_DISCOUNT"] = attr["value"]

            # write child record
            data.append(row.copy())
            logging.debug("child {} {} {} RecurringFee: {}".format(row["childBillingItemId"], row["INV_PRODID"], row["Description"],
                                                               row["childTotalRecurringCharge"]))
            logging.debug(row)
    return

def getAccountNetworkStorage():
    """
    Build Dataframe with accounts current network storage
    """
    logging.info("Getting details on existing Network Storage in account.")
    try:
        networkStorage = client['Account'].getNetworkStorage(id=ims_account, mask="id, createDate, capacityGb, nasType, notes, username, provisionedIops, billingItem.id")
    except Exception as e:
        logging.error("Account::getNetworkStorage {}, {}".format(e.faultCode, e.faultString))
        quit(1)

    storage_df = pd.DataFrame(columns=[
                               'id',
                               'billingItemId',
                               'createDate',
                               'capacityGb',
                               'nasType',
                               'notes',
                               'username',
                               'provisionedIops',
                               'iopsTier',
                               ])
    for item in networkStorage:
        if 'billingItem' in item:
            if 'id' in item['billingItem']:
                billingItemId = item['billingItem']['id']
            else:
                billingItemId = ""
        else:
            billingItemId = ""

        if 'createDate' in item:
            createDate = item['createDate']
        else:
            createDate = ""

        if 'capacityGb' in item:
            capacityGb = item['capacityGb']
        else:
            capacityGb = ""

        if 'nasType' in item:
            nasType = item['nasType']
        else:
            nasType = ""

        if 'notes' in item:
            notes = urllib.parse.unquote(item['notes'])
        else:
            notes = ""

        if 'username' in item:
            username = item['username']
        else:
            username = ""

        if 'provisionedIops' in item:
            provisionedIops = item['provisionedIops']
            if float(capacityGb) > 0:
                iopsTier = round(float(provisionedIops)/float(capacityGb),0)
            else:
                iopsTier = ""
        else:
            provisionedIops = ""
            iopsTier = ""

        row = pd.DataFrame({
                            'id': [item['id']],
                            'billingItemId': [billingItemId],
                            'createDate': [createDate],
                            'capacityGb': [capacityGb],
                            'nasType': [nasType],
                            'notes': [notes],
                            'username': [username],
                            'provisionedIops': [provisionedIops],
                            'iopsTier': [iopsTier]
                            })

        storage_df = pd.concat([storage_df, row], ignore_index=True)

    return storage_df

def getInvoiceDetail(startdate, enddate):
    """
    Read invoice top level detail from range of invoices
    """
    global client, data, networkStorageDF
    # Create dataframe to work with for classic infrastructure invoices
    data = []

    dallas = tz.gettz('US/Central')

    # get list of invoices between start month and endmonth
    invoiceList = getInvoiceList(startdate, enddate)

    if invoiceList == None:
        return invoiceList

    for invoice in invoiceList:
        if (float(invoice['invoiceTotalAmount']) == 0) and (float(invoice['invoiceTotalRecurringAmount']) == 0):
            continue

        invoiceID = invoice['id']
        # To align to CFTS billing cutoffs display time in Dallas timezone.
        invoiceDate = datetime.strptime(invoice['createDate'], "%Y-%m-%dT%H:%M:%S%z").astimezone(dallas)
        invoiceTotalAmount = float(invoice['invoiceTotalAmount'])
        CFTSInvoiceDate = getCFTSInvoiceDate(invoiceDate)

        invoiceTotalRecurringAmount = float(invoice['invoiceTotalRecurringAmount'])
        invoiceType = invoice['typeCode']
        recurringDesc = ""
        if invoiceType == "NEW":
            serviceDateStart = invoiceDate
            # get last day of month
            serviceDateEnd= serviceDateStart.replace(day=calendar.monthrange(serviceDateStart.year,serviceDateStart.month)[1])

        if invoiceType == "CREDIT" or invoiceType == "ONE-TIME-CHARGE":
            serviceDateStart = invoiceDate
            serviceDateEnd = invoiceDate

        totalItems = invoice['invoiceTopLevelItemCount']

        # PRINT INVOICE SUMMARY LINE
        logging.info('Invoice: {} Date: {} Type:{} Items: {} Amount: ${:,.2f}'.format(invoiceID, datetime.strftime(invoiceDate, "%Y-%m-%d"), invoiceType, totalItems, invoiceTotalRecurringAmount))

        limit = 75 ## set limit of record returned
        for offset in range(0, totalItems, limit):
            if ( totalItems - offset - limit ) < 0:
                remaining = totalItems - offset
            logging.info("Retrieving %s invoice line items for Invoice %s at Offset %s of %s" % (limit, invoiceID, offset, totalItems))

            try:
                """
                       if --storage specified on command line provide
                       additional mapping of current storage comments to billing
                       records billingItem.resourceTableId is link to storage.
                       note: user must have classic Infrastructure access for storage components
                """

                Billing_Invoice = client['Billing_Invoice'].getInvoiceTopLevelItems(id=invoiceID, limit=limit, offset=offset,
                                    mask="id, billingItemId,categoryCode,category,category.group,dPart,hourlyFlag,hostName,domainName,location,notes,product.description,product.taxCategory,product.attributes.attributeType," \
                                         "createDate,totalRecurringAmount,totalOneTimeAmount,usageChargeFlag,hourlyRecurringFee,children.billingItemId,children.dPart,children.description,children.category.group," \
                                         "children.categoryCode,children.product,children.product.taxCategory,children.product.attributes,children.product.attributes.attributeType,children.recurringFee")
            except SoftLayer.SoftLayerAPIError as e:
                logging.error("Billing_Invoice::getInvoiceTopLevelItems: %s, %s" % (e.faultCode, e.faultString))
                quit(1)
            count = 0


            # ITERATE THROUGH DETAIL
            for item in Billing_Invoice:
                logging.debug(item)
                totalOneTimeAmount = float(item['totalOneTimeAmount'])
                billingItemId = item['billingItemId']
                if "group" in item["category"]:
                    categoryGroup = item["category"]["group"]["name"]
                else:
                    categoryGroup = "Other"
                category = item["categoryCode"]
                categoryName = item["category"]["name"]
                taxCategory = item['product']['taxCategory']['name']
                description = item['product']['description']
                memory = getDescription("ram", item["children"])
                os = getDescription("os", item["children"])

                if 'notes' in item:
                    billing_notes = item["notes"]
                else:
                    billing_notes = ""

                if 'location' in item:
                    location = item["location"]["longName"]
                else:
                    location = ""

                if 'hostName' in item:
                    if 'domainName' in item:
                        hostName = item['hostName']+"."+item['domainName']
                    else:
                        hostName = item['hostName']
                else:
                    hostName = ""

                if "dPart" in item:
                    dPart = item["dPart"]
                else:
                    dPart = ""

                recurringFee = float(item['totalRecurringAmount'])
                NewEstimatedMonthly = 0

                """ Get d-code for Parent Item """
                INV_PRODID = ""
                INV_DIV = ""
                PLAN_ID = ""
                if "attributes" in item["product"]:
                    for attr in item["product"]["attributes"]:
                        if attr["attributeType"]["keyName"] == "BLUEMIX_PART_NUMBER":
                            INV_PRODID = attr["value"]
                        if attr["attributeType"]["keyName"] == "BLUEMIX_SERVICE_PLAN_DIVISION":
                            INV_DIV = attr["value"]
                        if attr["attributeType"]["keyName"] == "BLUEMIX_SERVICE_PLAN_ID":
                            PLAN_ID = attr["value"]

                # If Hourly calculate hourly rate and total hours
                if item["hourlyFlag"]:
                    # if hourly charges are previous month usage
                    serviceDateStart = invoiceDate - relativedelta(months=1)
                    serviceDateEnd = serviceDateStart.replace(day=calendar.monthrange(serviceDateStart.year, serviceDateStart.month)[1])
                    recurringDesc = "IaaS Usage"
                    hourlyRecurringFee = 0
                    hours = 0
                    if "hourlyRecurringFee" in item:
                        if float(item["hourlyRecurringFee"]) > 0:
                            hourlyRecurringFee = float(item['hourlyRecurringFee'])
                            for child in item["children"]:
                                if "hourlyRecurringFee" in child:
                                    hourlyRecurringFee = hourlyRecurringFee + float(child['hourlyRecurringFee'])
                            hours = round(float(recurringFee) / hourlyRecurringFee)            # Not an hourly billing item
                else:
                    if taxCategory == "PaaS":
                        # Non Hourly PaaS Usage from actual usage two months prior
                        serviceDateStart = invoiceDate - relativedelta(months=2)
                        serviceDateEnd = serviceDateStart.replace(day=calendar.monthrange(serviceDateStart.year, serviceDateStart.month)[1])
                        recurringDesc = "Platform Service Usage"
                    elif taxCategory == "IaaS":
                        if invoiceType == "RECURRING":
                            """ fix classic archive to be usage based"""
                            if categoryName == "Archive Storage Repository":
                                serviceDateStart = invoiceDate - relativedelta(months=1)
                                serviceDateEnd = serviceDateStart.replace(day=calendar.monthrange(serviceDateStart.year, serviceDateStart.month)[1])
                                recurringDesc = "IaaS Usage"
                            else:
                                serviceDateStart = invoiceDate
                                serviceDateEnd = serviceDateStart.replace(day=calendar.monthrange(serviceDateStart.year, serviceDateStart.month)[1])
                                recurringDesc = "IaaS Monthly"
                    elif taxCategory == "HELP DESK":
                        serviceDateStart = invoiceDate
                        serviceDateEnd = serviceDateStart.replace(
                            day=calendar.monthrange(serviceDateStart.year, serviceDateStart.month)[1])
                        recurringDesc = "Support Charges"
                    hourlyRecurringFee = 0
                    hours = 0

                # if storage flag specified, lookup existing note from object stored in dataframe
                if storageFlag and (category == "storage_service_enterprise" or category == "performance_storage_iops" or category == "storage_as_a_service"):
                    result = networkStorageDF.query('billingItemId == @billingItemId')
                    if len(result) > 0:
                        storage_notes = result['notes'].values[0]
                    else:
                        storage_notes = ""
                else:
                    storage_notes = ""
                if category == "storage_service_enterprise":
                    iops = getDescription("storage_tier_level", item["children"])
                    storage = getDescription("performance_storage_space", item["children"])
                    snapshot = getDescription("storage_snapshot_space", item["children"])
                    if snapshot == "":
                        description = storage + " " + iops + " "
                    else:
                        description = storage+" " + iops + " with " + snapshot
                elif category == "performance_storage_iops":
                    iops = getDescription("performance_storage_iops", item["children"])
                    storage = getDescription("performance_storage_space", item["children"])
                    description = storage + " " + iops
                elif category == "storage_as_a_service":
                    if item["hourlyFlag"]:
                        model = "Hourly"
                        for child in item["children"]:
                            if "hourlyRecurringFee" in child:
                                hourlyRecurringFee = hourlyRecurringFee + float(child['hourlyRecurringFee'])
                        if hourlyRecurringFee > 0:
                            hours = round(float(recurringFee) / hourlyRecurringFee)
                        else:
                            hours = 0
                    else:
                        model = "Monthly"
                    space = getStorageServiceUsage('performance_storage_space', item["children"])
                    tier = getDescription("storage_tier_level", item["children"])
                    snapshot = getDescription("storage_snapshot_space", item["children"])
                    if space == "" or tier == "":
                        description = model + " File Storage"
                    else:
                        if snapshot == "":
                            description = model + " File Storage " + space + " at " + tier
                        else:
                            snapshotspace = getStorageServiceUsage('storage_snapshot_space', item["children"])
                            description = model + " File Storage " + space + " at " + tier + " with " + snapshotspace
                elif category == "guest_storage":
                        imagestorage = getStorageServiceUsage("guest_storage_usage", item["children"])
                        if imagestorage == "":
                            description = description.replace('\n', " ")
                        else:
                            description = imagestorage
                else:
                    description = description.replace('\n', " ")


                if invoiceType == "NEW":
                    # calculate non pro-rated amount for use in forecast
                    daysInMonth = monthrange(invoiceDate.year, invoiceDate.month)[1]
                    daysLeft = daysInMonth - invoiceDate.day + 1
                    dailyAmount = recurringFee / daysLeft
                    NewEstimatedMonthly = dailyAmount * daysInMonth

                recordType = "Parent"

                # Append record to dataframe
                row = {'Portal_Invoice_Date': invoiceDate.strftime("%Y-%m-%d"),
                       'Portal_Invoice_Time': invoiceDate.strftime("%H:%M:%S%z"),
                       'Service_Date_Start': serviceDateStart.strftime("%Y-%m-%d"),
                       'Service_Date_End': serviceDateEnd.strftime("%Y-%m-%d"),
                       'IBM_Invoice_Month': CFTSInvoiceDate,
                       'Portal_Invoice_Number': invoiceID,
                       'RecordType': recordType,
                       'BillingItemId': billingItemId,
                       'hostName': hostName,
                       'location': location,
                       'billing_notes': billing_notes,
                       'Category_Group': categoryGroup,
                       'Category': categoryName,
                       'dPart': dPart,
                       'TaxCategory': taxCategory,
                       'Description': description,
                       'Memory': memory,
                       'OS': os,
                       'Hourly': item["hourlyFlag"],
                       'Usage': item["usageChargeFlag"],
                       'Hours': hours,
                       'HourlyRate': round(hourlyRecurringFee,5),
                       'totalRecurringCharge': round(recurringFee,3),
                       'totalOneTimeAmount': float(totalOneTimeAmount),
                       'NewEstimatedMonthly': float(NewEstimatedMonthly),
                       'InvoiceTotal': float(invoiceTotalAmount),
                       'InvoiceRecurring': float(invoiceTotalRecurringAmount),
                       'Type': invoiceType,
                       'Recurring_Description': recurringDesc,
                       'childTotalRecurringCharge': 0,
                       'INV_PRODID': INV_PRODID,
                       'INV_DIV': INV_DIV,
                       'PLAN_ID': PLAN_ID,
                        }
                if storageFlag:
                    row["storage_notes"] = storage_notes


                # write parent record
                data.append(row.copy())
                logging.info("parent {} {} RecurringFee: {}".format(row["BillingItemId"], row["Description"],row["totalRecurringCharge"]))
                logging.debug(row)

                if len(item["children"]) > 0:
                    parseChildren(row, categoryName, description, item["children"])

    columns = ['Portal_Invoice_Date',
               'Portal_Invoice_Time',
               'Service_Date_Start',
               'Service_Date_End',
               'IBM_Invoice_Month',
               'Portal_Invoice_Number',
               'Type',
               'RecordType',
               'BillingItemId',
               'hostName',
               'location',
               'Category_Group',
               'Category',
               'TaxCategory',
               'dPart',
               'Description',
               'Memory',
               'OS',
               'billing_notes',
               'Hourly',
               'Usage',
               'Hours',
               'HourlyRate',
               'totalRecurringCharge',
               'NewEstimatedMonthly',
               'totalOneTimeAmount',
               'InvoiceTotal',
               'InvoiceRecurring',
               'Recurring_Description',
               'childBillingItemId',
               'childParentCategory',
               'childParentProduct',
               'childUsage',
               'childTotalRecurringCharge',
               'INV_PRODID',
               'INV_DIV',
               'PLAN_ID',
               'FEATURE_ID',
               'IS_PRIVATE_NETWORK_ONLY',
               'DUAL_PATH_NETWORK',
               'INELIGIBLE_FOR_ACCOUNT_DISCOUNT']
    if storageFlag:
        columns.append("storage_notes")

    df = pd.DataFrame(data, columns=columns)

    return df

def createReport(filename, classicUsage):

    """
    New Format Output breaks out invoices at a product code level.  IaaS and PaaS are more accurately reflected on invoices.
    Though CFTS physical invoices received are seperate for IaaS vs PaaS the breakdown is combined onto one tab.
    """
    def createDetailTab(classicUsage):
        """
        Write detail tab to excel
        """
        logging.info("Creating detail tab.")
        classicUsage.to_excel(writer, sheet_name='Detail')
        usdollar = workbook.add_format({'num_format': '$#,##0.00'})
        format2 = workbook.add_format({'align': 'left'})
        worksheet = writer.sheets['Detail']
        worksheet.set_column('Q:AA', 18, usdollar)
        worksheet.set_column('AB:AB', 18, format2)
        worksheet.set_column('AC:AC', 18, usdollar)
        worksheet.set_column('W:W', 18, format2 )
        totalrows,totalcols=classicUsage.shape
        worksheet.autofilter(0,0,totalrows,totalcols)
        return

    def createAccountDetailTab(accountDetail):
        """
        Write account to excel
        """
        logging.info("Creating account tab.")
        accountDetail.to_excel(writer, sheet_name='AccountDetail')
        format2 = workbook.add_format({'align': 'left'})
        worksheet = writer.sheets['AccountDetail']
        worksheet.set_column('A:A', 5, format2)
        worksheet.set_column('B:B', 8, format2)
        worksheet.set_column('C:C', 40, format2)
        worksheet.set_column('D:D', 30, format2)
        worksheet.set_column('E:E', 30, format2)
        worksheet.set_column('F:F', 25, format2)
        worksheet.set_column('G:G', 15, format2)
        worksheet.set_column('H:J', 25, format2)
        worksheet.set_column('K:L', 40, format2)
        worksheet.set_column('L:L', 30, format2)
        worksheet.set_column('M:M', 18, format2)
        #totalrows,totalcols=accountDetail.shape
        #worksheet.autofilter(0,0,totalrows,totalcols)
        return
    def createUserTab(userList):
        """
        Write usertab to excel
        """
        logging.info("Creating user tab.")
        userList.to_excel(writer, sheet_name='Users')
        format2 = workbook.add_format({'align': 'left'})
        worksheet = writer.sheets['Users']
        worksheet.set_column('A:S', 20, format2)
        totalrows,totalcols=userList.shape
        worksheet.autofilter(0,0,totalrows,totalcols)
        return
    def createCategoryGroupSummary(classicUsage):
        """
        Map Portal Invoices to SLIC Invoices / Create Top Sheet per SLIC month
        """
    
        if len(classicUsage)>0:
            logging.info("Creating CategoryGroupSummary Tab.")
            parentRecords= classicUsage.query('RecordType == ["Parent"]')
            invoiceSummary = pd.pivot_table(parentRecords, index=["Type","dPart", "Category_Group", "Category"],
                                            values=["totalAmount"],
                                            columns=['IBM_Invoice_Month'],
                                            aggfunc={'totalAmount': np.sum,}, margins=True, margins_name="Total", fill_value=0).\
                                            rename(columns={'totalRecurringCharge': 'TotalRecurring'})
            invoiceSummary.to_excel(writer, sheet_name='CategoryGroupSummary')
            worksheet = writer.sheets['CategoryGroupSummary']
            format1 = workbook.add_format({'num_format': '$#,##0.00'})
            format2 = workbook.add_format({'align': 'left'})
            worksheet.set_column("A:A", 20, format2)
            worksheet.set_column("B:B", 20, format2)
            worksheet.set_column("C:C", 40, format2)
            worksheet.set_column("D:D", 60, format2)
            worksheet.set_column("E:ZZ", 18, format1)
        return
    def createCategooryDetail(classicUsage):
        """
        Build a pivot table by Category with totalRecurringCharges
        tab name CategorySummary
        """
    
        if len(classicUsage) > 0:
            logging.info("Creating CategoryDetail Tab.")
            parentRecords = classicUsage.query('RecordType == ["Parent"]')
            categorySummary = pd.pivot_table(parentRecords, index=["Type", "Category_Group", "Category", "Description"],
                                             values=["totalAmount"],
                                             columns=['IBM_Invoice_Month'],
                                             aggfunc={'totalAmount': np.sum}, margins=True, margins_name="Total", fill_value=0)
            categorySummary.to_excel(writer, sheet_name='CategoryDetail')
            worksheet = writer.sheets['CategoryDetail']
            format1 = workbook.add_format({'num_format': '$#,##0.00'})
            format2 = workbook.add_format({'align': 'left'})
            worksheet.set_column("A:A", 20, format2)
            worksheet.set_column("B:C", 50, format2)
            worksheet.set_column("D:D", 60, format2)
            worksheet.set_column("E:ZZ", 18, format1)
        return
    def createClassicCOS(classicUsage):
        """
        Build a pivot table of Classic Object Storage that displays charges appearing on CFTS invoice
        """
        if len(classicUsage) > 0:
            iaascosRecords = classicUsage.query('RecordType == ["Child"] and childParentProduct == ["Cloud Object Storage - S3 API"]')
            if len(iaascosRecords) > 0:
                logging.info("Creating Classic_COS_Detail Tab.")
                iaascosSummary = pd.pivot_table(iaascosRecords, index=["Type", "Category_Group", "childParentProduct", "Category", "Description"],
                                                 values=["childTotalRecurringCharge"],
                                                 columns=['IBM_Invoice_Month'],
                                                 aggfunc={'childTotalRecurringCharge': np.sum}, fill_value=0, margins=True, margins_name="Total")
                iaascosSummary.to_excel(writer, sheet_name='Classic_COS_Detail')
                worksheet = writer.sheets['Classic_COS_Detail']
                format1 = workbook.add_format({'num_format': '$#,##0.00'})
                format2 = workbook.add_format({'align': 'left'})
                worksheet.set_column("A:A", 20, format2)
                worksheet.set_column("B:E", 40, format2)
                worksheet.set_column("F:ZZ", 18, format1)
        return
    def createTopSheet(classicUsage):
        """
        Build a pivot table of items that typically show on CFTS invoice at child level
        paasCodes that appear on IaaS Invoice
        """

        months = classicUsage.IBM_Invoice_Month.unique()
        for i in months:
            logging.info("Creating CFTS Invoice Top Sheet tab for {}.".format(i))

            if len(classicUsage) > 0:
                """
                Get all the BSS child records with d-code in one of the IaaS divisions
                Exception D026XZX DNS appears on IaaS Invoice even though not in IaaS division
                """
                logging.info("Creating Infrastructure-as-a-Service detail for {}.".format(i))
                iaasDivs = ["7D", "SQ", "5M", "U3", "U6","U7"]
                childRecords = classicUsage.query('RecordType == ["Child"] and (INV_DIV in @iaasDivs or INV_PRODID == "D026XZX") and totalAmount > 0 and IBM_Invoice_Month == @i').copy()

                """ 
                Populate lineItemCateoogry with meaningful service name so that rows summarize correctly consistent with CFTS
                """
                childRecords["lineItemCategory"] = childRecords["Description"]
                for index, row in childRecords.iterrows():
                    part_number = row["INV_PRODID"].strip()
                    if part_number in dpartDescriptions:
                        childRecords.at[index, "lineItemCategory"] = dpartDescriptions[part_number]

                """ Get the parent Classic IaaS records not metered in BSS """
                iaasRecords = classicUsage.query('(IBM_Invoice_Month == @i and RecordType == ["Parent"] and TaxCategory != ["PaaS"] and totalAmount > 0)').copy()

                """
                Create a new lineItemCategory column for table based on Category
                Adjust VMware Licensing so the description is meaingful
                """
                iaasRecords["lineItemCategory"] = iaasRecords["Category"]
                for index, row in iaasRecords.iterrows():
                    if row["Category_Group"] == "Virtual Servers and Attached Services":
                        iaasRecords.at[index, "lineItemCategory"] = "Virtual Servers and Attached Services"
                    elif row["Category"] == "Software License":
                        if "vSAN" in row["Description"]:
                            iaasRecords.at[index, "lineItemCategory"] = "Software License VMware vSAN"
                        elif "NSX" in row["Description"]:
                            iaasRecords.at[index, "lineItemCategory"] = "Software License VMware NSX"
                        else:
                            iaasRecords.at[index, "lineItemCategory"] = "Software License"
                    elif row["Category_Group"] == "Other" and (row["Category"] == "Network Vlan" or row["Category"] == "Network Message Delivery") :
                        iaasRecords.at[index, "lineItemCategory"] = "Network Other"

                combined = pd.concat([childRecords, iaasRecords])

                iaasInvoice = pd.pivot_table(combined, index=["Portal_Invoice_Number", "Type", "Portal_Invoice_Date", "Service_Date_Start", "Service_Date_End", "dPart", "lineItemCategory"],
                                              values=["totalAmount"],
                                              aggfunc=np.sum, margins=True,
                                              margins_name="Total", fill_value=0)

                iaasInvoice.to_excel(writer, sheet_name='TopSheet_{}'.format(i),startcol=0, startrow=1)
                worksheet = writer.sheets['TopSheet_{}'.format(i)]
                format1 = workbook.add_format({'num_format': '$#,##0.00'})
                format2 = workbook.add_format({'align': 'left'})
                boldtext = workbook.add_format({'bold': True})
                worksheet.write(0, 0, "Infrastructure as a Service Charges appearing in {}".format(i),boldtext)
                worksheet.set_column("A:F", 20, format2)
                worksheet.set_column("G:G", 70, format2)
                worksheet.set_column("H:ZZ", 18, format1)

                logging.info("Creating Platform as a Service Detail for {}.".format(i))
                """
                Include all divisions that are not considered IaaS.  Exceptions: D026XZX DNS appears on IaaS invoice even though not in IaaS division
                """

                paasRecords = classicUsage.query('RecordType == ["Child"] and TaxCategory == ["PaaS"] and INV_DIV not in @iaasDivs and INV_PRODID != "D026XZX" and IBM_Invoice_Month == @i').copy()

                """ 
                Replace lineItemCategory with meaningful service name so that rows summarize correctly consistent with CFTS
                """
                paasRecords["lineItemCategory"] = paasRecords["childParentProduct"]
                for index, row in paasRecords.iterrows():
                    part_number = row["INV_PRODID"].strip()
                    if part_number in dpartDescriptions:
                        paasRecords.at[index, "lineItemCategory"] = dpartDescriptions[part_number]

                if len(paasRecords) > 0:
                    startrow = len(iaasInvoice.index) + 5
                    paasSummary = pd.pivot_table(paasRecords, index=["Portal_Invoice_Number", "Type", "Portal_Invoice_Date","Service_Date_Start", "Service_Date_End","dPart", "lineItemCategory"],
                                                    values=["totalAmount"],
                                                    aggfunc=np.sum, margins=True,
                                                    fill_value=0)
                    paasSummary.to_excel(writer, 'TopSheet_{}'.format(i),startcol=0, startrow=startrow)
                    worksheet.write(startrow-1,0, "Platform as a Service Charges appearing in {}".format(i), boldtext)

                creditItems = classicUsage.query('Type == "CREDIT" and IBM_Invoice_Month == @i').copy()

                if len(creditItems) > 0:
                    if len(paasRecords) > 0:
                        startrow = startrow + len(paasSummary.index) + 4
                    else:
                        startrow = len(iaasInvoice.index) + 5

                    logging.info("Creating Credit detail for {}.".format(i))

                    creditItems["lineItemCategory"] = creditItems["Category"]
                    pivot = pd.pivot_table(creditItems, index=["Portal_Invoice_Number", "Type", "Portal_Invoice_Date","Service_Date_Start", "Service_Date_End","dPart", "lineItemCategory"],
                                           values=["totalAmount"],
                                           aggfunc=np.sum, margins=True, margins_name="Total",
                                           fill_value=0)
                    pivot.to_excel(writer, sheet_name='TopSheet_{}'.format(i),startcol=0, startrow=startrow)
                    worksheet.write(startrow - 1, 0, "Credit detail appearing in {}".format(i), boldtext)

        return
    def createStorageTab(classicUsage):
        """
        Build a pivot table for Storage as a Service by Volume Name
        """

        storage = classicUsage.query(
            'Category == ["Storage As A Service"] or Category == ["Endurance"] and Type == ["RECURRING"]')

        if len(storage) > 0:
            logging.info("Creating Storage Detail Tab.")
            format_usdollar = workbook.add_format({'num_format': '$#,##0.00'})
            format_leftjustify = workbook.add_format()
            format_leftjustify.set_align('left')
            st = pd.pivot_table(storage,
                                index=["location", "Category", "billing_notes", "storage_notes", "Description"],
                                values=["totalRecurringCharge"],
                                columns=['IBM_Invoice_Month'],
                                aggfunc={'totalRecurringCharge': np.sum}, fill_value=0).rename(
                columns={'totalRecurringCharge': 'TotalRecurring'})

            """
            Create Storage-as-a-Service Tab
            """
            if st is not None:
                st.to_excel(writer, sheet_name='StoragePivot')
                worksheet = writer.sheets['StoragePivot']
                worksheet.set_column("A:C", 30, format_leftjustify)
                worksheet.set_column("D:D", 50, format_leftjustify)
                worksheet.set_column("E:ZZ", 18, format_usdollar)

        return
    def createHourlyVirtualServers(classicUsage):
        """
        Build a pivot table for Hourly VSI's with totalRecurringCharges
        """
        virtualServers = classicUsage.query('Category == ["Computing Instance"] and Hourly == [True]')
        if len(virtualServers) > 0:
            logging.info("Creating Hourly VSI Tab.")
            virtualServerPivot = pd.pivot_table(virtualServers, index=["Description", "OS"],
                                                values=["Hours", "totalRecurringCharge"],
                                                columns=['IBM_Invoice_Month'],
                                                aggfunc={'Description': len, 'Hours': np.sum,
                                                         'totalRecurringCharge': np.sum}, fill_value=0). \
                rename(columns={"Description": 'qty', 'Hours': 'Total Hours', 'totalRecurringCharge': 'TotalRecurring'})

            virtualServerPivot.to_excel(writer, sheet_name='HrlyVirtualServers')
            format_leftjustify = workbook.add_format()
            format_leftjustify.set_align('left')
            worksheet = writer.sheets['HrlyVirtualServers']
            worksheet.set_column('A:B', 40, format_leftjustify)

        return
    def createMonthlyVirtualServers(classicUsage):
        """
        Build a pivot table for Monthly VSI's with totalRecurringCharges
        """
        monthlyVirtualServers = classicUsage.query('Category == ["Computing Instance"] and Hourly == [False]')
        if len(monthlyVirtualServers) > 0:
            logging.info("Creating Monthly VSI Tab.")
            virtualServerPivot = pd.pivot_table(monthlyVirtualServers, index=["Description", "OS"],
                                                values=["totalRecurringCharge"],
                                                columns=['IBM_Invoice_Month'],
                                                aggfunc={'Description': len, 'totalRecurringCharge': np.sum},
                                                fill_value=0). \
                rename(columns={"Description": 'qty', 'totalRecurringCharge': 'TotalRecurring'})
            virtualServerPivot.to_excel(writer, sheet_name='MnthlyVirtualServers')
            format_leftjustify = workbook.add_format()
            format_leftjustify.set_align('left')
            worksheet = writer.sheets['MnthlyVirtualServers']
            worksheet.set_column('A:B', 40, format_leftjustify)
        return
    def createHourlyBareMetalServers(classicUsage):
        """
        Build a pivot table for Hourly Bare Metal with totalRecurringCharges
        """
        bareMetalServers = classicUsage.query('Category == ["Server"]and Hourly == [True]')
        if len(bareMetalServers) > 0:
            logging.info("Creating Hourly Bare Metal Tab.")
            pivot = pd.pivot_table(bareMetalServers, index=["Description", "OS"],
                                   values=["Hours", "totalRecurringCharge"],
                                   columns=['IBM_Invoice_Month'],
                                   aggfunc={'Description': len, 'totalRecurringCharge': np.sum}, fill_value=0). \
                rename(columns={"Description": 'qty', 'Hours': np.sum, 'totalRecurringCharge': 'TotalRecurring'})
            pivot.to_excel(writer, sheet_name='HrlyBaremetalServers')
            format_leftjustify = workbook.add_format()
            format_leftjustify.set_align('left')
            worksheet = writer.sheets['HrlyBaremetalServers']
            worksheet.set_column('A:B', 40, format_leftjustify)
        return
    def createMonthlyBareMetalServers(classicUsage):
        """
        Build a pivot table for Monthly Bare Metal with totalRecurringCharges
        """
        monthlyBareMetalServers = classicUsage.query('Category == ["Server"] and Hourly == [False]')
        if len(monthlyBareMetalServers) > 0:
            logging.info("Creating Monthly Bare Metal Tab.")
            pivot = pd.pivot_table(monthlyBareMetalServers, index=["location", "Description", "OS"],
                                   values=["totalRecurringCharge"],
                                   columns=['IBM_Invoice_Month'],
                                   aggfunc={'Description': len, 'totalRecurringCharge': np.sum}, fill_value=0). \
                rename(columns={"Description": 'qty', 'totalRecurringCharge': 'TotalRecurring'})
            pivot.to_excel(writer, sheet_name='MthlyBaremetalServers')
            format_leftjustify = workbook.add_format()
            format_leftjustify.set_align('left')
            worksheet = writer.sheets['MthlyBaremetalServers']
            worksheet.set_column('A:C', 40, format_leftjustify)
        return

    global writer, workbook

    # Write dataframe to excel
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook = writer.book
    logging.info("Creating {}.".format(filename))

    # combine one time amounts and total recurring charge in datafrane
    classicUsage["totalAmount"] = classicUsage["totalOneTimeAmount"] + classicUsage["totalRecurringCharge"] + classicUsage["childTotalRecurringCharge"]

    # create pivots for various tabs for Type2 SLIC based on flags
    if accountFlag:
        createAccountDetailTab(accountDetail)
    if userFlag:
        createUserTab(userList)

    if detailFlag:
        createDetailTab(classicUsage)

    if reconciliationFlag:
        createTopSheet(classicUsage)

    if summaryFlag:
        createCategoryGroupSummary(classicUsage)
        createCategooryDetail(classicUsage)

    if serverDetailFlag:
        createHourlyVirtualServers(classicUsage)
        createMonthlyVirtualServers(classicUsage)
        createHourlyBareMetalServers(classicUsage)
        createMonthlyBareMetalServers(classicUsage)

    if cosdetailFlag:
        createClassicCOS(classicUsage)

    if storageFlag:
        createStorageTab(classicUsage)

    # If BSS Flag set and using APIKEY then pull BSS Usage for corresponding month
    if bssFlag and args.IC_API_KEY != None:
        getBSS()

    writer.close()
    return

def multi_part_upload(bucket_name, item_name, file_path):
    try:
        logging.info("Starting file transfer for {0} to bucket: {1}".format(item_name, bucket_name))
        # set 5 MB chunks
        part_size = 1024 * 1024 * 5

        # set threadhold to 15 MB
        file_threshold = 1024 * 1024 * 15

        # set the transfer threshold and chunk size
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold,
            multipart_chunksize=part_size
        )

        # the upload_fileobj method will automatically execute a multi-part upload
        # in 5 MB chunks for all files over 15 MB
        with open(file_path, "rb") as file_data:
            cos.Object(bucket_name, item_name).upload_fileobj(
                Fileobj=file_data,
                Config=transfer_config
            )
        logging.info("Transfer for {0} complete".format(item_name))
    except ClientError as be:
        logging.error("CLIENT ERROR: {0}".format(be))
    except Exception as e:
        logging.error("Unable to complete multi-part upload: {0}".format(e))
    return

def sendEmail(startdate, enddate, sendGridTo, sendGridFrom, sendGridSubject, sendGridApi, outputname):
    # Send output to email distributionlist via SendGrid

    html = ("<p><b>invoiceAnalysis Output Attached for {} to {} </b></br></p>".format(datetime.strftime(startdate, "%m/%d/%Y"), datetime.strftime(enddate, "%m/%d/%Y")))

    to_list = Personalization()
    for email in sendGridTo.split(","):
        to_list.add_to(Email(email))

    message = Mail(
        from_email=sendGridFrom,
        subject=sendGridSubject,
        html_content=html
    )

    message.add_personalization(to_list)

    # create attachment from file
    file_path = os.path.join("./", outputname)
    with open(file_path, 'rb') as f:
        data = f.read()
        f.close()
    encoded = base64.b64encode(data).decode()
    attachment = Attachment()
    attachment.file_content = FileContent(encoded)
    attachment.file_type = FileType('application/xlsx')
    attachment.file_name = FileName(outputname)
    attachment.disposition = Disposition('attachment')
    attachment.content_id = ContentId('invoiceAnalysis')
    message.attachment = attachment
    try:
        sg = SendGridAPIClient(sendGridApi)
        response = sg.send(message)
        logging.info("Email Send succesfull to {}, status code = {}.".format(sendGridTo,response.status_code))
    except Exception as e:
        logging.error("Email Send Error, status code = %s." % e.to_dict)
    return

def getBSS():
    """
     call functions in ibmCloudUsage to get corresponding IBM Cloud BSS metered usage
     depending on account configuration this usage data may not include relevant discounts
     CFTS Invoices contain IBM Cloud BSS Metered usage from 2 months prior
    """

    def getAccountId(IC_API_KEY):
        ##########################################################
        ## Get AccountId for this API Key
        ##########################################################

        try:
            api_key = iam_identity_service.get_api_keys_details(
                iam_api_key=IC_API_KEY
            ).get_result()
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

        return api_key["account_id"]
    def createSDK(IC_API_KEY):
        """
        Create SDK clients
        """
        global usage_reports_service, resource_controller_service, iam_identity_service, global_search_service

        try:
            authenticator = IAMAuthenticator(IC_API_KEY)
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

        try:
            iam_identity_service = IamIdentityV1(authenticator=authenticator)
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

        try:
            usage_reports_service = UsageReportsV4(authenticator=authenticator)
            usage_reports_service.enable_retries(max_retries=5, retry_interval=1.0)
            usage_reports_service.set_http_config({'timeout': 120})
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

        try:
            resource_controller_service = ResourceControllerV2(authenticator=authenticator)
            resource_controller_service.enable_retries(max_retries=5, retry_interval=1.0)
            resource_controller_service.set_http_config({'timeout': 120})
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)

        try:
            global_search_service = GlobalSearchV2(authenticator=authenticator)
            global_search_service.enable_retries(max_retries=5, retry_interval=1.0)
            global_search_service.set_http_config({'timeout': 120})
        except ApiException as e:
            logging.error("API exception {}.".format(str(e)))
            quit(1)
    def prePopulateTagCache():
        """
        Pre Populate Tagging data into cache
        """
        logging.info("Tag Cache being pre-populated with tags.")
        search_cursor = None
        items = []
        while True:
            response = global_search_service.search(query='tags:*',
                                                    search_cursor=search_cursor,
                                                    fields=["tags"],
                                                    limit=1000)
            scan_result = response.get_result()

            items = items + scan_result["items"]
            if "search_cursor" not in scan_result:
                break
            else:
                search_cursor = scan_result["search_cursor"]

        tag_cache = {}
        for resource in items:
            resourceId = resource["crn"]

            tag_cache[resourceId] = resource["tags"]

        return tag_cache
    def prePopulateResourceCache():
        """
        Retrieve all Resources for account from resource controller and pre-populate cache
        """
        logging.info("Resource_cache being pre-populated with active resources in account.")
        all_results = []
        pager = ResourceInstancesPager(
            client=resource_controller_service,
            limit=50
        )

        try:
            while pager.has_next():
                next_page = pager.get_next()
                assert next_page is not None
                all_results.extend(next_page)
            logging.debug("resource_instance={}".format(all_results))
        except ApiException as e:
            logging.error(
                "API Error.  Can not retrieve instances from controller {}: {}".format(str(e.code), e.message))
            quit(1)

        resource_cache = {}
        for resource in all_results:
            resourceId = resource["crn"]
            resource_cache[resourceId] = resource

        return resource_cache
    def getAccountUsage(start, end):
        """
        Get IBM Cloud Service from account for range of months.
        Note: This usage will bill two months later for SLIC.  For example April Usage, will invoice on the end of June CFTS invoice.
        """
        data = []
        while start <= end:
            usageMonth = start.strftime("%Y-%m")
            logging.info("Retrieving Account Usage from {}.".format(usageMonth))
            start += relativedelta(months=+1)

            try:
                usage = usage_reports_service.get_account_usage(
                    account_id=accountId,
                    billingmonth=usageMonth,
                    names=True
                ).get_result()
            except ApiException as e:
                if e.code == 424:
                    logging.warning("API exception {}.".format(str(e)))
                    continue
                else:
                    logging.error("API exception {}.".format(str(e)))
                    quit(1)

            logging.debug("usage {}={}".format(usageMonth, usage))
            for resource in usage['resources']:
                for plan in resource['plans']:
                    for metric in plan['usage']:
                        row = {
                            'account_id': usage["account_id"],
                            'month': usageMonth,
                            'currency_code': usage['currency_code'],
                            'billing_country': usage['billing_country'],
                            'resource_id': resource['resource_id'],
                            'resource_name': resource['resource_name'],
                            'billable_charges': resource["billable_cost"],
                            'billable_rated_charges': resource["billable_rated_cost"],
                            'plan_id': plan['plan_id'],
                            'plan_name': plan['plan_name'],
                            'metric': metric['metric'],
                            'unit_name': metric['unit_name'],
                            'quantity': float(metric['quantity']),
                            'rateable_quantity': metric['rateable_quantity'],
                            'cost': metric['cost'],
                            'rated_cost': metric['rated_cost'],
                        }

                        if len(metric['discounts']) > 0:
                            row['discount'] = metric['discounts'][0]['discount']
                        else:
                            discount = 0

                        if len(metric['price']) > 0:
                            row['price'] = metric['price']
                        else:
                            row['price'] = "[]"
                        # add row to data
                        data.append(row.copy())

        accountUsage = pd.DataFrame(data,
                                    columns=['account_id', 'month', 'currency_code', 'billing_country', 'resource_id',
                                             'resource_name',
                                             'billable_charges', 'billable_rated_charges', 'plan_id', 'plan_name',
                                             'metric', 'unit_name', 'quantity',
                                             'rateable_quantity', 'cost', 'rated_cost', 'discount', 'price'])

        return accountUsage

    def getInstancesUsage(start, end):
        """
        Get instances resource usage for month of specific resource_id
        """

        def getResourceInstancefromCloud(resourceId):
            """
            Retrieve Resource Details from resource controller
            """
            try:
                resource_instance = resource_controller_service.get_resource_instance(
                    id=resourceId).get_result()
                logging.debug("resource_instance={}".format(resource_instance))
            except ApiException as e:
                resource_instance = {}
                if e.code == 403:
                    logging.warning(
                        "You do not have the required permissions to retrieve the instance {} {}: {}".format(resourceId,
                                                                                                             str(e.code),
                                                                                                             e.message))
                else:
                    logging.warning(
                        "Error: Instance {} {}: {}".format(resourceId, str(e.code), e.message))
            return resource_instance

        def getResourceInstance(resourceId):
            """
            Check Cache for Resource Details which may have been retrieved previously
            """
            if resourceId not in resource_controller_cache:
                logging.debug("Cache miss for Resource {}".format(resourceId))
                resource_controller_cache[resourceId] = getResourceInstancefromCloud(resourceId)
            return resource_controller_cache[resourceId]

        def getTags(resourceId):
            """
            Check Tag Cache for Resource
            """
            if resourceId not in tag_cache:
                logging.debug("Cache miss for Tag {}".format(resourceId))
                tags = []
            else:
                tags = tag_cache[resourceId]
            return tags

        data = []
        limit = 200  ## set limit of record returned

        """ Loop through months """
        while start <= end:
            usageMonth = start.strftime("%Y-%m")
            logging.info("Retrieving Instances Usage from {}.".format(usageMonth))
            start += relativedelta(months=+1)
            try:
                instances_usage = usage_reports_service.get_resource_usage_account(
                    account_id=accountId,
                    billingmonth=usageMonth, names=True, limit=limit).get_result()
            except ApiException as e:
                logging.error("Fatal Error with get_resource_usage_account: {}".format(e))
                quit(1)

            recordstart = 1
            if recordstart + limit > instances_usage["count"]:
                recordstop = instances_usage["count"]
            else:
                recordstop = recordstart + limit - 1
            logging.info(
                "Requesting Instance {} Usage: retrieved from {} to {} of Total {}".format(usageMonth, recordstart,
                                                                                           recordstop,
                                                                                           instances_usage[
                                                                                               "count"]))
            if "next" in instances_usage:
                nextoffset = instances_usage["next"]["offset"]
            else:
                nextoffset = ""

            while True:
                for instance in instances_usage["resources"]:
                    logging.debug("Parsing Details for Instance {}.".format(instance["resource_instance_id"]))

                    if "pricing_country" in instance:
                        pricing_country = instance["pricing_country"]
                    else:
                        pricing_country = ""

                    if "billing_country" in instance:
                        billing_country = instance["billing_country"]
                    else:
                        billing_country = ""

                    if "currency_code" in instance:
                        currency_code = instance["currency_code"]
                    else:
                        currency_code = ""

                    if "pricing_region" in instance:
                        pricing_region = instance["pricing_region"]
                    else:
                        pricing_region = ""

                    if "resource_group_id" in instance:
                        resource_group_id = instance["resource_group_id"]
                    else:
                        resource_group_id = ""

                    if "resource_group_name" in instance:
                        resource_group_name = instance["resource_group_name"]
                    else:
                        resource_group_name = ""

                    row = {
                        "account_id": instance["account_id"],
                        "instance_id": instance["resource_instance_id"],
                        "resource_group_id": resource_group_id,
                        "month": instance["month"],
                        "pricing_country": pricing_country,
                        "billing_country": billing_country,
                        "currency_code": currency_code,
                        "plan_id": instance["plan_id"],
                        "plan_name": instance["plan_name"],
                        "billable": instance["billable"],
                        "pricing_plan_id": instance["pricing_plan_id"],
                        "pricing_region": pricing_region,
                        "region": instance["region"],
                        "service_id": instance["resource_id"],
                        "service_name": instance["resource_name"],
                        "resource_group_name": resource_group_name,
                        "instance_name": instance["resource_instance_name"]
                    }

                    """
                    Get additional resource instance detail
                    """

                    # get instance detail from cache or resource controller
                    resource_instance = getResourceInstance(instance["resource_instance_id"])

                    if "created_at" in resource_instance:
                        created_at = resource_instance["created_at"]
                        """Create Provision Date Field using US East Timezone for Zulu conversion"""
                        provisionDate = pd.to_datetime(created_at, format="%Y-%m-%dT%H:%M:%S.%f")
                        provisionDate = provisionDate.strftime("%Y-%m-%d")
                    else:
                        created_at = ""
                        provisionDate = ""

                    if "updated_at" in resource_instance:
                        updated_at = resource_instance["updated_at"]
                    else:
                        updated_at = ""

                    if "deleted_at" in resource_instance:
                        deleted_at = resource_instance["deleted_at"]
                        """Create deProvision Date Field using US East Timezone for Zulu conversion"""
                        if deleted_at != None:
                            deprovisionDate = pd.to_datetime(deleted_at, format="%Y-%m-%dT%H:%M:%S.%f")
                            deprovisionDate = deprovisionDate.strftime("%Y-%m-%d")
                        else:
                            deprovisionDate = ""
                    else:
                        deleted_at = ""
                        deleted_by = ""
                        deprovisionDate = ""

                    if "created_by" in resource_instance:
                        created_by = resource_instance["created_by"]
                    else:
                        created_by = ""

                    if "deleted_by" in resource_instance:
                        deleted_by = resource_instance["deleted_by"]
                    else:
                        deleted_by = ""

                    if "updated_by" in resource_instance:
                        updated_by = resource_instance["updated_by"]
                    else:
                        updated_by = ""

                    if "restored_at" in resource_instance:
                        restored_at = resource_instance["restored_at"]
                    else:
                        restored_at = ""

                    if "restored_by" in resource_instance:
                        restored_by = resource_instance["restored_by"]
                    else:
                        restored_by = ""

                    if "state" in resource_instance:
                        state = resource_instance["state"]
                    else:
                        state = ""

                    if "type" in resource_instance:
                        type = resource_instance["type"]
                    else:
                        type = ""

                    """
                    For  Servers obtain intended profile and virtual or baremetal server details
                    """
                    az = ""
                    LifecycleAction = ""
                    profile = ""
                    cpuFamily = ""
                    numberOfVirtualCPUs = ""
                    NumberOfGPUs = ""
                    NumberOfInstStorageDisks = ""
                    MemorySizeMiB = ""
                    NodeName = ""
                    NumberofCores = ""
                    NumberofSockets = ""
                    Bandwidth = ""
                    OSName = ""
                    OSVendor = ""
                    OSVersion = ""
                    Capacity = ""
                    IOPS = ""

                    if "extensions" in resource_instance:
                        if "VirtualMachineProperties" in resource_instance["extensions"]:
                            profile = resource_instance["extensions"]["VirtualMachineProperties"]["Profile"]
                            cpuFamily = resource_instance["extensions"]["VirtualMachineProperties"]["CPUFamily"]
                            numberOfVirtualCPUs = resource_instance["extensions"]["VirtualMachineProperties"][
                                "NumberOfVirtualCPUs"]
                            MemorySizeMiB = resource_instance["extensions"]["VirtualMachineProperties"]["MemorySizeMiB"]
                            NodeName = resource_instance["extensions"]["VirtualMachineProperties"]["NodeName"]
                            NumberOfGPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfGPUs"]
                            NumberOfInstStorageDisks = resource_instance["extensions"]["VirtualMachineProperties"][
                                "NumberOfInstStorageDisks"]

                        elif "BMServerProperties" in resource_instance["extensions"]:
                            profile = resource_instance["extensions"]["BMServerProperties"]["Profile"]
                            MemorySizeMiB = resource_instance["extensions"]["BMServerProperties"]["MemorySizeMiB"]
                            NodeName = resource_instance["extensions"]["BMServerProperties"]["NodeName"]
                            NumberofCores = resource_instance["extensions"]["BMServerProperties"]["NumberOfCores"]
                            NumberofSockets = resource_instance["extensions"]["BMServerProperties"]["NumberOfSockets"]
                            Bandwidth = resource_instance["extensions"]["BMServerProperties"]["Bandwidth"]
                            OSName = resource_instance["extensions"]["BMServerProperties"]["OSName"]
                            OSVendor = resource_instance["extensions"]["BMServerProperties"]["OSVendor"]
                            OSVersion = resource_instance["extensions"]["BMServerProperties"]["OSVersion"]

                        elif "VolumeInfo" in resource_instance["extensions"]:
                            Capacity = resource_instance["extensions"]["VolumeInfo"]["Capacity"]
                            IOPS = resource_instance["extensions"]["VolumeInfo"]["IOPS"]

                        if "Resource" in resource_instance["extensions"]:
                            if "AvailabilityZone" in resource_instance["extensions"]["Resource"]:
                                az = resource_instance["extensions"]["Resource"]["AvailabilityZone"]
                            if "Location" in resource_instance["extensions"]["Resource"]:
                                region = resource_instance["extensions"]["Resource"]["Location"]["Region"]
                            if "LifecycleAction" in resource_instance["extensions"]["Resource"]:
                                LifecycleAction = resource_instance["extensions"]["Resource"]["LifecycleAction"]

                    # get tags attached to instance from cache or resource controller
                    tags = getTags(instance["resource_instance_id"])

                    # parse role tag into comma delimited list
                    if len(tags) > 0:
                        role = ",".join([str(item.split(":")[1]) for item in tags if "role:" in item])
                    else:
                        role = ""

                    row_addition = {
                        "provision_date": provisionDate,
                        "created_at": created_at,
                        "created_by": created_by,
                        "updated_at": updated_at,
                        "updated_by": updated_by,
                        "deprovision_date": deprovisionDate,
                        "deleted_at": deleted_at,
                        "deleted_by": deleted_by,
                        "restored_at": restored_at,
                        "restored_by": restored_by,
                        "instance_state": state,
                        "type": type,
                        "instance_profile": profile,
                        "cpu_family": cpuFamily,
                        "numberOfVirtualCPUs": numberOfVirtualCPUs,
                        "MemorySizeMiB": MemorySizeMiB,
                        "NodeName": NodeName,
                        "NumberOfGPUs": NumberOfGPUs,
                        "NumberOfInstStorageDisks": NumberOfInstStorageDisks,
                        "lifecycleAction": LifecycleAction,
                        "BMnumberofCores": NumberofCores,
                        "BMnumberofSockets": NumberofSockets,
                        "BMbandwidth": Bandwidth,
                        "OSName": OSName,
                        "OSVendor": OSVendor,
                        "OSVersion": OSVersion,
                        "capacity": Capacity,
                        "iops": IOPS,
                        "tags": tags,
                        "instance_role": role,
                        "availability_zone": az
                    }

                    # combine original row with additions
                    row = row | row_addition

                    for usage in instance["usage"]:
                        metric = usage["metric"]
                        unit = usage["unit"]
                        quantity = float(usage["quantity"])
                        cost = usage["cost"]
                        rated_cost = usage["rated_cost"]
                        rateable_quantity = float(usage["rateable_quantity"])
                        price = usage["price"]
                        discount = usage["discounts"]
                        metric_name = usage["metric_name"]
                        unit_name = usage["unit_name"]

                        row_addition = {
                            "metric": metric,
                            "unit": unit,
                            "quantity": quantity,
                            "cost": cost,
                            "rated_cost": rated_cost,
                            "rateable_quantity": rateable_quantity,
                            "price": price,
                            "discount": discount,
                            "metric_name": metric_name,
                            'unit_name': unit_name,
                        }

                        row = row | row_addition

                        data.append(row.copy())

                if nextoffset != "":
                    recordstart = recordstart + limit
                    if recordstart + limit > instances_usage["count"]:
                        recordstop = instances_usage["count"]
                    else:
                        recordstop = recordstart + limit - 1
                    logging.info("Requesting Instance {} Usage: retrieving from {} to {} of Total {}".format(usageMonth,
                                                                                                             recordstart,
                                                                                                             recordstop,
                                                                                                             instances_usage[
                                                                                                                 "count"]))
                    try:
                        instances_usage = usage_reports_service.get_resource_usage_account(
                            account_id=accountId,
                            billingmonth=usageMonth, names=True, limit=limit, start=nextoffset).get_result()
                    except ApiException as e:
                        logging.error("Error with get_resource_usage_account: {}".format(e))
                        quit(1)

                    if "next" in instances_usage:
                        nextoffset = instances_usage["next"]["offset"]
                    else:
                        nextoffset = ""
                else:
                    break
            """ created Datatable from List """
            instancesUsage = pd.DataFrame(data, columns=list(data[0].keys()))
        return instancesUsage
    def createChargesbyServer(servers, month):
        """
        Create Pivot by Server for current month (consolidate metrics)
        """

        """ Query only virtual CPU,  VCPU metric and last month so it calculates current total VCPU """

        logging.info("Calculating total charges per server.")
        servers = servers.query('month == @month')
        vcpu = pd.pivot_table(servers, index=["region", "service_name", "instance_role", "instance_name", "instance_id",
                                              "instance_profile"],
                              values=["rated_cost", "cost"],
                              aggfunc={"rated_cost": np.sum, "cost": np.sum},
                              margins=True, margins_name="Total",
                              fill_value=0)

        new_order = ["rated_cost", "cost"]
        vcpu = vcpu.reindex(new_order, axis=1)
        vcpu.to_excel(writer, '{}_VPC_Servers'.format(month))
        worksheet = writer.sheets['{}_VPC_Servers'.format(month)]
        format2 = workbook.add_format({'align': 'left'})
        format3 = workbook.add_format({'num_format': '#,##0'})
        format4 = workbook.add_format({'num_format': '$#,##0.00'})
        worksheet.set_column("A:A", 15, format2)
        worksheet.set_column("B:B", 25, format2)
        worksheet.set_column("C:C", 15, format2)
        worksheet.set_column("D:E", 120, format2)
        worksheet.set_column("F:F", 20, format2)
        worksheet.set_column("G:H", 18, format4)
        return
    def createChargesbyVolume(volumes, month):
        """
        Create BM VCPU deployed by role, account, and az
        """

        logging.info("Calculating Block Volumes deployed.")
        """ Query """
        volumes = volumes.query('metric == "GIGABYTE_HOURS" and month == @month')
        volumes = pd.pivot_table(volumes, index=["region", "service_name", "resource_group_name", "instance_name", "instance_id", "capacity", "iops"],
                                 values=["cost", "rated_cost"],
                                 aggfunc={"cost": np.sum, "rated_cost": np.sum},
                                 margins=True, margins_name="Total",
                                 fill_value=0)

        new_order = ["rated_cost", "cost"]
        volumes = volumes.reindex(new_order, axis=1)
        volumes.to_excel(writer, '{}_VPC_Volumes'.format(month))
        worksheet = writer.sheets['{}_VPC_Volumes'.format(month)]
        format2 = workbook.add_format({'align': 'left'})
        format3 = workbook.add_format({'num_format': '#,##0'})
        format4 = workbook.add_format({'num_format': '$#,##0.00'})
        worksheet.set_column("A:A", 15, format2)
        worksheet.set_column("B:C", 30, format2)
        worksheet.set_column("D:D", 40, format2)
        worksheet.set_column("E:E", 120, format2)
        worksheet.set_column("F:F", 18, format2)
        worksheet.set_column("G:I", 18, format2)
        worksheet.set_column("H:I", 18, format4)
        return
    def createUsageSummaryTab(paasUsage):
        logging.info("Creating Usage Summary tab.")
        usageSummary = pd.pivot_table(paasUsage, index=["resource_name"],
                                      columns=["month"],
                                      values=["rated_cost", "cost"],
                                      aggfunc=np.sum, margins=True, margins_name="Total",
                                      fill_value=0)
        new_order = ["rated_cost", "cost"]
        usageSummary = usageSummary.reindex(new_order, axis=1, level=0)
        usageSummary.to_excel(writer, 'Cloud_Usage')
        worksheet = writer.sheets['Cloud_Usage']
        format1 = workbook.add_format({'num_format': '$#,##0.00'})
        format2 = workbook.add_format({'align': 'left'})
        worksheet.set_column("A:A", 35, format2)
        worksheet.set_column("B:ZZ", 18, format1)

    """ Enable CloudSDK """
    createSDK(args.IC_API_KEY)
    accountId = getAccountId(args.IC_API_KEY)

    logging.info("Retrieving Instance data from AccountId: {}.".format(accountId))

    """
    Pre-populate Account Data to accelerate report generation
    """
    tag_cache = prePopulateTagCache()
    resource_controller_cache = prePopulateResourceCache()

    """
    Gather both account and instance level usage using Cloud SDK
    """
    bssStartDate = startdate - relativedelta(months=1)
    bssEndDate = enddate - relativedelta(months=2)
    accountUsage = getAccountUsage(bssStartDate, bssEndDate)
    instancesUsage = getInstancesUsage(bssStartDate, bssEndDate)

    createUsageSummaryTab(accountUsage)
    months = instancesUsage.month.unique()
    for i in months:
        """
        Create VPC Related Tabs
        """
        servers = instancesUsage.query('(service_id == "is.instance" or service_id == "is.bare-metal-server") and month == @i')
        createChargesbyServer(servers, i)
        storage = instancesUsage.query('service_id == "is.volume" and month == @i')
        createChargesbyVolume(storage, i)
    return


if __name__ == "__main__":
    setup_logging()
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Export usage detail by invoice month to an Excel file for all IBM Cloud Classic invoices and corresponding PaaS Consumption.")
    parser.add_argument("-k", default=os.environ.get('IC_API_KEY', None), dest="IC_API_KEY", help="IBM Cloud API Key")
    parser.add_argument("-u", "--username", default=os.environ.get('ims_username', None), metavar="username", help="IBM IMS Userid")
    parser.add_argument("-p", "--password", default=os.environ.get('ims_password', None), metavar="password", help="IBM IMS Password")
    parser.add_argument("-a", "--account", default=os.environ.get('ims_account', None), metavar="account", help="IMS Account")
    parser.add_argument("-s", "--startdate", default=os.environ.get('startdate', None), help="Start Year & Month in format YYYY-MM")
    parser.add_argument("-e", "--enddate", default=os.environ.get('enddate', None), help="End Year & Month in format YYYY-MM")
    parser.add_argument("--debug", action=argparse.BooleanOptionalAction, help="Set Debug level for logging.")
    parser.add_argument("--load", action=argparse.BooleanOptionalAction, help="Load dataframes from pkl files for test purposes.")
    parser.add_argument("--save", action=argparse.BooleanOptionalAction, help="Store dataframes to pkl files for test purposes.")
    parser.add_argument("--months", default=os.environ.get('months', 1), help="Number of months including last full month to include in report.")
    parser.add_argument("--COS_APIKEY", default=os.environ.get('COS_APIKEY', None), help="COS apikey to use for Object Storage.")
    parser.add_argument("--COS_ENDPOINT", default=os.environ.get('COS_ENDPOINT', None), help="COS endpoint to use for Object Storage.")
    parser.add_argument("--COS_INSTANCE_CRN", default=os.environ.get('COS_INSTANCE_CRN', None), help="COS Instance CRN to use for file upload.")
    parser.add_argument("--COS_BUCKET", default=os.environ.get('COS_BUCKET', None), help="COS Bucket name to use for file upload.")
    parser.add_argument("--sendGridApi", default=os.environ.get('sendGridApi', None), help="SendGrid ApiKey used to email output.")
    parser.add_argument("--sendGridTo", default=os.environ.get('sendGridTo', None), help="SendGrid comma deliminated list of emails to send output to.")
    parser.add_argument("--sendGridFrom", default=os.environ.get('sendGridFrom', None), help="Sendgrid from email to send output from.")
    parser.add_argument("--sendGridSubject", default=os.environ.get('sendGridSubject', None), help="SendGrid email subject for output email")
    parser.add_argument("--output", default=os.environ.get('output', 'invoice-analysis.xlsx'), help="Filename Excel output file. (including extension of .xlsx)")
    parser.add_argument("--SL_PRIVATE", default=False, action=argparse.BooleanOptionalAction, help="Use IBM Cloud Classic Private API Endpoint")
    parser.add_argument('--storage', default=False, action=argparse.BooleanOptionalAction, help="Include File, BLock and Classic Cloud Object Storage detail analysis.")
    parser.add_argument('--detail', default=True, action=argparse.BooleanOptionalAction, help="Whether to Write detail tabs to worksheet.")
    parser.add_argument('--users', default=False, action=argparse.BooleanOptionalAction, help="Include user detail.")
    parser.add_argument('--accountdetail', default=False, action=argparse.BooleanOptionalAction, help="Include account detail.")
    parser.add_argument('--summary', default=True, action=argparse.BooleanOptionalAction, help="Whether to Write summary tabs to worksheet.")
    parser.add_argument('--reconciliation', default=False, action=argparse.BooleanOptionalAction, help="Whether to write invoice reconciliation tabs to worksheet.")
    parser.add_argument('--serverdetail', default=False, action=argparse.BooleanOptionalAction, help="Whether to write server detail tabs to worksheet.")
    parser.add_argument('--cosdetail', default=False, action=argparse.BooleanOptionalAction, help="Whether to write Classic Object Storage tab to worksheet.")
    parser.add_argument('--bss', default=False, action=argparse.BooleanOptionalAction, help="Retreive BSS usage for corresponding months using ibmCloudUsage.py.")

    args = parser.parse_args()
    if args.debug:
        log = logging.getLogger()
        log.handlers[0].setLevel(logging.DEBUG)
        log.handlers[1].setLevel(logging.DEBUG)

    logging.info("Creating detail dPart table for report use.")
    dpartDescriptions = yaml.load(open('dpart-descriptions.yaml', 'r'), Loader=Loader)

    """Set Flags to determine which Tabs are created in output"""
    storageFlag = args.storage
    detailFlag = args.detail
    summaryFlag = args.summary
    reconciliationFlag = args.reconciliation
    serverDetailFlag = args.serverdetail
    cosdetailFlag = args.cosdetail
    bssFlag = args.bss
    userFlag = args.users
    accountFlag = args.accountdetail

    if args.startdate == None or args.enddate == None:
        months = int(args.months)
        dallas = tz.gettz('US/Central')
        today = datetime.today().astimezone(dallas)
        if today.day > 19:
            enddate = today.strftime('%Y-%m')
            startdate = today - relativedelta(months=months - 1)
            startdate = startdate.strftime("%Y-%m")
        else:
            enddate = today - relativedelta(months=1)
            enddate = enddate.strftime('%Y-%m')
            startdate = today - relativedelta(months=(months))
            startdate = startdate.strftime("%Y-%m")
    else:
        startdate = args.startdate
        enddate = args.enddate


    """
    If no APIKEY set, then check for internal IBM credentials
    NOTE: internal authentication requires internal SDK version & Global Protect VPN.
    """
    if args.load == True:
        logging.info( "Loading usage data from classicUsage.pkl file.")
        classicUsage = pd.read_pickle("classicUsage.pkl")
    else:
        if args.IC_API_KEY == None:
            if args.username == None or args.password == None or args.account == None:
                logging.error("You must provide either IBM Cloud ApiKey or Internal Employee credentials & IMS account.")
                quit(1)
            else:
                if args.username != None or args.password != None:
                    logging.info("Using Internal endpoint and employee credentials.")
                    ims_username = args.username
                    ims_password = args.password
                    if args.account == None:
                        ims_account = input("IMS Account:")
                    else:
                        ims_account = args.account
                    ims_yubikey = input("Yubi Key:")
                    SL_ENDPOINT = "http://internal.applb.softlayer.local/v3.1/internal/xmlrpc"
                    client = createEmployeeClient(SL_ENDPOINT, ims_username, ims_password, ims_yubikey)
                else:
                    logging.error("Error!  Can't find internal credentials or ims account.")
                    quit(1)
        else:
            logging.info("Using IBM Cloud Account API Key.")
            IC_API_KEY = args.IC_API_KEY
            ims_account = None

            # Change endpoint to private Endpoint if command line open chosen
            if args.SL_PRIVATE:
                SL_ENDPOINT = "https://api.service.softlayer.com/xmlrpc/v3.1"
            else:
                SL_ENDPOINT = "https://api.softlayer.com/xmlrpc/v3.1"

                # Create Classic infra API client
                client = SoftLayer.Client(username="apikey", api_key=IC_API_KEY, endpoint_url=SL_ENDPOINT)


        """
        Retrieve Existing Account Users and Network Storage if requested by flag
        """
        if accountFlag:
            accountDetail = getAccountDetail()

        if userFlag:
            userList = getUsers()

        if storageFlag:
            networkStorageDF = getAccountNetworkStorage()

        # Calculate invoice dates based on SLIC invoice cutoffs.
        startdate, enddate = getInvoiceDates(startdate, enddate)

        #  Retrieve Invoices from classic
        classicUsage = getInvoiceDetail(startdate, enddate)

    """"
    Build Exel Report Report with Charges
    """
    createReport(args.output, classicUsage)

    if args.sendGridApi != None:
        sendEmail(startdate, enddate, args.sendGridTo, args.sendGridFrom, args.sendGridSubject, args.sendGridApi, args.output)

    # upload created file to COS if COS credentials provided
    if args.COS_APIKEY != None:
        cos = ibm_boto3.resource("s3",
                                 ibm_api_key_id=args.COS_APIKEY,
                                 ibm_service_instance_id=args.COS_INSTANCE_CRN,
                                 config=Config(signature_version="oauth"),
                                 endpoint_url=args.COS_ENDPOINT
                                 )
        multi_part_upload(args.COS_BUCKET, args.output, "./" + args.output)

    logging.info("invoiceAnalysis complete.")