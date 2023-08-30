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
import os, logging, logging.config, os.path, argparse, base64
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import *
from dateutil import tz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Personalization, Email, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId)
import ibm_boto3
from ibm_botocore.client import Config, ClientError
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalTaggingV1, GlobalSearchV2
from ibm_platform_services.resource_controller_v2 import *
from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from dotenv import load_dotenv


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
        logging.error("API Error.  Can not retrieve instances from controller {}: {}".format(str(e.code),e.message))
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


    accountUsage = pd.DataFrame(data, columns=['account_id', 'month', 'currency_code', 'billing_country', 'resource_id', 'resource_name',
                    'billable_charges', 'billable_rated_charges', 'plan_id', 'plan_name', 'metric', 'unit_name', 'quantity',
                    'rateable_quantity','cost', 'rated_cost', 'discount', 'price'])

    return accountUsage
def getInstancesUsage(start,end):
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
                    "You do not have the required permissions to retrieve the instance {} {}: {}".format(resourceId, str(e.code), e.message))
            else:
                logging.warning(
                    "Error: Instance {} {}: {}".format(resourceId, str(e.code),e.message))
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
        logging.info("Requesting Instance {} Usage: retrieved from {} to {} of Total {}".format(usageMonth, recordstart,
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

                row = {
                    "account_id": instance["account_id"],
                    "instance_id": instance["resource_instance_id"],
                    "resource_group_id": instance["resource_group_id"],
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
                    "resource_group_name": instance["resource_group_name"],
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
                        numberOfVirtualCPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfVirtualCPUs"]
                        MemorySizeMiB = resource_instance["extensions"]["VirtualMachineProperties"]["MemorySizeMiB"]
                        NodeName = resource_instance["extensions"]["VirtualMachineProperties"]["NodeName"]
                        NumberOfGPUs = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfGPUs"]
                        NumberOfInstStorageDisks = resource_instance["extensions"]["VirtualMachineProperties"]["NumberOfInstStorageDisks"]

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
                    "MemorySizeMiB":  MemorySizeMiB,
                    "NodeName":  NodeName,
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
                logging.info("Requesting Instance {} Usage: retrieving from {} to {} of Total {}".format(usageMonth, recordstart,
                                                                                              recordstop,
                                                                                              instances_usage["count"]))
                try:
                    instances_usage = usage_reports_service.get_resource_usage_account(
                        account_id=accountId,
                        billingmonth=usageMonth, names=True,limit=limit, start=nextoffset).get_result()
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
def createServiceDetail(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating ServiceUsageDetail tab.")

    paasUsage.to_excel(writer, "ServiceUsageDetail")
    worksheet = writer.sheets['ServiceUsageDetail']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:C", 12, format2)
    worksheet.set_column("D:E", 25, format2)
    worksheet.set_column("F:G", 18, format1)
    worksheet.set_column("H:I", 25, format2)
    worksheet.set_column("J:J", 18, format1)
    totalrows,totalcols=paasUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
    return
def createInstancesDetailTab(instancesUsage):
    """
    Write detail tab to excel
    """
    logging.info("Creating instances detail tab.")

    instancesUsage.to_excel(writer, "Instances_Detail")
    worksheet = writer.sheets['Instances_Detail']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:C", 12, format2)
    worksheet.set_column("D:E", 25, format2)
    worksheet.set_column("F:G", 18, format1)
    worksheet.set_column("H:I", 25, format2)
    worksheet.set_column("J:J", 18, format1)
    totalrows,totalcols=instancesUsage.shape
    worksheet.autofilter(0,0,totalrows,totalcols)
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
    usageSummary.to_excel(writer, 'Usage_Summary')
    worksheet = writer.sheets['Usage_Summary']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:A", 35, format2)
    worksheet.set_column("B:ZZ", 18, format1)
def createMetricSummary(paasUsage):
    logging.info("Creating Metric Plan Summary tab.")
    metricSummaryPlan = pd.pivot_table(paasUsage, index=["resource_name", "plan_name", "metric"],
                                 columns=["month"],
                                 values=["quantity", "cost"],
                                 aggfunc=np.sum, margins=True, margins_name="Total",
                                 fill_value=0)
    new_order = ["quantity", "cost"]
    metricSummaryPlan = metricSummaryPlan.reindex(new_order, axis=1, level=0)
    metricSummaryPlan.to_excel(writer, 'MetricPlanSummary')
    worksheet = writer.sheets['MetricPlanSummary']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0.00000'})
    worksheet.set_column("A:A", 30, format2)
    worksheet.set_column("B:B", 40, format2)
    worksheet.set_column("C:C", 40, format2)
    months = len(paasUsage.month.unique())
    worksheet.set_column(3, 3 + months, 18, format3)
    worksheet.set_column(4 + months, 4 + (months * 2), 18, format1)
    return
def createChargesbyServer(servers, month):
    """
    Create Pivot by Server for current month (consolidate metrics)
    """

    """ Query only virtual CPU,  VCPU metric and last month so it calculates current total VCPU """

    logging.info("Calculating total charges per server (virtual and BareMetal).")
    servers = servers.query('month == @month')
    vcpu = pd.pivot_table(servers, index=["region", "service_name", "instance_role", "instance_name", "instance_id", "instance_profile"],
                                    values=["rated_cost", "cost"],
                                    aggfunc={"rated_cost": np.sum, "cost": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0)

    new_order = ["rated_cost", "cost"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, '{}_VPC_Server_List'.format(month))
    worksheet = writer.sheets['{}_VPC_Server_List'.format(month)]
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

    logging.info("Calculating Block Volume charges.")

    volumes = volumes.query('month == @month')
    volumes = pd.pivot_table(volumes, index=["region", "service_name", "resource_group_name", "instance_name", "instance_id", "capacity", "iops"],
                             values=["cost", "rated_cost"],
                             aggfunc={"cost": np.sum, "rated_cost": np.sum},
                             margins=True, margins_name="Total",
                             fill_value=0)

    new_order = ["rated_cost", "cost"]
    volumes = volumes.reindex(new_order, axis=1)

    volumes.to_excel(writer, '{}_VPC_Volume_List'.format(month))
    worksheet = writer.sheets['{}_VPC_Volume_List'.format(month)]
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
def createChargesCOSInstance(cos, month):
    """
    Create Table of COS INstances with Charge metrics
    """

    logging.info("Calculating COS Instance charges.")

    cos = cos.query('month == @month')
    cos = pd.pivot_table(cos, index=["region", "resource_group_name", "instance_name", "plan_name", "metric", "unit", "quantity"],
                             values=["cost", "rated_cost"],
                             aggfunc={"cost": np.sum, "rated_cost": np.sum},
                             margins=True, margins_name="Total",
                             fill_value=0)

    new_order = ["rated_cost", "cost"]
    cos = cos.reindex(new_order, axis=1)

    cos.to_excel(writer, '{}_COS_List'.format(month))
    worksheet = writer.sheets['{}_COS_List'.format(month)]
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:A", 15, format2)
    worksheet.set_column("B:D", 30, format2)
    worksheet.set_column("E:E", 40, format2)
    worksheet.set_column("F:F", 18, format2)
    worksheet.set_column("G:G", 18, format3)
    worksheet.set_column("H:I", 18, format4)
    return

def createVirtualServerTab(servers, month):
    """
    Create Pivot by Original Provision Date for current servers
    """

    """ Query only virtual CPU,  VCPU metric and last month so it calculates current total VCPU """
    servers = servers.query('service_id == "is.instance" and metric == "VCPU_HOURS" and month == @month')

    logging.info("Calculating current Virtual Server vCPU by provision date.")

    vcpu = pd.pivot_table(servers, index=["region", "availability_zone", "resource_group_name", "instance_role", "instance_profile", "provision_date", "deprovision_date"],
                                    values=["instance_id", "numberOfVirtualCPUs"],
                                    aggfunc={"instance_id": "nunique", "numberOfVirtualCPUs": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count", "numberOfVirtualCPUs"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, '{}_VPC_VirtualCoreSummary'.format(month))
    worksheet = writer.sheets['{}_VPC_VirtualCoreSummary'.format(month)]
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:B", 15, format2)
    worksheet.set_column("C:E", 30, format2)
    worksheet.set_column("F:G", 18, format2)
    worksheet.set_column("H:I", 18, format3)
    return
def createBMServerTab(servers, month):
    """
    Create BM VCPU deployed by role, account, and az
    """

    logging.info("Calculating Bare Metal CPU deployed.")
    """ Query CPU,  VCPU metric and last month so it calculates current total VCPU """
    servers = servers.query('service_id == "is.bare-metal-server" and metric == "BARE_METAL_SERVER_HOURS" and month == @month')
    vcpu = pd.pivot_table(servers, index=["region", "availability_zone", "resource_group_name", "instance_role", "instance_profile", "provision_date", "deprovision_date"],
                                    values=["instance_id", "BMnumberofCores", "BMnumberofSockets"],
                                    aggfunc={"instance_id": "nunique", "BMnumberofCores": np.sum, "BMnumberofSockets": np.sum},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', "BMnumberofCores": "Cores", "BMnumberofSockets": "Sockets"})

    new_order = ["instance_count", "Cores", "Sockets"]
    vcpu = vcpu.reindex(new_order, axis=1)
    vcpu.to_excel(writer, '{}_VPC_BM_CoreSummary'.format(month))
    worksheet = writer.sheets['{}_VPC_BM_CoreSummary'.format(month)]
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:B", 15, format2)
    worksheet.set_column("C:E", 30, format2)
    worksheet.set_column("F:G", 18, format2)
    worksheet.set_column("H:J", 18, format3)
    return

    def createChargesbyVolume(volumes, month):
        """
        Create BM VCPU deployed by role  tag
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
def createVolumeSummary(volumes, month):
    """
    Create BM VCPU deployed by role, account, and az
    """

    logging.info("Calculating Block Volume Summary.")
    """ Query """
    volumes = volumes.query('metric == "GIGABYTE_HOURS" and month == @month')
    volumes = pd.pivot_table(volumes, index=["region", "availability_zone", "resource_group_name", "instance_role", "provision_date", "deprovision_date"],
                                    values=["instance_id", "capacity", "iops", "cost", "rated_cost"],
                                    aggfunc={"instance_id": "nunique", "capacity": np.sum, "iops": np.sum, "cost": np.sum, "rated_cost": np.sum},
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count", "capacity", "iops", "rated_cost", "cost"]
    volumes = volumes.reindex(new_order, axis=1)
    volumes.to_excel(writer, '{}_VPC_Volumes_Summary'.format(month))
    worksheet = writer.sheets['{}_VPC_Volumes_Summary'.format(month)]
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:B", 15, format2)
    worksheet.set_column("C:E", 30, format2)
    worksheet.set_column("F:F", 18, format2)
    worksheet.set_column("G:I", 18, format3)
    worksheet.set_column("J:K", 18, format4)
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
        quit(1)
    except Exception as e:
        logging.error("Unable to complete multi-part upload: {0}".format(e))
        quit(1)
    return
def sendEmail(startdate, enddate, sendGridTo, sendGridFrom, sendGridSubject, sendGridApi, outputname):
    """
    Semd a file via SendGrid mail service
    :param startdate: Start month of report
    :param enddate:  End month of report
    :param sendGridTo: Email distribution list
    :param sendGridFrom:  Email for from
    :param sendGridSubject: Subject of email
    :param sendGridApi: apikey to use for SendGrid account
    :param outputname: file to send.
    :return:
    """

    html = ("<p><b>IBM Cloud Usage Output Attached for months {} to {} </b></br></p>".format(datetime.strftime(startdate, "%Y-%m"), datetime.strftime(enddate, "%Y-%m")))

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
        quit(1)
    return

if __name__ == "__main__":
    setup_logging()
    load_dotenv()
    parser = argparse.ArgumentParser(description="Calculate IBM Cloud Usage.")
    parser.add_argument("--apikey", default=os.environ.get('IC_API_KEY', None), metavar="apikey", help="IBM Cloud API Key")
    parser.add_argument("--output", default=os.environ.get('output', 'ibmCloudUsage.xlsx'), help="Filename Excel output file. (including extension of .xlsx)")
    parser.add_argument("--load", action=argparse.BooleanOptionalAction, help="load dataframes from pkl files for testing purposes.")
    parser.add_argument("--save", action=argparse.BooleanOptionalAction, help="Store dataframes to pkl files for testing purposes.")
    parser.add_argument("--months", default=os.environ.get('months', 1), help="Number of months including current month to include in report.")
    parser.add_argument("--vpc", action=argparse.BooleanOptionalAction, help="Include additional VPC analysis tabs.")
    parser.add_argument("--cosinstances", action=argparse.BooleanOptionalAction, help="Include additional COS Instance Detail.")
    parser.add_argument("-s", "--startdate", default=os.environ.get('startdate', None), help="Start Year & Month in format YYYY-MM")
    parser.add_argument("-e", "--enddate", default=os.environ.get('enddate', None), help="End Year & Month in format YYYY-MM")
    parser.add_argument("--cos", "--COS", action=argparse.BooleanOptionalAction, help="Write output to COS bucket destination specified.")
    parser.add_argument("--COS_APIKEY", default=os.environ.get('COS_APIKEY', None), help="COS apikey to use for Object Storage.")
    parser.add_argument("--COS_ENDPOINT", default=os.environ.get('COS_ENDPOINT', None), help="COS endpoint to use for Object Storage.")
    parser.add_argument("--COS_INSTANCE_CRN", default=os.environ.get('COS_INSTANCE_CRN', None), help="COS Instance CRN to use for file upload.")
    parser.add_argument("--COS_BUCKET", default=os.environ.get('COS_BUCKET', None), help="COS Bucket name to use for file upload.")
    parser.add_argument("--sendgrid", action=argparse.BooleanOptionalAction, help="Send file via sendGrid.")
    parser.add_argument("--sendGridApi", default=os.environ.get('sendGridApi', None), help="SendGrid ApiKey used to email output.")
    parser.add_argument("--sendGridTo", default=os.environ.get('sendGridTo', None), help="SendGrid comma deliminated list of emails to send output to.")
    parser.add_argument("--sendGridFrom", default=os.environ.get('sendGridFrom', None), help="Sendgrid from email to send output from.")
    parser.add_argument("--sendGridSubject", default=os.environ.get('sendGridSubject', None), help="SendGrid email subject for output email")
    args = parser.parse_args()

    """
    Parse Date Parameters
    For IBM Cloud Usage using actual Months - If current month included month to date usage included.
    """
    
    if args.startdate == None or args.enddate == None:
        months = int(args.months)
        dallas = tz.gettz('US/Central')
        enddate = datetime.today().astimezone(dallas)
        startdate = enddate - relativedelta(months=months - 1)
    else:
        startdate = datetime.strptime(args.startdate, "%Y-%m")
        enddate = datetime.strptime(args.enddate, "%Y-%m")

    if args.load:
        logging.info("Retrieving Usage and Instance data stored data")
        accountUsage = pd.read_pickle("accountUsage.pkl")
        instancesUsage = pd.read_pickle("instanceUsage.pkl")
    else:
        if args.apikey == None:
            logging.error("You must provide IBM Cloud ApiKey with view access to usage reporting.")
            quit(1)
        else:
            apikey = args.apikey
            instancesUsage = pd.DataFrame()
            accountUsage = pd.DataFrame()
            createSDK(apikey)
            accountId = getAccountId(apikey)
            logging.info("Retrieving Usage and Instance data from AccountId: {}.".format(accountId))
            """
            Pre-populate Account Data to accelerate report generation
            """
            tag_cache = prePopulateTagCache()
            resource_controller_cache = prePopulateResourceCache()

            logging.info("Retrieving Usage and Instance data from AccountId: {}.".format(accountId))

            # Get Usage Data via API
            accountUsage = pd.concat([accountUsage, getAccountUsage(startdate, enddate)])
            instancesUsage = pd.concat([instancesUsage, getInstancesUsage(startdate, enddate)])

            if args.save:
                accountUsage.to_pickle("accountUsage.pkl")
                instancesUsage.to_pickle("instanceUsage.pkl")


    """
    Write Dataframe to Excel Tabs (sheets)
    """
    writer = pd.ExcelWriter(args.output, engine='xlsxwriter')
    workbook = writer.book
    createServiceDetail(accountUsage)
    createInstancesDetailTab(instancesUsage)
    createUsageSummaryTab(accountUsage)
    createMetricSummary(accountUsage)
    if args.cosinstances:
        """
        Create COS Detail tab
        """
        cos = instancesUsage.query('service_name == "Cloud Object Storage"')
        months = instancesUsage.month.unique()
        for i in months:
            createChargesCOSInstance(cos,i)

    if args.vpc:
        """
        Create VPC Server Tabs
        """
        servers = instancesUsage.query('service_id == "is.instance" or service_id == "is.bare-metal-server"')
        storage = instancesUsage.query('service_id == "is.volume"')

        months = instancesUsage.month.unique()

        for i in months:
            """ create VPC Virtual Server & BM Server detail"""
            createChargesbyServer(servers,i)
            createVirtualServerTab(servers, i)
            createBMServerTab(servers, i)
            """ Create VPC Volume Tabs"""
            createChargesbyVolume(storage, i)
            createVolumeSummary(storage, i)

    writer.close()
    """
    If SendGrid specified send email with generated file to email distribution list specified
    """
    if args.sendgrid:
        sendEmail(startdate, enddate, args.sendGridTo, args.sendGridFrom, args.sendGridSubject, args.sendGridApi, args.output)

    """
    If Cloud Object Storage specified copy generated file to bucket.
    """
    if args.cos:
        cos = ibm_boto3.resource("s3",
                                 ibm_api_key_id=args.COS_APIKEY,
                                 ibm_service_instance_id=args.COS_INSTANCE_CRN,
                                 config=Config(signature_version="oauth"),
                                 endpoint_url=args.COS_ENDPOINT
                                 )
        multi_part_upload(args.COS_BUCKET, args.output, "./" + args.output)

    logging.info("Usage Report generation of {} file is complete.".format(args.output))