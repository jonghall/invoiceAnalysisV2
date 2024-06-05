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
import os, logging, logging.config, os.path, argparse, base64, requests, pytz
import pandas as pd
from datetime import datetime, timezone
from dateutil.relativedelta import *
from dateutil import tz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Personalization, Email, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId)
import ibm_boto3
from urllib import parse
from ibm_botocore.client import Config, ClientError
from ibm_platform_services import IamIdentityV1, UsageReportsV4, GlobalSearchV2
from ibm_platform_services.resource_controller_v2 import *
from ibm_platform_services.user_management_v1 import *
from ibm_vpc import VpcV1
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
    global authenticator, user_management_service, usage_reports_service, resource_controller_service, iam_identity_service, global_search_service, vpc_service_us_south, vpc_service_us_east, \
        vpc_service_br_sao, vpc_service_ca_tor, vpc_service_eu_gb, vpc_service_eu_de, vpc_service_eu_es, vpc_service_au_syd, vpc_service_jp_tok, vpc_service_jp_osa, \
        endpoints

    try:
        authenticator = IAMAuthenticator(IC_API_KEY)
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        iam_identity_service = IamIdentityV1(authenticator=authenticator)
        iam_identity_service.enable_retries(max_retries=5, retry_interval=1.0)
        iam_identity_service.set_http_config({'timeout': 120})
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

    try:
        user_management_service = UserManagementV1(authenticator=authenticator)
        user_management_service.enable_retries(max_retries=5, retry_interval=1.0)
        user_management_service.set_http_config({'timeout': 120})
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)


    """
        Define RIAS Service endpoints for each VPC region
        """
    try:
        vpc_service_us_south = VpcV1(authenticator=authenticator)
        vpc_service_us_south.set_service_url('https://us-south.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_us_east = VpcV1(authenticator=authenticator)
        vpc_service_us_east.set_service_url('https://us-east.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_ca_tor = VpcV1(authenticator=authenticator)
        vpc_service_ca_tor.set_service_url('https://ca-tor.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_br_sao = VpcV1(authenticator=authenticator)
        vpc_service_br_sao.set_service_url('https://br-sao.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_eu_gb = VpcV1(authenticator=authenticator)
        vpc_service_eu_gb.set_service_url('https://eu-gb.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_eu_de = VpcV1(authenticator=authenticator)
        vpc_service_eu_de.set_service_url('https://eu-de.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_eu_es = VpcV1(authenticator=authenticator)
        vpc_service_eu_es.set_service_url('https://eu-es.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_au_syd = VpcV1(authenticator=authenticator)
        vpc_service_au_syd.set_service_url('https://au-syd.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_jp_tok = VpcV1(authenticator=authenticator)
        vpc_service_jp_tok.set_service_url('https://jp-tok.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    try:
        vpc_service_jp_osa = VpcV1(authenticator=authenticator)
        vpc_service_jp_osa.set_service_url('https://jp-osa.iaas.cloud.ibm.com/v1')
    except ApiException as e:
        logging.error("API exception {}.".format(str(e)))
        quit(1)

    endpoints = [{"region": "au-syd", "endpoint": vpc_service_au_syd},
                 {"region": "jp-osa", "endpoint": vpc_service_jp_osa},
                 {"region": "jp-tok", "endpoint": vpc_service_jp_tok},
                 {"region": "eu-de", "endpoint": vpc_service_eu_de},
                 {"region": "eu-es", "endpoint": vpc_service_eu_es},
                 {"region": "eu-gb", "endpoint": vpc_service_eu_gb},
                 {"region": "ca-tor", "endpoint": vpc_service_ca_tor},
                 {"region": "us-south", "endpoint": vpc_service_us_south},
                 {"region": "us-east", "endpoint": vpc_service_us_east},
                 {"region": "br-sao", "endpoint": vpc_service_br_sao}]
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
def prePopulateUserCache(account_id):
        """
        Populate List of Users for Account
        :param ibmid:
        :return:
        """
        logging.info("User Cache being pre-populated with users.")
        """ Read User Map to Sales Role to user """
        user_map = pd.read_json("user-map.json")

        all_results = []
        pager = UsersPager(
            client=user_management_service,
            account_id=account_id,
        )
        while pager.has_next():
            next_page = pager.get_next()
            assert next_page is not None
            all_results.extend(next_page)

        user_cache = {}
        for user in all_results:
            iam_id = user["iam_id"]
            email = user["email"]
            """Lookup user in file by email """
            userrole = user_map[(user_map['email'] == email)]
            if len(userrole) > 0:
                user["role"] = userrole.iloc[0]["GTM Role"]
                user["org"] = userrole.iloc[0]["OrgName"]
                user["geo"] = userrole.iloc[0]["Geo"]
                user["market"] = userrole.iloc[0]["Market"]
            else:
                user["role"] = ""
                user["org"] = ""
                user["geo"] = ""
                user["market"] = ""
            user_cache[iam_id] = user

        return user_cache
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

    def getUser(iam_id):
        """
        Check User Cache for User
        """
        if iam_id not in user_cache:
            logging.debug("Cache miss for User {}".format(iam_id))
            user_profile = []
        else:
            user_profile = user_cache[iam_id]
        return user_profile
    def getImage(region, image):
        """
        Check Image_Cache for image details to get OSName, OSVendor, and OSVersion.
        :param image:
        :return:
        """

        def getImagefromApi(region, id):
            """ Get image data using api from specific region endpoint """
            endpoint = next((item["endpoint"] for item in endpoints if item["region"] == region), False)
            if endpoint is False:
                logging.error("No valid VPC Endpoint found for region {}".format(region))
                quit()

            try:
                image = endpoint.get_image(id)
                result = image.result
            except ApiException as e:

                logging.warning(
                    "Get VPC image data {} in {} failed with status code {}:{}".format(id, region, str(e.code),
                                                                                       e.message))
                result = {}

            """
            If valid result, add to cache for next lookup
            """
            if result != {}:
                logging.info("Caching VPC image data for image {} in image_cache.".format(id))
                image_cache[id] = result
            return result

        global image_cache
        if image not in image_cache:
            """ Cache miss and lookup via api """
            image_data = getImagefromApi(region, image)
        else:
            image_data = image_cache[image]
        return image_data

    def getVPCInstance(instance):
        """
        Check Tag_Cache for Resource tags which may have been retrieved previously
        """
        global vpc_instance_cache
        if instance not in vpc_instance_cache:
            logging.debug("Cache miss for VPC instance {}".format(instance))
            instance_data = []
        else:
            instance_data = vpc_instance_cache[instance]
        return instance_data

    def getWorker(instance):
        """
        Check worker cache to retrieve worker details
        """
        if instance not in worker_cache:
            logging.debug("Cache miss for worker instance {}".format(instance))
            instance_data = []
        else:
            instance_data = worker_cache[instance]
        return instance_data

    def getCluster(instance):
        """
        Check cluster cache to retrieve cluster details
        """
        if instance not in cluster_cache:
            logging.debug("Cache miss for cluster instance {}".format(instance))
            instance_data = []
        else:
            instance_data = cluster_cache[instance]
        return instance_data

    def getBMInitialization(region, id):
        """ Retrieve BM intitialization Image to determine Operating System Info """
        endpoint = next((item["endpoint"] for item in endpoints if item["region"] == region), False)
        if endpoint is False:
            logging.error("No valid VPC Endpoint found for region {}".format(region))
            quit()

        try:
            bare_metal_server_initialization = endpoint.get_bare_metal_server_initialization(id=id).get_result()
        except ApiException as e:
            logging.error(
                "Get BM Initialization data {} in {} failed with status code {}:{}".format(id, region, str(e.code),
                                                                                           e.message))
            quit(1)

        image_id = bare_metal_server_initialization["image"]["id"]
        image_data = getImage(region, image_id)

        return image_data


    global vpc_instance_cache, tag_cache, resource_controller_cache
    data = []
    nytz = pytz.timezone('America/New_York')

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
                logging.debug("Parsing Details for Instance {} in account {} of service {}..".format( instance["resource_instance_id"], instance["account_id"], instance["resource_id"]))
                pricing_country = instance.get("pricing_country", "")
                billing_country = instance.get("billing_country", "")
                currency_code = instance.get("currency_code", "")
                pricing_region = instance.get("pricing_region", "")
                resource_instance_id = instance.get("resource_instance_id")
                row = {
                    "account_id": instance["account_id"],
                    "instance_id": resource_instance_id,
                    "resource_group_id": instance.get("resource_group_id", ""),
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
                    "resource_group_name": instance.get("resource_group_name", ""),
                    "instance_name": instance["resource_instance_name"]
                }

                """
                 Initialize variables for records
                """
                created_at = ""
                provision_date = ""
                created_by = ""
                created_by_name = ""
                created_by_email = ""
                updated_at = ""
                updated_by = ""
                deleted_at = ""
                deprovision_date = ""
                deleted_by = ""
                restored_at = ""
                restored_by = ""
                state = ""
                type = ""
                profile = ""
                NumberOfInstStorageDisks = 0
                LifecycleAction = ""
                vpc = ""
                zone = ""
                primary_network_interface_subnet = ""
                primary_network_interface_primary_ip = ""
                numberOfVirtualCPUs = 00
                MemorySizeMiB = 0
                numa_count = 0
                vsibandwidth = 0
                total_network_bandwidth = 0
                total_volume_bandwidth = 0
                boot_volume_capacity = 0
                boot_volume_iops = 0
                bootVolumeCRN = ""
                bootVolumeName = ""
                numAttachedDataVolumes = ""
                totalDataVolumeCapacity = ""
                NumberofCores = 0
                NumberofSockets = 0
                ThreadsPerCore = 0
                BMbandwidth = 0
                BMdisks = 0
                OSName = ""
                OSVendor = ""
                OSVersion = ""
                volume_capacity = ""
                volume_iops = ""
                cluster_id = ""
                cluster_name = ""
                cluster_workers = ""
                cluster_version = ""
                cluster_state = ""
                cluster_status = ""
                worker_name = ""
                worker_state = ""
                worker_health = ""
                worker_version = ""
                worker_location = ""
                worker_flavor = ""
                worker_pool = ""
                dedicated_host = ""
                reservation_name = ""
                architecture = ""
                manufacturer = ""
                gpu_manufacturer = ""
                gpu_model = ""
                gpu_count = 0
                gpu_memory = 0
                lifecycle_state = ""
                health_state = ""
                status = ""
                NumberofCores = 0
                NumberofSockets = 0
                ThreadsPerCore = 0
                BMRawStorage = 0

                """ If not classic infrastructure query resource controller & VPC """
                if "classic_infrastructure" not in instance["resource_id"]:
                    """
                    Get additional resource instance detail from resource controller
                    """
                    resource_controller_instance = getResourceInstance(resource_instance_id)
                    logging.debug(
                        "Resource Controller JSON Data for instance [{}]".format(resource_controller_instance))

                    if "resource_id" in resource_controller_instance:
                        """
                        If resource controller data available capture additional fields
                        """
                        if "created_at" in resource_controller_instance:
                            created_at = resource_controller_instance["created_at"]
                            if created_at != "":
                                provision_date = pd.to_datetime(created_at, format="ISO8601").astimezone(timezone.utc).strftime("%Y-%m-%d")

                        if "updated_at" in resource_controller_instance:
                            updated_at = resource_controller_instance.get("updated_at", "")

                        if "deleted_at" in resource_controller_instance:
                            deleted_at = resource_controller_instance["deleted_at"]
                            if deleted_at != None:
                                deprovision_date = pd.to_datetime(deleted_at, format="ISO8601").astimezone(timezone.utc).strftime("%Y-%m-%d")

                        created_by = resource_controller_instance.get("created_by", "")
                        """ Lookup IBMid from cache to get user info """
                        user_profile = getUser(created_by)
                        if "firstname" in user_profile and "lastname" in user_profile:
                            created_by_name = user_profile["firstname"] + " " + user_profile["lastname"]
                        if "email" in user_profile:
                            created_by_email = user_profile["email"]
                        deleted_by = resource_controller_instance.get("deleted_by", "")
                        updated_by = resource_controller_instance.get("updated_by", "")
                        restored_at = resource_controller_instance.get("restored_at", "")
                        restored_by = resource_controller_instance.get("restored_by", "")
                        state = resource_controller_instance.get("state", "")
                        type = resource_controller_instance.get("type", "")

                        if "extensions" in resource_controller_instance:
                            if resource_controller_instance["resource_id"] == "is.instance":
                                if "VirtualMachineProperties" in resource_controller_instance["extensions"]:
                                    profile = resource_controller_instance["extensions"][
                                        "VirtualMachineProperties"].get("Profile", "")
                            elif resource_controller_instance["resource_id"] == "is.bare-metal-server":
                                if "BMServerProperties" in resource_controller_instance["extensions"]:
                                    profile = resource_controller_instance["extensions"]["BMServerProperties"].get(
                                        "Profile", "")
                            elif resource_controller_instance["resource_id"] == "is.volume":
                                if "VolumeInfo" in resource_controller_instance["extensions"]:
                                    volume_capacity = float(
                                        resource_controller_instance["extensions"]["VolumeInfo"].get("Capacity", 0))
                                    volume_iops = float(
                                        resource_controller_instance["extensions"]["VolumeInfo"].get("IOPS", 0))

                            if "Profile" in resource_controller_instance["extensions"]:
                                """ VirtualServer and Bare-metal Server extensions depreciated, profile stored in extension """
                                profile = resource_controller_instance["extensions"]["Profile"]

                            if "Resource" in resource_controller_instance["extensions"]:
                                zone = resource_controller_instance["extensions"]["Resource"].get("AvailabilityZone",
                                                                                                  "")
                                region = resource_controller_instance["extensions"]["Resource"]["Location"].get(
                                    "Region", "")
                                LifecycleAction = resource_controller_instance["extensions"]["Resource"].get(
                                    "LifecycleAction", "")

                        if resource_controller_instance["resource_id"] == "containers-kubernetes":
                            """
                            Get IKS or ROKS details
                            """
                            if instance["plan_id"] == "containers.kubernetes.cluster.roks" or instance[
                                "plan_id"] == "containers.kubernetes.cluster":
                                """
                                This is the cluster instance of a ROKS or IKS Cluster
                                """
                                cluster_id = instance["resource_instance_name"]
                                cluster = getCluster(cluster_id)
                                if len(cluster) > 0:
                                    cluster_name = cluster["name"]
                                    cluster_workers = cluster["workerCount"]
                                    cluster_version = cluster["masterKubeVersion"]
                                    cluster_state = cluster["state"]
                                    cluster_status = cluster["status"]
                                    vpc = cluster["vpc"]

                            if instance["plan_id"] == "containers.kubernetes.vpc.gen2.roks" or instance["plan_id"] == "containers.kubernetes.vpc.gen2":
                                """
                                This is an worker instance belonging to an IKS or ROKS cluster
                                Get cluster details from cluster cache
                                """
                                cluster_id = instance["resource_instance_name"][
                                             0:instance["resource_instance_name"].find('_')]
                                cluster = getCluster(cluster_id)


                                if len(cluster) > 0:
                                    cluster_name = cluster["name"]
                                    cluster_workers = cluster["workerCount"]
                                    cluster_version = cluster["masterKubeVersion"]
                                    cluster_state = cluster["state"]
                                    cluster_status = cluster["status"]
                                    vpc = cluster["vpc"]
                                """
                                Get worker details from worker_cache
                                """
                                worker_name = instance["resource_instance_name"][
                                              instance["resource_instance_name"].find('_') + 1:]
                                worker_instance = getWorker(worker_name)
                                if len(worker_instance) > 0:
                                    worker_state = worker_instance["lifecycle"]["actualState"]
                                    worker_health = worker_instance["health"]["state"]
                                    worker_version = worker_instance["kubeVersion"]["actual"]
                                    worker_location = worker_instance["location"]
                                    worker_flavor = worker_instance["flavor"]
                                    worker_pool = worker_instance["poolName"]
                                    zone = worker_instance["location"]
                                    vpc = worker_instance["vpc"]
                                    primary_network_interface_primary_ip = worker_instance["networkInterfaces"][0]["ipAddress"]
                                    primary_network_interface_subnet = worker_instance["subnet"]

                        if resource_controller_instance["resource_id"] == "is.instance" or resource_controller_instance["resource_id"] == "is.bare-metal-server":
                            """
                            if is.instance get VPC Virtual Machines information
                            Get additional VPC configuration data 
                            """
                            vpcinstance = getVPCInstance(resource_instance_id)
                            if len(vpcinstance) > 0:
                                if "vcpu" in vpcinstance:
                                    numberOfVirtualCPUs = vpcinstance["vcpu"]["count"]
                                    architecture = vpcinstance["vcpu"]["architecture"]
                                    manufacturer = vpcinstance["vcpu"]["manufacturer"]
                                if "memory" in vpcinstance:
                                    MemorySizeMiB = vpcinstance["memory"]
                                if "bandwidth" in vpcinstance:
                                    vsibandwidth = vpcinstance["bandwidth"]
                                if "total_network_bandwidth" in vpcinstance:
                                    total_network_bandwidth = vpcinstance["total_network_bandwidth"]
                                if "total_volume_bandwidth" in vpcinstance:
                                    total_volume_bandwidth = vpcinstance["total_volume_bandwidth"]
                                if "primary_network_interface" in vpcinstance:
                                    primary_network_interface_primary_ip = \
                                    vpcinstance["primary_network_interface"]["primary_ip"]["address"]
                                    primary_network_interface_subnet = \
                                    vpcinstance["primary_network_interface"]["subnet"]["name"]
                                if "numa_count" in vpcinstance:
                                    numa_count = vpcinstance["numa_count"]
                                if "vpc" in vpcinstance:
                                    vpc = vpcinstance["vpc"]["name"]
                                if "dedicated_host" in vpcinstance:
                                    dedicated_host = vpcinstance["dedicated_host"]["name"]
                                if "reservation" in instance:
                                    reservation_name = vpcinstance["reservation"]["name"]
                                if "gpu" in instance:
                                    gpu_manufacturer = vpcinstance["gpu"]["manufacturer"]
                                    gpu_model = vpcinstance["gpu"]["model"]
                                    gpu_memory = vpcinstance["gpu"]["memory"]
                                    gpu_count = vpcinstance["gpu"]["count"]
                                if "lifecycle_state" in instance:
                                    lifecycle_state = vpcinstance["lifecycle_state"]
                                if "status" in vpcinstance:
                                    status = vpcinstance["status"]
                                if "health_state" in vpcinstance:
                                    health_state = vpcinstance["health_state"]

                                """ If Az & Region missing from resource controller update from VPC data """
                                if region == "" and "zone" in vpcinstance:
                                    """ Parse region from zone if missing from RC, not independent variable because source data collected from reigon endpoint """
                                    region = vpcinstance["zone"]["name"][0:vpcinstance["zone"]["name"].rfind("-")]
                                if zone == "" and "zone" in vpcinstance:
                                    zone = vpcinstance["zone"]["name"]
                                if profile == "" and "profile" in vpcinstance:
                                    profile = vpcinstance["profile"]["name"]

                                if resource_controller_instance["resource_id"] == "is.instance" and "disks" in vpcinstance:
                                    NumberOfInstStorageDisks = len(vpcinstance["disks"])
                                """
                                Get Boot Image Operating System data from image cache
                                """
                                if resource_controller_instance["resource_id"] == "is.instance" and "image" in vpcinstance:
                                    """ image lookup for virtual server using vpc image id not crn """
                                    image_data = getImage(region, vpcinstance["image"]["id"])
                                    if len(image_data) > 0:
                                        OSName = image_data["operating_system"]["name"]
                                        OSVendor = image_data["operating_system"]["vendor"]
                                        OSVersion = image_data["operating_system"]["version"]
                                """
                                Get Block Storage Details for Virtual Server
                                """
                                if "boot_volume_attachment" in vpcinstance:
                                    bootVolumeCRN = vpcinstance["boot_volume_attachment"]["volume"].get("crn", None)

                                if bootVolumeCRN != None or bootVolumeCRN != "":
                                    """
                                    Get additional resource instance detail of Boot Volume from resource controller
                                    """
                                    if "boot_volume_attachment" in vpcinstance:
                                        bootVolumeName = vpcinstance["boot_volume_attachment"]["volume"].get("name", None)
                                        resourceDetail = getResourceInstance(bootVolumeCRN)

                                    if "extensions" in resource_controller_instance:
                                        if "VolumeInfo" in resource_controller_instance["extensions"]:
                                            boot_volume_capacity = float(
                                                resource_controller_instance["extensions"]["VolumeInfo"].get("Capacity", 0))
                                            boot_volume_iops = float(
                                                resource_controller_instance["extensions"]["VolumeInfo"].get("IOPS", 0))

                                if "volume_attachments" in vpcinstance:
                                    numAttachedDataVolumes = len(vpcinstance["volume_attachments"]) - 1
                                    totalDataVolumeCapacity = 0
                                    attachedDataVolumeDetail = []
                                    for volume in vpcinstance["volume_attachments"]:
                                        volumerow = {}
                                        volumeCRN = volume["volume"]["crn"]
                                        volumerow["name"] = volume["volume"]["name"]
                                        volumerow["id"] = volume["volume"]["id"]
                                        """ Ignore if Boot Volume """
                                        if bootVolumeCRN != volumeCRN:
                                            """
                                            Get additional resource instance detail of data Volumes from resource controller
                                            """
                                            resourceDetail = getResourceInstance(volumeCRN)
                                            if "extensions" in resourceDetail:
                                                if "VolumeInfo" in resourceDetail["extensions"]:
                                                    if "Capacity" in resourceDetail["extensions"]["VolumeInfo"]:
                                                        volumerow["capacity"] = \
                                                        resourceDetail["extensions"]["VolumeInfo"]["Capacity"]
                                                        totalDataVolumeCapacity = totalDataVolumeCapacity + float(
                                                            volumerow["capacity"])
                                                    if "IOPS" in resourceDetail["extensions"]["VolumeInfo"]:
                                                        volumerow["iops"] = resourceDetail["extensions"]["VolumeInfo"][
                                                            "IOPS"]
                                            attachedDataVolumeDetail.append(volumerow)
                                if resource_controller_instance["resource_id"] == "is.bare-metal-server":
                                    """
                                    Get Bare Metal Specific information
                                    """
                                    if len(vpcinstance) > 0:
                                        # Get BM initiation information to get image data.
                                        image_data = getBMInitialization(instance["region"], vpcinstance["id"])
                                        if len(image_data) > 0:
                                            OSName = image_data["operating_system"]["name"]
                                            OSVendor = image_data["operating_system"]["vendor"]
                                            OSVersion = image_data["operating_system"]["version"]

                                        if "cpu" in vpcinstance:
                                            architecture = vpcinstance["cpu"]["architecture"]
                                            NumberofCores = vpcinstance["cpu"]["core_count"]
                                            NumberofSockets = vpcinstance["cpu"]["socket_count"]
                                            ThreadsPerCore = vpcinstance["cpu"]["threads_per_core"]
                                        if "disks" in vpcinstance:
                                            disks = len(vpcinstance["disks"])
                                            BMRawStorage = 0
                                            for storage in vpcinstance["disks"]:
                                                if storage["interface_type"] == "nvme":
                                                    BMRawStorage = BMRawStorage + float(storage["size"])


                # get related tags attached to instance from cache
                tags = getTags(resource_instance_id)

                row_addition = {
                    "created_at": created_at,
                    "provision_date": provision_date,
                    "created_by": created_by,
                    "created_by_name": created_by_name,
                    "created_by_email": created_by_email,
                    "updated_at": updated_at,
                    "updated_by": updated_by,
                    "deleted_at": deleted_at,
                    "deprovision_date": deprovision_date,
                    "deleted_by": deleted_by,
                    "restored_at": restored_at,
                    "restored_by": restored_by,
                    "instance_state": state,
                    "type": type,
                    "instance_profile": profile,
                    "rcLifecycleAction": LifecycleAction,
                    "vpc": vpc,
                    "zone": zone,
                    "VSI_reservation_name": reservation_name,
                    "VSI_dedicated_host": dedicated_host,
                    "VSI_primaryNetworkSubnet": primary_network_interface_subnet,
                    "VSI_primaryNetworkPrimaryIp": primary_network_interface_primary_ip,
                    "VSI_virtualCPUs": numberOfVirtualCPUs,
                    "VSI_memorySizeMiB": MemorySizeMiB,
                    "VSI_numaCount": numa_count,
                    "VSI_architecture": architecture,
                    "VSI_manufacturer": manufacturer,
                    "VSI_gpu_manufacturer": gpu_manufacturer,
                    "VSI_gpu_model": gpu_model,
                    "VSI_gpu_memory": gpu_memory,
                    "VSI_gpu_count": gpu_count,
                    "VSI_totalBandwidth": vsibandwidth,
                    "VSI_totalNetworkBandwidth": total_network_bandwidth,
                    "VSI_totalVolumeBandwidth": total_volume_bandwidth,
                    "VSI_bootVolumeCapacity": boot_volume_capacity,
                    "VSI_bootVolumeIops": boot_volume_iops,
                    "VSI_bootVolumeCRN": bootVolumeCRN,
                    "VSI_bootVolumeName": bootVolumeName,
                    "VSI_NumberOfInstStorageDisks": NumberOfInstStorageDisks,
                    "VSI_numAttachedDataVolumes": numAttachedDataVolumes,
                    "VSI_totalDataVolumeCapacity": totalDataVolumeCapacity,
                    "BM_numberofCores": NumberofCores,
                    "BM_numberofSockets": NumberofSockets,
                    "BM_ThredsPerCore": ThreadsPerCore,
                    "BM_bandwidth": BMbandwidth,
                    "BM_disks": BMdisks,
                    "lifecycle_state": lifecycle_state,
                    "health_state": health_state,
                    "status": status,
                    "BMnumberofCores": NumberofCores,
                    "BMnumberofSockets": NumberofSockets,
                    "BMthreadsPerCore": ThreadsPerCore,
                    "BMRawStorage": BMRawStorage,
                    "OSName": OSName,
                    "OSVendor": OSVendor,
                    "OSVersion": OSVersion,
                    "volume_capacity": volume_capacity,
                    "volume_iops": volume_iops,
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "cluster_workers": cluster_workers,
                    "cluster_version": cluster_version,
                    "cluster_state": cluster_state,
                    "cluster_status": cluster_status,
                    "worker_name": worker_name,
                    "worker_state": worker_state,
                    "worker_health": worker_health,
                    "worker_version": worker_version,
                    "worker_location": worker_location,
                    "worker_flavor": worker_flavor,
                    "worker_pool": worker_pool,
                    "tags": tags,
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

        """ created Datatable from List if data exists otherwise initialize an empty dataframe """
        if len(data) > 0:
            instancesUsage = pd.DataFrame(data, columns=list(data[0].keys()))
        else:
            instancesUsage = pd.DataFrame()

    return instancesUsage
def populateVPCInstanceCache():
    """
    Get VPC instance information and create cache from each VPC regional endpoint
    """

    logging.info("VPC Cache being pre-populated with Virtual sever details for account.")
    items = []

    for ep in endpoints:
        """ Get virtual servers fro each VPC endpoint """
        endpoint = ep["endpoint"]
        instances = endpoint.list_instances()
        while True:
            try:
                result = instances.get_result()
            except ApiException as e:
                logging.error("List VPC virtual server instances with status code{}:{}".format(str(e.code), e.message))
                quit(1)

            items = items + result["instances"]
            if "next" not in result:
                break
            else:
                next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
                instances = endpoint.list_instances(start=next["start"])

        """ Get Bare Metal"""
        instances = endpoint.list_bare_metal_servers()
        while True:
            try:
                result = instances.get_result()
            except ApiException as e:
                logging.error("List BM server instances with status code {}:{}".format(str(e.code), e.message))
                quit(1)

            items = items + result["bare_metal_servers"]
            if "next" not in result:
                break
            else:
                next = dict(parse.parse_qsl(parse.urlsplit(result["next"]["href"]).query))
                instances = endpoint.list_bare_metal_servers(start=next["start"])

    instance_cache = {}
    for resource in items:
        crn = resource["crn"]
        instance_cache[crn] = resource

    return instance_cache
def populateClusterCache():
    """
    Get list of Kubernetes Clusters and Worker Nodes
    :return:
    """
    logging.info("Kubernetes worker cache being pre-populated from account.")
    cluster_cache = {}
    worker_cache = {}
    headers = {"Authorization": "Bearer "+authenticator.token_manager.get_token()}
    resp = requests.get('https://containers.cloud.ibm.com/global/v2/vpc/getClusters', headers=headers)
    if resp.status_code == 200:
        clusters = json.loads(resp.content)
    else:
        print("{} Error getting clusters.".format(resp.status_code))
        print("Error Data: {}".format(json.loads(resp.content)['errors']))
        quit(1)

    for cluster in clusters:
        cluster_id = cluster["id"]
        """ Get detail including VPC that isn't available in getClusters"""
        vpc = ""
        resp = requests.get("https://containers.cloud.ibm.com/global/v2/vpc/getCluster?cluster={}".format(cluster_id), headers=headers)
        if resp.status_code == 200:
            cluster_detail = json.loads(resp.content)
            """ Get VPC Name """
            endpoint = next((item["endpoint"] for item in endpoints if item["region"] == cluster_detail["region"]), False)
            if endpoint is False:
                logging.error("No valid VPC Endpoint found for K8 Cluster region {}".format(cluster_detail["region"]))
                quit()
            try:
                vpc = endpoint.get_vpc(cluster_detail["vpcs"][0])
                result = vpc.result
                vpc = result["name"]
                cluster_detail["vpc"] = vpc
            except ApiException as e:
                logging.warning(
                    "Get VPC data {} in {} failed with status code for k8 cluster {} {}:{}".format(id, cluster_detail["region"], cluster_id, str(e.code),
                                                                                 e.message))
                quit()

            cluster_cache[cluster_id] = cluster_detail

        resp = requests.get('https://containers.cloud.ibm.com/global/v2/vpc/getWorkers?cluster={}&showDeleted=True'.format(cluster_id), headers=headers)
        if resp.status_code == 200:
            workers = json.loads(resp.content)
        else:
            print("{} Error getting clusters.".format(resp.status_code))
            print("Error Data: {}".format(json.loads(resp.content)['errors']))
            quit(1)
        for worker in workers:
            id = worker["id"]
            worker["vpc"] = vpc
            """ Get Subnet """
            endpoint = next((item["endpoint"] for item in endpoints if item["region"] == cluster_detail["region"]), False)
            if endpoint is False:
                logging.error("No valid VPC Endpoint found for K8 Cluster region {}".format(cluster_detail["region"]))
                quit()
            try:
                subnet = endpoint.get_subnet(worker["networkInterfaces"][0]["subnetID"])
                result = subnet.result
                subnet_name = result["name"]
                worker["subnet"] = subnet_name
            except ApiException as e:
                logging.warning(
                    "Get Subnet data {} in {} failed with status code for k8 cluster {} {}:{}".format(id, cluster_detail["region"], cluster_id, str(e.code),
                                                                                 e.message))
                quit()
            worker_cache[id] = worker
    return cluster_cache, worker_cache
def createServiceDetail(paasUsage):
    """
    Write Service Usage detail tab to excel
    """
    logging.info("Creating ServiceUsageDetail tab.")

    paasUsage.to_excel(writer, sheet_name="ServiceUsageDetail")
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

    instancesUsage.to_excel(writer, sheet_name="Instances_Detail")
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
                                    values=["cost"],
                                    aggfunc="sum", margins=True, margins_name="Total",
                                    fill_value=0)
    new_order = ["cost"]
    usageSummary = usageSummary.reindex(new_order, axis=1, level=0)
    usageSummary.to_excel(writer, sheet_name='UsageSummary', startrow=2)
    worksheet = writer.sheets['UsageSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:A", 35, format2)
    worksheet.set_column("B:ZZ", 18, format1)
def createMetricSummary(paasUsage):
    logging.info("Creating Metric Plan Summary tab.")
    metricSummaryPlan = pd.pivot_table(paasUsage, index=["resource_name", "plan_name", "metric"],
                                 columns=["month"],
                                 values=["quantity", "cost"],
                                 aggfunc="sum", margins=True, margins_name="Total",
                                 fill_value=0)
    new_order = ["quantity", "cost"]
    metricSummaryPlan = metricSummaryPlan.reindex(new_order, axis=1, level=0)
    metricSummaryPlan.to_excel(writer, sheet_name='MetricPlanSummary', startrow=2)
    worksheet = writer.sheets['MetricPlanSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
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
def createChargesbyServer(servers):
    """
    Create Pivot by Server for current month (consolidate metrics)
    """

    """ Query only virtual CPU,  VCPU metric and last month so it calculates current total VCPU """

    logging.info("Calculating total charges per server (virtual and BareMetal).")
    vcpu = pd.pivot_table(servers, index=["region", "vpc", "instance_name", "instance_id", "instance_profile"],
                                    columns=["month"],
                                    values=["cost"],
                                    aggfunc={"cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0)

    new_order = ["cost"]
    vcpu = vcpu.reindex(new_order, axis=1, level=0)
    vcpu.to_excel(writer, 'ServerList',startrow=2)
    worksheet = writer.sheets['ServerList']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:A", 15, format2)
    worksheet.set_column("B:B", 25, format2)
    worksheet.set_column("C:C", 25, format2)
    worksheet.set_column("D:D", 120, format2)
    worksheet.set_column("E:E", 20, format2)
    worksheet.set_column("F:G", 18, format4)


    return
def createServerProvisioningTab(servers):
    logging.info("Creating Server Provisioning Tab by User.")
    vcpu = pd.pivot_table(servers, index=["created_by_name", "region", "vpc", "instance_name", "service_name", "instance_profile", "provision_date", "deprovision_date", "instance_state"],
                                    columns=["month"],
                                    values=["cost"],
                                    aggfunc={"cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0)

    new_order = ["cost"]
    vcpu = vcpu.reindex(new_order, axis=1, level=0)
    vcpu.to_excel(writer, sheet_name='ServersByUser', startrow=2)
    worksheet = writer.sheets['ServersByUser']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:A", 25, format2)
    worksheet.set_column("B:B", 15, format2)
    worksheet.set_column("C:C", 15, format2)
    worksheet.set_column("D:D", 40, format2)
    worksheet.set_column("E:I", 20, format2)
    worksheet.set_column("J:ZZ", 18, format4)
    return
def createChargesCOSInstance(cos):
    """
    Create Table of COS INstances with Charge metrics
    """

    logging.info("Calculating COS Instance charges.")

    pivot = pd.pivot_table(cos, index=["region", "resource_group_name", "instance_name", "plan_name", "metric", "unit"],
                             columns=["month"],
                             values=["quantity", "cost"],
                             aggfunc={"quantity": "sum", "cost": "sum"},
                             margins=True, margins_name="Total",
                             fill_value=0)

    new_order = ["quantity", "cost"]
    pivot = pivot.reindex(new_order, axis=1, level=0)

    pivot.to_excel(writer, sheet_name='COSBuckets', startrow=2)
    worksheet = writer.sheets['COSBuckets']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:A", 15, format2)
    worksheet.set_column("B:D", 30, format2)
    worksheet.set_column("E:E", 40, format2)
    worksheet.set_column("F:F", 18, format2)

    # Calculate and format Cost Columns by month
    months = len(cos.month.unique())
    startcol = 6
    endcol = startcol + months
    worksheet.set_column(startcol, endcol, 10, format3)
    worksheet.set_column(endcol + 1, endcol + 2 + months, 15, format4)

    return
def createVirtualServerTab(servers):
    """
    Create Virtual Server Summary region, vpc, zone, and profile
    """

    """ Query only virtual CPU,  VCPU metric and last month so it calculates current total VCPU """
    servers = servers.query('service_id == "is.instance"')

    logging.info("Creating VPC Virtual Server Summary Tab.")

    vcpu = pd.pivot_table(servers, index=["region", "vpc", "zone", "resource_group_name", "instance_profile"],
                                    columns=["month"],
                                    values=["instance_id", "cost"],
                                    aggfunc={"instance_id": "nunique", "cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', 'VSI_virtualCPUs': 'vCPU', "VSI_memorySizeMiB": "memoryMiB"})

    new_order = ["instance_count", "vCPU", "memoryMiB", "cost"]
    vcpu = vcpu.reindex(new_order, axis=1, level=0)
    vcpu.to_excel(writer, sheet_name='VirtualServerSummary',startrow=2)
    worksheet = writer.sheets['VirtualServerSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:C", 15, format2)
    worksheet.set_column("D:E", 25, format2)
    # Calculate and format Cost Columns by month
    months = len(servers.month.unique())
    startcol = 5
    endcol = startcol + months
    worksheet.set_column(startcol, endcol, 10, format3)
    worksheet.set_column(endcol + 1, endcol + 2 + months, 15, format1)

    return
def createBMServerTab(servers):
    """
    Create BM SUmmary region, vpc, zone and profile
    """

    logging.info("Creating Bare Metal Summary Tab.")
    servers = servers.query('service_id == "is.bare-metal-server"')

    if len(servers) == 0:
        return
    vcpu = pd.pivot_table(servers, index=["region", "vpc", "zone", "resource_group_name", "instance_profile"],
                                    columns=["month"],
                                    values=["instance_id", "cost"],
                                    aggfunc={"instance_id": "nunique", "cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count'})

    new_order = ["instance_count",  "cost"]
    vcpu = vcpu.reindex(new_order, axis=1, level=0)
    vcpu.to_excel(writer, 'BMServerSummary',startrow=2)
    worksheet = writer.sheets['BMServerSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:C", 15, format2)
    worksheet.set_column("D:E", 30, format2)
    # Calculate and format Cost Columns by month
    months = len(servers.month.unique())
    startcol = 5
    endcol = startcol + months
    worksheet.set_column(startcol, endcol, 10, format3)
    worksheet.set_column(endcol + 1 , endcol + 2 + months, 15, format1)
    return
def createVolumeSummary(volumes):
    """
    Create BM VCPU deployed by role, account, and az
    """

    logging.info("Creating Block Volume Summary tab.")

    pivot = pd.pivot_table(volumes, index=["region", "zone", "resource_group_name", "volume_iops", "volume_capacity"],
                                    columns=["month"],
                                    values=["instance_id", "cost"],
                                    aggfunc={"instance_id": "nunique", "cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0).rename(columns={'instance_id': 'instance_count', 'volume_capacity': 'capacity', 'volume_iops': 'iops'})

    new_order = ["instance_count",  "cost"]
    pivot = pivot.reindex(new_order, axis=1, level=0)
    pivot.to_excel(writer, sheet_name='VolumesSummary', startrow=2)
    worksheet = writer.sheets['VolumesSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    format4 = workbook.add_format({'num_format': '$#,##0.00'})
    worksheet.set_column("A:B", 15, format2)
    worksheet.set_column("C:E", 30, format2)
    # Calculate and format Cost Columns by month
    months = len(volumes.month.unique())
    startcol = 5
    endcol = startcol + months
    worksheet.set_column(startcol, endcol, 10, format3)
    worksheet.set_column(endcol + 1 , endcol + 2 + months, 15, format1)
    return
def createkubernetesTab(workers):
    """
    Create BM SUmmary region, vpc, zone and profile
    """

    logging.info("Creating Kubernetes Summary Tab.")

    pivot = pd.pivot_table(workers, index=["region", "vpc", "cluster_name", "worker_pool", "worker_name", "zone", "worker_flavor", "created_by_name", "instance_state"],
                                    columns=["month"],
                                    values=["cost"],
                                    aggfunc={"cost": "sum"},
                                    margins=True, margins_name="Total",
                                    fill_value=0)

    new_order = ["cost"]
    pivot = pivot.reindex(new_order, axis=1, level=0)
    pivot.to_excel(writer, sheet_name='KubernetesSummary',startrow=2)
    worksheet = writer.sheets['KubernetesSummary']
    boldtext = workbook.add_format({'bold': True})
    worksheet.write(0, 0, "Current month to date usage as of {}".format(runtimestamp),boldtext)
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    format3 = workbook.add_format({'num_format': '#,##0'})
    worksheet.set_column("A:A", 12, format2)
    worksheet.set_column("B:B", 25, format2)
    worksheet.set_column("C:D", 25, format2)
    worksheet.set_column("E:E", 60, format2)
    worksheet.set_column("F:G", 15, format2)
    worksheet.set_column("H:H", 25, format2)
    worksheet.set_column("I:I", 15, format3)
    # Calculate and format Cost Columns by month
    months = len(workers.month.unique())
    startcol = 9
    endcol = startcol + months
    worksheet.set_column(startcol, endcol, 10, format1)
    return
def createUserTab(user_cache):
    """
    Create User Tab
    """
    logging.info("Creating Users Tab.")

    users = []
    for key in user_cache:
        users.append(user_cache[key])

    users = pd.DataFrame.from_records(users, columns=["iam_id","firstname","lastname","state","email","role","org","geo","market","added_on","invitedOn"])
    users.to_excel(writer,  sheet_name="Users")
    worksheet = writer.sheets['Users']
    format1 = workbook.add_format({'num_format': '$#,##0.00'})
    format2 = workbook.add_format({'align': 'left'})
    worksheet.set_column("A:A", 5, format2)
    worksheet.set_column("B:B", 35, format2)
    worksheet.set_column("C:D", 25, format2)
    worksheet.set_column("E:E", 10, format2)
    worksheet.set_column("F:F", 30, format2)
    worksheet.set_column("G:G", 35, format2)
    worksheet.set_column("H:J", 20, format2)
    worksheet.set_column("K:L", 20, format2)
    totalrows,totalcols=users.shape
    worksheet.autofilter(0,0,totalrows,totalcols)

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
    parser.add_argument("--detail", action=argparse.BooleanOptionalAction, help="Include service usage detail tabs.")
    parser.add_argument("--cosinstances", action=argparse.BooleanOptionalAction, help="Include additional COS Instance Detail.")
    parser.add_argument("--kubernetes", action=argparse.BooleanOptionalAction, help="Include additional Kube Cluster Detail.")
    parser.add_argument("--users", action=argparse.BooleanOptionalAction, help="Include tab with details on users in account.")
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
            timestamp = datetime.now(timezone.utc)
            runtimestamp = timestamp.strftime("%H:%M UTC on %b %d, %Y")
            filetimestamp = "_{}".format(timestamp.strftime("%Y%m%d_%H%M"))
            logging.info("Running IBM Cloud Usage Report for AccountId: {} at {}.".format(accountId,runtimestamp))
            """
            Pre-populate Account Data to accelerate report generation
            """
            user_cache = prePopulateUserCache(accountId)
            tag_cache = prePopulateTagCache()
            resource_controller_cache = prePopulateResourceCache()
            """
            Pre-populate Configuration data on VPC and Clusters (requires Viewer access of VPC and Kubernetes clusters)
            """
            image_cache = {}
            vpc_instance_cache = populateVPCInstanceCache()
            cluster_cache, worker_cache = populateClusterCache()
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
    if args.detail:
        createServiceDetail(accountUsage)
        createInstancesDetailTab(instancesUsage)

    if len(accountUsage) > 0:
        createUsageSummaryTab(accountUsage)
        createMetricSummary(accountUsage)

    if args.vpc and len(instancesUsage) > 0:
        """
        Create VPC Server Tabs
        """
        servers = instancesUsage.query('service_id == "is.instance" or service_id == "is.bare-metal-server"')
        storage = instancesUsage.query('service_id == "is.volume"')

        months = instancesUsage.month.unique()

        """ create VPC Virtual Server & BM Server detail"""
        if len(servers) > 0:
            createVirtualServerTab(servers)
            createBMServerTab(servers)
            createServerProvisioningTab(servers)
        if len(storage) > 0:
            createVolumeSummary(storage)
    if args.cosinstances and len(instancesUsage) > 0:
        """
        Create COS Detail tab
        """
        cos = instancesUsage.query('service_name == "Cloud Object Storage"')
        if len(cos) > 0:
            createChargesCOSInstance(cos)

    if args.kubernetes and len(instancesUsage) > 0:
        workers = instancesUsage.query('service_id == "containers-kubernetes"')
        if len(workers) > 0:
            createkubernetesTab(workers)

    if args.users:
        createUserTab(user_cache)

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
        output = args.output
        split_tup = os.path.splitext(args.output)
        """ remove file extension """
        file_name = split_tup[0]
        cos = ibm_boto3.resource("s3",
                                 ibm_api_key_id=args.COS_APIKEY,
                                 ibm_service_instance_id=args.COS_INSTANCE_CRN,
                                 config=Config(signature_version="oauth"),
                                 endpoint_url=args.COS_ENDPOINT
                                 )
        multi_part_upload(args.COS_BUCKET, file_name + filetimestamp + ".xlsx", "./" + args.output)

    logging.info("Usage Report generation of {} file is complete.".format(args.output))