##
## Account Bare Metal allowed storage report
## Place APIKEY & Username in config.ini
## or pass via commandline  (example: ConfigurationReport.py -u=userid -k=apikey)
##

import SoftLayer, json, os, argparse, logging, logging.config, random
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
def createEmployeeClient(end_point_employee, employee_user, passw, token):
    """Creates a softlayer-python client that can make API requests for a given employee_user"""
    client_noauth = SoftLayer.Client(endpoint_url=end_point_employee)
    client_noauth.auth = None
    employee = client_noauth['SoftLayer_User_Employee']
    result = employee.performExternalAuthentication(employee_user, passw, token)
    # Save result['hash'] somewhere to not have to login for every API request
    client_employee = SoftLayer.employee_client(username=employee_user, access_token=result['hash'], endpoint_url=end_point_employee)
    return client_employee
def read_ims_accounts(filename):
    """
    Read IMS account numbers from a CSV or text file.
    Each row should contain one IMS account number.
    
    @param filename: string, path to the file containing IMS account numbers
    @return: list of strings, IMS account numbers (stripped of whitespace)
    """
    accounts = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                # Strip whitespace and skip empty lines
                account = line.strip()
                if account:
                    # If the file is CSV with commas, take the first column
                    if ',' in account:
                        account = account.split(',')[0].strip()
                    accounts.append(account)
        logging.info(f"Successfully read {len(accounts)} IMS account numbers from {filename}")
        return accounts
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        return []
    except Exception as e:
        logging.error(f"Error reading IMS accounts from {filename}: {str(e)}")
        return []

def write_json_to_file(data, filename, indent=2):
    """
    Write a dictionary with JSON data to a text file.
    
    @param data: dict or list, data to be written as JSON
    @param filename: string, path to the output file
    @param indent: int, number of spaces for JSON indentation (default: 2)
    @return: bool, True if successful, False otherwise
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=indent)
        logging.info(f"Successfully wrote JSON data to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error writing JSON data to {filename}: {str(e)}")
        return False


class MockSoftLayerAccount:
    """Mock SoftLayer Account service for testing"""
    
    def __init__(self, account_id):
        self.account_id = account_id
        self.all_hardware = self._generate_hardware_data()
    
    def _generate_hardware_data(self, count=25):
        """Generate random hardware data"""
        hardware_list = []
        datacenters = ['dal10', 'dal12', 'dal13', 'wdc04', 'wdc07', 'sjc03', 'sjc04', 'lon02', 'lon04', 'fra02', 'tok02', 'syd01']
        os_types = ['VSphere', 'CentOS', 'RedHat', 'Ubuntu', 'Windows', 'Debian']
        storage_types = ['NAS', 'ISCSI', 'NFS']
        
        for i in range(count):
            hardware_id = 1000000 + i
            datacenter = random.choice(datacenters)
            os_type = random.choice(os_types)
            hostname = f"hardware-{self.account_id}-{i:03d}.{datacenter}.ibm.com"
            
            # Generate software components
            software_components = []
            if os_type:
                software_components.append({
                    'id': 10000 + i,
                    'softwareLicense': {
                        'softwareDescription': {
                            'name': os_type
                        }
                    }
                })
            
            # Generate storage allocations (more for VSphere systems)
            storage_count = random.randint(2, 8) if os_type == 'VSphere' else random.randint(0, 3)
            allowed_network_storage = []
            
            for s in range(storage_count):
                storage_id = 5000000 + (i * 100) + s
                storage = {
                    'id': storage_id,
                    'nasType': random.choice(storage_types),
                    'capacityGb': random.choice([500, 1000, 2000, 4000, 8000, 12000]),
                    'iops': random.choice([0.25, 2, 4, 10]) if random.random() > 0.3 else None,
                    'bytesUsed': random.randint(100000000, 8000000000000)
                }
                allowed_network_storage.append(storage)
            
            hardware = {
                'id': hardware_id,
                'hostname': hostname,
                'datacenter': {
                    'id': 100 + datacenters.index(datacenter),
                    'name': datacenter
                },
                'softwareComponents': software_components,
                'allowedNetworkStorage': allowed_network_storage
            }
            hardware_list.append(hardware)
        
        return hardware_list
    
    def getHardware(self, id=None, limit=None, offset=0, mask=None):
        """
        Mock implementation of SoftLayer_Account.getHardware
        
        @param id: account ID (ignored in mock, uses self.account_id)
        @param limit: number of results to return
        @param offset: offset for pagination
        @param mask: object mask (currently returns all data regardless)
        @return: list of hardware dictionaries
        """
        logging.info(f"Mock API: getHardware called with id={id}, limit={limit}, offset={offset}")
        
        # Apply pagination
        start = offset
        end = offset + limit if limit else len(self.all_hardware)
        
        result = self.all_hardware[start:end]
        logging.info(f"Mock API: Returning {len(result)} hardware items")
        
        return result


class MockSoftLayerClient:
    """Mock SoftLayer Client for testing"""
    
    def __init__(self, account_id=None, **kwargs):
        self.account_id = account_id
        self.services = {
            'Account': MockSoftLayerAccount(account_id)
        }
    
    def __getitem__(self, service_name):
        """Return mock service"""
        if service_name in self.services:
            return self.services[service_name]
        else:
            raise Exception(f"Mock service {service_name} not implemented")


if __name__ == "__main__":

    load_dotenv()
    ## READ CommandLine Arguments and load configuration file
    parser = argparse.ArgumentParser(description="Creates a json file for all the bare metal related storage for a list of IMS accounts.")
    parser.add_argument("-u", "--username", default=os.environ.get('ims_username', None), metavar="username",
                        help="IMS Userid")
    parser.add_argument("-p", "--password", default=os.environ.get('ims_password', None), metavar="password",
                        help="IMS Password")
    parser.add_argument("-a", "--account", default=os.environ.get('ims_account', None), metavar="account",
                        help="IMS Account")
    parser.add_argument("-k", "--IC_API_KEY", default=os.environ.get('IC_API_KEY', None), metavar="apikey",
                        help="IBM Cloud API Key")
    parser.add_argument("-i", "--inputfile", default=os.environ.get('inputfile', 'accounts.txt'), metavar="inputfile", 
                        help="Input file containing list of IMS accounts to report on.  One account number per line.")  
    parser.add_argument("-c", "--config", help="config.ini file to load")
    parser.add_argument("-o", "--output", default=os.environ.get('output', 'storage.json'),
                       help="Text filename for output file. (including extension of .json)")
    parser.add_argument("--mock", action="store_true",
                       help="Use mock/simulated SoftLayer API endpoint (for testing without credentials)")

    args = parser.parse_args()

    setup_logging()

    # Use mock endpoint if requested
    if args.mock:
        logging.info("Using MOCK SoftLayer API endpoint")
        print("\n" + "="*60)
        print("MOCK MODE: Using simulated SoftLayer API")
        print("="*60 + "\n")
        # Create a mock client - we'll use the first account from the input file
        client = None  # Will be created per account in the loop
    elif args.IC_API_KEY == None:
        if args.username == None or args.password == None or args.account == None:
            logging.error("You must provide either IBM Cloud ApiKey or Internal Employee credentials & IMS account.")
            quit()
        else:
            if args.username != None or args.password != None or args.account != None:
                logging.info("Using Internal endpoint and employee credentials.")
                ims_username = args.username
                ims_password = args.password
                ims_yubikey = input("Yubi Key:")
                ims_account = args.account
                SL_ENDPOINT = "http://internal.applb.dal10.softlayer.local/v3.1/internal/xmlrpc"
                client = createEmployeeClient(SL_ENDPOINT, ims_username, ims_password, ims_yubikey)
            else:
                logging.error("Error!  Can't find internal credentials or ims account.")
                quit()
    else:
        logging.info("Using IBM Cloud Account API Key.")
        IC_API_KEY = args.IC_API_KEY
        ims_account = None

        # Change endpoint to private Endpoint if command line open chosen

        SL_ENDPOINT = "https://api.softlayer.com/xmlrpc/v3.1"

        # Create Classic infra API client
        client = SoftLayer.Client(username="apikey", api_key=IC_API_KEY, endpoint_url=SL_ENDPOINT)

    """
    READ LIST OF IMS ACCOUNTS AND 
    GET STORAGE ALLOCATION OF ALL HARDWARE DEVICES PER ACCOUNT
    """

    """ Get list of ims account numbers from file. Each row should contain one ims account number."""
    accountList = read_ims_accounts(args.inputfile)

    accountRecords = [] 
    for imsAccount in accountList:
        # Create mock client for this account if in mock mode
        if args.mock:
            client = MockSoftLayerClient(account_id=imsAccount)
            logging.info(f"Created mock client for account {imsAccount}")
        
        limit = 10
        offset = 0
        """ Initialize account record """
        accountRecords.append({"accountId": imsAccount,
                               "hardware": []})

        while True:
            hardwarelist = client['Account'].getHardware(id=imsAccount, limit=limit, offset=offset, mask='id,datacenter.name,softwareComponents,allowedNetworkStorage.capacityGb, allowedNetworkStorage.nasType, allowedNetworkS.bytesUsed, allowedNetworkStorage.iops')

            logging.info("Requesting Hardware for account {}, limit={} @ offset {}, returned={}".format(imsAccount, limit, offset, len(hardwarelist)))
            if len(hardwarelist) == 0:
                break
            else:
                offset = offset + len(hardwarelist)
            """
            Extract hardware data from json
            """
            hardwareRecords = []
            for hardware in hardwarelist:
                datacenterName = hardware['datacenter']['name']
                hardwareId = hardware['id']

                if "softwareComponents" in hardware:
                    if len(hardware["softwareComponents"])>0:
                        os = hardware["softwareComponents"][0]["softwareLicense"]["softwareDescription"]["name"]
                    else:
                        os = ""
                else:
                    os = ""

                """ If allowed network storage records exist iterate through storage"""

                if "allowedNetworkStorage" in hardware:
                        if len(hardware['allowedNetworkStorage']) > 0:
                            hardwareData = {
                                "hardwareId": hardwareId,
                                "datacenter": datacenterName,
                                "os": os,
                                "storage":[]
                            }
                            storageRecords = []
                            for storage in hardware['allowedNetworkStorage']:
                                storageId = storage['id']
                                storageType = ""
                                iops = 0
                                capacity = 0
                                bytesUsed = 0        
                                if 'nasType' in storage.keys():
                                    storageType = storage['nasType']
                                if 'iops' in storage.keys():
                                    iops = storage['iops']
                                if 'capacityGb' in storage.keys():
                                    capacity = storage['capacityGb']
                                if 'bytesUsed' in storage.keys():
                                    bytesUsed = storage['bytesUsed']
                                data = {       
                                        'storageId': storageId,
                                        'storageType': storageType,
                                        'iops': iops,
                                        'capacityGb': capacity,
                                        'bytesUsed': bytesUsed
                                    }
                                storageRecords.append(data)
                            hardwareData["storage"] = storageRecords
                            hardwareRecords.append(hardwareData) 


                if len(hardwareRecords) > 0:
                    accountRecords[-1]['hardware'] = hardwareRecords        
    write_json_to_file(accountRecords, args.output, indent=2)

