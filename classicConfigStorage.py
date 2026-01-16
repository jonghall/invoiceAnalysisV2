##
## Account Bare Metal allowed storage report
##

import SoftLayer, json, os, argparse, logging, logging.config
from dotenv import load_dotenv
from mock_softlayer import MockSoftLayerClient

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

def extract_os_from_hardware(hardware):
    """
    Extract the operating system name from hardware data.
    
    @param hardware: dict, hardware data containing softwareComponents
    @return: string, OS name or empty string if not found
    """
    if "softwareComponents" in hardware:
        if len(hardware["softwareComponents"]) > 0:
            return hardware["softwareComponents"][0]["softwareLicense"]["softwareDescription"]["name"]
    return ""

def extract_storage_from_hardware(hardware):
    """
    Extract storage information from hardware data.
    
    @param hardware: dict, hardware data containing allowedNetworkStorage
    @return: list of dicts, storage records with id, type, iops, capacity, and bytes used
    """
    storage_records = []
    
    if "allowedNetworkStorage" in hardware and len(hardware['allowedNetworkStorage']) > 0:
        for storage in hardware['allowedNetworkStorage']:
            storage_data = {
                'storageId': storage.get('id'),
                'storageType': storage.get('nasType', ''),
                'iops': storage.get('iops', 0),
                'capacityGb': storage.get('capacityGb', 0),
                'bytesUsed': storage.get('bytesUsed', 0)
            }
            storage_records.append(storage_data)
    
    return storage_records

def process_hardware_list(hardware_list):
    """
    Process a list of hardware items and extract relevant storage data.
    
    @param hardware_list: list of dicts, hardware items from SoftLayer API
    @return: list of dicts, processed hardware records with storage information
    """
    hardware_records = []
    
    for hardware in hardware_list:
        datacenter_name = hardware['datacenter']['name']
        hardware_id = hardware['id']
        os = extract_os_from_hardware(hardware)
        storage_records = extract_storage_from_hardware(hardware)
        
        # Only include hardware that has storage
        if storage_records:
            hardware_data = {
                "hardwareId": hardware_id,
                "datacenter": datacenter_name,
                "os": os,
                "storage": storage_records
            }
            hardware_records.append(hardware_data)
    
    return hardware_records

def get_account_hardware_storage(client, account_id, limit=10):
    """
    Retrieve all hardware with storage for a given account using pagination.
    
    @param client: SoftLayer client instance (real or mock)
    @param account_id: string, IMS account ID
    @param limit: int, number of records to retrieve per API call (default: 10)
    @return: list of dicts, hardware records with storage information
    """
    offset = 0
    all_hardware_records = []
    mask = 'id,datacenter.name,softwareComponents,allowedNetworkStorage.capacityGb,allowedNetworkStorage.nasType,allowedNetworkStorage.bytesUsed,allowedNetworkStorage.iops'
    
    while True:
        hardware_list = client['Account'].getHardware(
            id=account_id, 
            limit=limit, 
            offset=offset, 
            mask=mask
        )
        
        logging.info(f"Requesting Hardware for account {account_id}, limit={limit} @ offset {offset}, returned={len(hardware_list)}")
        
        if len(hardware_list) == 0:
            break
        
        # Process this batch of hardware
        hardware_records = process_hardware_list(hardware_list)
        all_hardware_records.extend(hardware_records)
        
        offset += len(hardware_list)
    
    return all_hardware_records


if __name__ == "__main__":

    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Create a JSON file with bare metal storage information for a list of IMS accounts."
    )
    
    # Authentication arguments
    parser.add_argument("-u", "--username", 
                        default=os.environ.get('ims_username', None),
                        metavar="USERNAME",
                        help="IMS user ID for internal authentication.")
    parser.add_argument("-p", "--password",
                        default=os.environ.get('ims_password', None),
                        metavar="PASSWORD",
                        help="IMS password for internal authentication.")
    parser.add_argument("-a", "--account",
                        default=os.environ.get('ims_account', None),
                        metavar="ACCOUNT",
                        help="IMS account number for internal authentication.")
    parser.add_argument("-k", "--api-key",
                        default=os.environ.get('IC_API_KEY', None),
                        metavar="API_KEY",
                        dest="api_key",
                        help="IBM Cloud API key for authentication.")
    
    # Input/Output arguments
    parser.add_argument("-i", "--input",
                        default=os.environ.get('input', 'accounts.txt'),
                        metavar="FILE",
                        help="Input file containing list of IMS accounts (one per line). Default: accounts.txt")
    parser.add_argument("-o", "--output",
                        default=os.environ.get('output', 'storage.json'),
                        metavar="FILE",
                        help="Output JSON file for storage data. Default: storage.json")
    parser.add_argument("-c", "--config",
                        default=None,
                        metavar="FILE",
                        help="Configuration file to load (optional).")
    
    # Mode arguments
    parser.add_argument("--mock",
                        action="store_true",
                        help="Use mock/simulated SoftLayer API for testing without credentials.")

    args = parser.parse_args()

    setup_logging()

    # Determine which client to use based on provided credentials
    client = None
    
    if args.mock:
        # Mock mode - client will be created per account in the loop
        logging.info("Using MOCK SoftLayer API endpoint")
        print("\n" + "="*60)
        print("MOCK MODE: Using simulated SoftLayer API")
        print("="*60 + "\n")
    
    elif args.api_key:
        # IBM Cloud API Key authentication
        logging.info("Using IBM Cloud Account API Key.")
        SL_ENDPOINT = "https://api.softlayer.com/xmlrpc/v3.1"
        client = SoftLayer.Client(username="apikey", api_key=args.api_key, endpoint_url=SL_ENDPOINT)
    
    elif args.username and args.password and args.account:
        # Internal employee authentication
        logging.info("Using Internal endpoint and employee credentials.")
        ims_yubikey = input("Yubi Key:")
        SL_ENDPOINT = "http://internal.applb.dal10.softlayer.local/v3.1/internal/xmlrpc"
        client = createEmployeeClient(SL_ENDPOINT, args.username, args.password, ims_yubikey)
    
    else:
        logging.error("You must provide either IBM Cloud API Key (--api-key) or Internal Employee credentials (--username, --password, --account).")
        quit()

    """
    READ LIST OF IMS ACCOUNTS AND 
    GET STORAGE ALLOCATION OF ALL HARDWARE DEVICES PER ACCOUNT
    """

    """ Get list of ims account numbers from file. Each row should contain one ims account number."""
    accountList = read_ims_accounts(args.input)

    accountRecords = [] 
    for imsAccount in accountList:
        # Create mock client for this account if in mock mode
        if args.mock:
            client = MockSoftLayerClient(account_id=imsAccount)
            logging.info(f"Created mock client for account {imsAccount}")
        
        # Get all hardware with storage for this account
        hardware_records = get_account_hardware_storage(client, imsAccount, limit=10)
        
        # Add account record with hardware data
        accountRecords.append({
            "accountId": imsAccount,
            "hardware": hardware_records
        })
    
    write_json_to_file(accountRecords, args.output, indent=2)

