"""
Mock SoftLayer API Client Module

This module provides mock implementations of SoftLayer API services for testing purposes.
It allows scripts to run without requiring actual SoftLayer credentials or making real API calls.

Usage:
    from mock_softlayer import MockSoftLayerClient
    
    client = MockSoftLayerClient(account_id='123456')
    hardware = client['Account'].getHardware(id='123456', limit=10, offset=0)

Adding New Services:
    1. Create a new mock service class (e.g., MockSoftLayerNetwork)
    2. Implement the required methods for that service
    3. Register the service in MockSoftLayerClient.__init__() services dict
"""

import random
import logging


class MockSoftLayerAccount:
    """
    Mock implementation of SoftLayer_Account service.
    
    This service provides methods for retrieving account-level information
    such as hardware, virtual servers, network storage, etc.
    """
    
    def __init__(self, account_id):
        """
        Initialize the mock Account service.
        
        @param account_id: The IMS account ID to simulate
        """
        self.account_id = account_id
        self.all_hardware = self._generate_hardware_data()
    
    def _generate_hardware_data(self, count=25):
        """
        Generate random hardware data for testing.
        
        @param count: Number of hardware items to generate
        @return: List of hardware dictionaries
        """
        hardware_list = []
        datacenters = ['dal10', 'dal12', 'dal13', 'wdc04', 'wdc07', 'sjc03', 'sjc04', 
                      'lon02', 'lon04', 'fra02', 'tok02', 'syd01']
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
        
        Simulates retrieving hardware devices for an account with pagination support.
        
        @param id: Account ID (ignored in mock, uses self.account_id)
        @param limit: Number of results to return
        @param offset: Offset for pagination
        @param mask: Object mask (currently returns all data regardless)
        @return: List of hardware dictionaries
        """
        logging.info(f"Mock API: getHardware called with id={id}, limit={limit}, offset={offset}")
        
        # Apply pagination
        start = offset
        end = offset + limit if limit else len(self.all_hardware)
        
        result = self.all_hardware[start:end]
        logging.info(f"Mock API: Returning {len(result)} hardware items")
        
        return result


class MockSoftLayerNetwork:
    """
    Mock implementation of SoftLayer_Network service.
    
    This service provides methods for network-related operations.
    Currently a placeholder for future implementation.
    """
    
    def __init__(self, account_id):
        """
        Initialize the mock Network service.
        
        @param account_id: The IMS account ID to simulate
        """
        self.account_id = account_id
    
    # Add network-related methods here as needed
    # Example: getSubnets(), getVlans(), etc.


class MockSoftLayerVirtualGuest:
    """
    Mock implementation of SoftLayer_Virtual_Guest service.
    
    This service provides methods for virtual server operations.
    Currently a placeholder for future implementation.
    """
    
    def __init__(self, account_id):
        """
        Initialize the mock Virtual Guest service.
        
        @param account_id: The IMS account ID to simulate
        """
        self.account_id = account_id
    
    # Add virtual guest methods here as needed
    # Example: getObject(), getBillingItem(), etc.


class MockSoftLayerClient:
    """
    Mock SoftLayer Client for testing purposes.
    
    This client mimics the behavior of the real SoftLayer Python client
    but returns simulated data instead of making actual API calls.
    
    Available Services:
        - Account: Account-level operations (hardware, virtual servers, etc.)
        - Network: Network operations (placeholder for future implementation)
        - Virtual_Guest: Virtual server operations (placeholder)
    
    Usage:
        client = MockSoftLayerClient(account_id='123456')
        hardware = client['Account'].getHardware(id='123456', limit=10)
    """
    
    def __init__(self, account_id=None, **kwargs):
        """
        Initialize the mock SoftLayer client.
        
        @param account_id: The IMS account ID to simulate
        @param kwargs: Additional parameters (ignored, for compatibility with real client)
        """
        self.account_id = account_id
        
        # Register all available mock services
        # Add new services here as they are implemented
        self.services = {
            'Account': MockSoftLayerAccount(account_id),
            'Network': MockSoftLayerNetwork(account_id),
            'Virtual_Guest': MockSoftLayerVirtualGuest(account_id),
        }
        
        logging.info(f"Initialized MockSoftLayerClient for account {account_id}")
    
    def __getitem__(self, service_name):
        """
        Return a mock service by name.
        
        @param service_name: Name of the service (e.g., 'Account', 'Network')
        @return: Mock service instance
        @raises Exception: If service is not implemented
        """
        if service_name in self.services:
            return self.services[service_name]
        else:
            available = ', '.join(self.services.keys())
            raise Exception(
                f"Mock service '{service_name}' not implemented. "
                f"Available services: {available}"
            )
    
    def authenticate_with_password(self, username, password):
        """
        Mock authentication method (does nothing).
        
        @param username: Username (ignored)
        @param password: Password (ignored)
        @return: True
        """
        logging.info(f"Mock API: authenticate_with_password called for user {username}")
        return True


# Convenience function for creating mock clients
def create_mock_client(account_id=None, **kwargs):
    """
    Create and return a mock SoftLayer client.
    
    This is a convenience function that can be used as a drop-in replacement
    for SoftLayer.Client() in test environments.
    
    @param account_id: The IMS account ID to simulate
    @param kwargs: Additional parameters (ignored)
    @return: MockSoftLayerClient instance
    """
    return MockSoftLayerClient(account_id=account_id, **kwargs)
