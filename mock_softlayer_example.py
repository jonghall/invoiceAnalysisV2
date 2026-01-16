"""
Example script demonstrating how to use the mock_softlayer module.

This shows how other scripts can integrate the mock SoftLayer client
for testing without requiring real credentials.
"""

import logging
from mock_softlayer import MockSoftLayerClient, create_mock_client

# Set up basic logging to see mock API activity
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def example_basic_usage():
    """Basic example: Create a mock client and retrieve hardware."""
    print("\n=== Example 1: Basic Usage ===")
    
    # Create a mock client for account 123456
    client = MockSoftLayerClient(account_id='123456')
    
    # Retrieve hardware (first 10 items)
    hardware_list = client['Account'].getHardware(id='123456', limit=10, offset=0)
    
    print(f"Retrieved {len(hardware_list)} hardware items")
    
    # Display first hardware item details
    if hardware_list:
        hw = hardware_list[0]
        print(f"\nFirst hardware item:")
        print(f"  ID: {hw['id']}")
        print(f"  Hostname: {hw['hostname']}")
        print(f"  Datacenter: {hw['datacenter']['name']}")
        print(f"  Storage devices: {len(hw.get('allowedNetworkStorage', []))}")


def example_pagination():
    """Example showing pagination through all hardware."""
    print("\n=== Example 2: Pagination ===")
    
    client = MockSoftLayerClient(account_id='789012')
    
    limit = 5
    offset = 0
    total_items = 0
    
    while True:
        hardware_list = client['Account'].getHardware(
            id='789012', 
            limit=limit, 
            offset=offset
        )
        
        if not hardware_list:
            break
        
        total_items += len(hardware_list)
        print(f"Page starting at offset {offset}: {len(hardware_list)} items")
        offset += len(hardware_list)
    
    print(f"Total hardware items retrieved: {total_items}")


def example_convenience_function():
    """Example using the convenience function."""
    print("\n=== Example 3: Using Convenience Function ===")
    
    # Use create_mock_client() as a drop-in replacement for SoftLayer.Client()
    client = create_mock_client(account_id='345678')
    
    hardware_list = client['Account'].getHardware(id='345678', limit=5)
    
    print(f"Retrieved {len(hardware_list)} items using convenience function")


def example_error_handling():
    """Example showing error handling for non-existent services."""
    print("\n=== Example 4: Error Handling ===")
    
    client = MockSoftLayerClient(account_id='999999')
    
    try:
        # Try to access a service that doesn't exist
        result = client['NonExistentService'].someMethod()
    except Exception as e:
        print(f"Caught expected error: {e}")


if __name__ == "__main__":
    print("Mock SoftLayer Client Examples")
    print("=" * 50)
    
    example_basic_usage()
    example_pagination()
    example_convenience_function()
    example_error_handling()
    
    print("\n" + "=" * 50)
    print("All examples completed successfully!")
