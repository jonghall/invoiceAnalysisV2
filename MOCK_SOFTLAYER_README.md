# Mock SoftLayer API Client

This module provides mock implementations of SoftLayer API services for testing purposes without requiring actual SoftLayer credentials or making real API calls.

## Features

- **Easy Integration**: Drop-in replacement for `SoftLayer.Client()`
- **Extensible Design**: Easily add new mock services
- **Realistic Data**: Generates random but realistic test data
- **Pagination Support**: Implements proper pagination for large datasets
- **No Dependencies**: Works without SoftLayer credentials or network access

## Usage

### Basic Usage

```python
from mock_softlayer import MockSoftLayerClient

# Create a mock client
client = MockSoftLayerClient(account_id='123456')

# Use it just like the real SoftLayer client
hardware = client['Account'].getHardware(id='123456', limit=10, offset=0)

for hw in hardware:
    print(f"Hardware: {hw['hostname']} in {hw['datacenter']['name']}")
```

### Using in Scripts

```python
import argparse
from mock_softlayer import MockSoftLayerClient
import SoftLayer

parser = argparse.ArgumentParser()
parser.add_argument("--mock", action="store_true", help="Use mock API")
args = parser.parse_args()

# Conditionally use mock or real client
if args.mock:
    client = MockSoftLayerClient(account_id='123456')
else:
    client = SoftLayer.Client(username="apikey", api_key=API_KEY)

# Rest of your code works the same way
hardware = client['Account'].getHardware(...)
```

### Convenience Function

```python
from mock_softlayer import create_mock_client

# Quick way to create a mock client
client = create_mock_client(account_id='123456')
```

## Currently Implemented Services

### Account Service

**Methods:**
- `getHardware(id, limit, offset, mask)` - Retrieve hardware devices with pagination

**Generated Data:**
- 25 hardware items per account by default
- Randomized datacenters (dal10, wdc04, lon02, etc.)
- Randomized OS types (VSphere, CentOS, RedHat, etc.)
- Network storage allocations (NAS, ISCSI, NFS)
- Storage capacity, IOPS, and usage statistics

### Placeholder Services

The following services are registered but not yet implemented:
- **Network**: Network operations (subnets, VLANs, etc.)
- **Virtual_Guest**: Virtual server operations

## Adding New Services

To add a new mock service:

1. **Create a new service class:**

```python
class MockSoftLayerBilling:
    """Mock SoftLayer_Billing service"""
    
    def __init__(self, account_id):
        self.account_id = account_id
    
    def getInvoices(self, limit=None, offset=0):
        # Implement mock invoice generation
        invoices = []
        # ... generate mock data ...
        return invoices
```

2. **Register it in MockSoftLayerClient:**

```python
class MockSoftLayerClient:
    def __init__(self, account_id=None, **kwargs):
        self.account_id = account_id
        self.services = {
            'Account': MockSoftLayerAccount(account_id),
            'Network': MockSoftLayerNetwork(account_id),
            'Virtual_Guest': MockSoftLayerVirtualGuest(account_id),
            'Billing': MockSoftLayerBilling(account_id),  # Add here
        }
```

3. **Use it in your scripts:**

```python
client = MockSoftLayerClient(account_id='123456')
invoices = client['Billing'].getInvoices(limit=10)
```

## Examples

See [mock_softlayer_example.py](mock_softlayer_example.py) for complete working examples including:
- Basic usage
- Pagination
- Convenience functions
- Error handling

Run the examples:
```bash
python3 mock_softlayer_example.py
```

## Integration with Existing Scripts

Scripts using the mock module:
- [classicConfigStorage.py](classicConfigStorage.py) - Storage report with `--mock` flag

To add mock support to your script:

1. Import the mock client:
   ```python
   from mock_softlayer import MockSoftLayerClient
   ```

2. Add a command-line flag:
   ```python
   parser.add_argument("--mock", action="store_true", 
                      help="Use mock API for testing")
   ```

3. Conditionally create the client:
   ```python
   if args.mock:
       client = MockSoftLayerClient(account_id=account_id)
   else:
       client = SoftLayer.Client(username="apikey", api_key=API_KEY)
   ```

## Benefits

- **Development**: Test scripts without API access
- **CI/CD**: Run automated tests without credentials
- **Demonstrations**: Show functionality without live data
- **Debugging**: Consistent, reproducible test data
- **Learning**: Understand API structure without quota limits

## Limitations

- Mock data is randomly generated, not from real accounts
- Not all SoftLayer services are implemented yet
- Some service methods may return simplified data structures
- Object masks are currently ignored (returns full objects)

## Contributing

To extend this module with additional services or improve existing ones:

1. Follow the existing pattern for service classes
2. Add comprehensive docstrings
3. Generate realistic test data
4. Register new services in `MockSoftLayerClient`
5. Add examples to [mock_softlayer_example.py](mock_softlayer_example.py)
