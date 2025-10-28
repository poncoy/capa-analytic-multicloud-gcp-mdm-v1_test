# capa-analytic-multicloud-gcp-mdm-v1_test

## Time Formatter Module

A Python module for formatting and handling time in 24-hour format (HH:mm), suitable for session time display in analytics applications.

### Features

- Format datetime objects to 24-hour format (e.g., "12:44")
- Create time strings from hour and minute components
- Parse time strings back to time objects
- Get current time in 24-hour format
- Comprehensive input validation

### Usage

```python
from time_formatter import format_time_from_components

# Format time from components
time_str = format_time_from_components(12, 44)
print(time_str)  # Output: "12:44"
```

See `example.py` for more usage examples.

### Testing

Run the unit tests:

```bash
python -m unittest test_time_formatter.py -v
```

### Files

- `time_formatter.py` - Main module with time formatting functions
- `test_time_formatter.py` - Unit tests for the time formatter
- `example.py` - Example usage and demonstration