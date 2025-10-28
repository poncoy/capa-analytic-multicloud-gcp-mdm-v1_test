#!/usr/bin/env python
"""
Example usage of the time_formatter module.

This demonstrates the time formatting functionality for displaying
session times in 24-hour format (HH:mm).
"""

from datetime import datetime
from time_formatter import (
    format_time_24hr,
    format_time_from_components,
    parse_time_24hr,
    get_current_time_24hr,
)


def main():
    """Demonstrate time formatting functionality."""
    print("Time Formatter Demo")
    print("=" * 50)
    
    # Example 1: Format current time
    print("\n1. Current time in 24-hour format:")
    current_time = get_current_time_24hr()
    print(f"   Current time: {current_time}")
    
    # Example 2: Format specific datetime
    print("\n2. Format specific datetime (2025-10-28 12:44:00):")
    dt = datetime(2025, 10, 28, 12, 44, 0)
    formatted = format_time_24hr(dt)
    print(f"   Formatted time: {formatted}")
    
    # Example 3: Format from components
    print("\n3. Format time from hour and minute components:")
    time_str = format_time_from_components(12, 44)
    print(f"   Hour=12, Minute=44 → {time_str}")
    
    # Example 4: Parse time string
    print("\n4. Parse time string to time object:")
    time_str = "12:44"
    time_obj = parse_time_24hr(time_str)
    print(f"   Input: '{time_str}'")
    print(f"   Parsed: {time_obj}")
    print(f"   Hour: {time_obj.hour}, Minute: {time_obj.minute}")
    
    # Example 5: Various time formats throughout the day
    print("\n5. Various times throughout the day:")
    times = [
        (0, 0, "Midnight"),
        (6, 30, "Early morning"),
        (12, 0, "Noon"),
        (12, 44, "Afternoon"),
        (18, 15, "Evening"),
        (23, 59, "End of day"),
    ]
    
    for hour, minute, label in times:
        formatted = format_time_from_components(hour, minute)
        print(f"   {label:15s}: {formatted}")
    
    print("\n" + "=" * 50)
    print("Demo complete!")


if __name__ == "__main__":
    main()
