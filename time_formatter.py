"""
Time formatting utility for session time display.

This module provides functions to format time in 24-hour format (HH:mm).
"""

from datetime import datetime, time


def format_time_24hr(dt: datetime) -> str:
    """
    Format a datetime object to 24-hour time format (HH:mm).
    
    Args:
        dt: datetime object to format
        
    Returns:
        String representation of time in HH:mm format (e.g., "12:44")
    """
    return dt.strftime("%H:%M")


def format_time_from_components(hour: int, minute: int) -> str:
    """
    Format time from hour and minute components.
    
    Args:
        hour: Hour in 24-hour format (0-23)
        minute: Minute (0-59)
        
    Returns:
        String representation of time in HH:mm format (e.g., "12:44")
        
    Raises:
        ValueError: If hour or minute values are out of range
    """
    if not (0 <= hour <= 23):
        raise ValueError(f"Hour must be between 0 and 23, got {hour}")
    if not (0 <= minute <= 59):
        raise ValueError(f"Minute must be between 0 and 59, got {minute}")
    
    return f"{hour:02d}:{minute:02d}"


def parse_time_24hr(time_str: str) -> time:
    """
    Parse a time string in HH:mm format to a time object.
    
    Args:
        time_str: Time string in HH:mm format (e.g., "12:44")
        
    Returns:
        time object
        
    Raises:
        ValueError: If the time string is not in valid HH:mm format
    """
    try:
        dt = datetime.strptime(time_str, "%H:%M")
        return dt.time()
    except ValueError as e:
        raise ValueError(f"Invalid time format. Expected HH:mm, got '{time_str}'") from e


def get_current_time_24hr() -> str:
    """
    Get the current time in 24-hour format (HH:mm).
    
    Returns:
        String representation of current time in HH:mm format
    """
    return datetime.now().strftime("%H:%M")
