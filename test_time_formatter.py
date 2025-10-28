"""
Unit tests for time_formatter module.
"""

import unittest
from datetime import datetime, time
from time_formatter import (
    format_time_24hr,
    format_time_from_components,
    parse_time_24hr,
    get_current_time_24hr,
)


class TestTimeFormatter(unittest.TestCase):
    """Test cases for time formatting functions."""
    
    def test_format_time_24hr(self):
        """Test formatting datetime to 24-hour format."""
        dt = datetime(2025, 10, 28, 12, 44, 30)
        result = format_time_24hr(dt)
        self.assertEqual(result, "12:44")
        
    def test_format_time_24hr_midnight(self):
        """Test formatting midnight."""
        dt = datetime(2025, 10, 28, 0, 0, 0)
        result = format_time_24hr(dt)
        self.assertEqual(result, "00:00")
        
    def test_format_time_24hr_noon(self):
        """Test formatting noon."""
        dt = datetime(2025, 10, 28, 12, 0, 0)
        result = format_time_24hr(dt)
        self.assertEqual(result, "12:00")
        
    def test_format_time_24hr_evening(self):
        """Test formatting evening time."""
        dt = datetime(2025, 10, 28, 23, 59, 59)
        result = format_time_24hr(dt)
        self.assertEqual(result, "23:59")
    
    def test_format_time_from_components_valid(self):
        """Test formatting time from valid components."""
        result = format_time_from_components(12, 44)
        self.assertEqual(result, "12:44")
        
    def test_format_time_from_components_single_digit(self):
        """Test formatting time with single digit values."""
        result = format_time_from_components(9, 5)
        self.assertEqual(result, "09:05")
        
    def test_format_time_from_components_invalid_hour(self):
        """Test formatting with invalid hour."""
        with self.assertRaises(ValueError) as context:
            format_time_from_components(24, 30)
        self.assertIn("Hour must be between 0 and 23", str(context.exception))
        
    def test_format_time_from_components_negative_hour(self):
        """Test formatting with negative hour."""
        with self.assertRaises(ValueError):
            format_time_from_components(-1, 30)
            
    def test_format_time_from_components_invalid_minute(self):
        """Test formatting with invalid minute."""
        with self.assertRaises(ValueError) as context:
            format_time_from_components(12, 60)
        self.assertIn("Minute must be between 0 and 59", str(context.exception))
        
    def test_format_time_from_components_negative_minute(self):
        """Test formatting with negative minute."""
        with self.assertRaises(ValueError):
            format_time_from_components(12, -1)
    
    def test_parse_time_24hr_valid(self):
        """Test parsing valid time string."""
        result = parse_time_24hr("12:44")
        expected = time(12, 44)
        self.assertEqual(result, expected)
        
    def test_parse_time_24hr_midnight(self):
        """Test parsing midnight."""
        result = parse_time_24hr("00:00")
        expected = time(0, 0)
        self.assertEqual(result, expected)
        
    def test_parse_time_24hr_invalid_format(self):
        """Test parsing invalid format."""
        with self.assertRaises(ValueError) as context:
            parse_time_24hr("12:44:30")
        self.assertIn("Invalid time format", str(context.exception))
        
    def test_parse_time_24hr_invalid_string(self):
        """Test parsing invalid string."""
        with self.assertRaises(ValueError):
            parse_time_24hr("not a time")
            
    def test_parse_time_24hr_invalid_hour(self):
        """Test parsing with invalid hour value."""
        with self.assertRaises(ValueError):
            parse_time_24hr("25:30")
    
    def test_get_current_time_24hr_format(self):
        """Test getting current time returns proper format."""
        result = get_current_time_24hr()
        # Check format matches HH:MM pattern
        self.assertRegex(result, r"^\d{2}:\d{2}$")
        
        # Verify it can be parsed back
        parsed = parse_time_24hr(result)
        self.assertIsInstance(parsed, time)


if __name__ == "__main__":
    unittest.main()
