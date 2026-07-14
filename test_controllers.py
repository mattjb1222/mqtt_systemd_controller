#!/usr/bin/env python3
"""
Test script to verify the controller structure works correctly
"""
import os
import sys

def test_files_exist():
    """Test that all required files exist"""
    required_files = [
        'mqtt_systemd_start_stop.py',
        'mqtt_systemd_enable_disable.py',
        'run_parallel_controllers.py',
        'README.md'
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(f"Missing files: {missing_files}")
        return False
    else:
        print("All required files exist")
        return True

def test_imports():
    """Test that we can import the controller modules"""
    try:
        # Test importing the main controller
        import mqtt_systemd_controller
        print("✓ Main controller imports successfully")

        # Test importing the enable/disable controller
        import mqtt_systemd_enable_disable
        print("✓ Enable/Disable controller imports successfully")

        return True
    except Exception as e:
        print(f"✗ Import test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Testing MQTT Systemd Controller Structure...")
    print("=" * 50)

    success = True
    success &= test_files_exist()
    success &= test_imports()

    print("=" * 50)
    if success:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed!")
        return 1

if __name__ == '__main__':
    sys.exit(main())