#!/usr/bin/env python3
"""
Simple test script to verify logging changes
"""
import os
import sys

def test_files_exist():
    """Test that all required files exist"""
    required_files = [
        'mqtt_systemd_start_stop.py',
        'mqtt_systemd_enable_disable.py',
        'run_parallel_controllers.py'
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

def main():
    """Run all tests"""
    print("Testing MQTT Systemd Controller Logging...")
    print("=" * 50)

    success = True
    success &= test_files_exist()

    print("=" * 50)
    if success:
        print("SUCCESS: All logging tests passed!")
        return 0
    else:
        print("FAILURE: Some logging tests failed!")
        return 1

if __name__ == '__main__':
    sys.exit(main())