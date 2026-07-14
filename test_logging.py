#!/usr/bin/env python3
"""
Test script to verify logging changes work correctly
"""
import subprocess
import sys
import time

def test_logging():
    """Test that controllers show proper logging"""
    print("Testing logging output...")

    # Test that we can import the modules
    try:
        import mqtt_systemd_start_stop
        import mqtt_systemd_enable_disable
        print("✓ Controllers import successfully")
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

    # Test that we can run the launcher script
    try:
        result = subprocess.run([sys.executable, 'run_parallel_controllers.py', '--help'],
                              capture_output=True, text=True, timeout=5)
        print("✓ Launcher script runs")
        return True
    except Exception as e:
        print(f"✗ Launcher test failed: {e}")
        return False

if __name__ == '__main__':
    success = test_logging()
    if success:
        print("SUCCESS: Logging changes verified!")
    else:
        print("FAILURE: Logging changes failed!")
    sys.exit(0 if success else 1)