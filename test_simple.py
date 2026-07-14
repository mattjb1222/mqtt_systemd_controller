#!/usr/bin/env python3
"""
Simple test script to verify the MQTT systemd controller can be imported
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import mqtt_systemd_start_stop
    print("SUCCESS: Successfully imported mqtt_systemd_start_stop")

    # Test that we can access the StartStopController class
    from mqtt_systemd_start_stop import StartStopController
    print("SUCCESS: Successfully imported ServiceController")

    # Test instantiation with minimal configuration
    controller = StartStopController(debug=False)
    print("SUCCESS: Successfully instantiated StartStopController")

    print("All tests passed!")

except Exception as e:
    print(f"FAILED: Test failed with error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)