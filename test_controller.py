#!/usr/bin/env python3
"""
Simple test script to verify the MQTT systemd controller can be imported and instantiated
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from mqtt_systemd_start_stop import StartStopController
    print("✓ Successfully imported StartStopController")

    # Test instantiation (without actual MQTT connection)
    controller = StartStopController(debug=False)
    print("✓ Successfully instantiated StartStopController")

    # Test basic configuration
    print(f"✓ Broker: {controller.broker}")
    print(f"✓ Port: {controller.port}")
    print(f"✓ Topics: {controller.topics}")
    print("✓ All tests passed!")

except Exception as e:
    print(f"✗ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)