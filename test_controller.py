#!/usr/bin/env python3
"""
Simple test script to verify the MQTT systemd controller can be imported and instantiated
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from mqtt_systemd_controller import ServiceController
    print("✓ Successfully imported ServiceController")

    # Test instantiation (without actual MQTT connection)
    controller = ServiceController(debug=False)
    print("✓ Successfully instantiated ServiceController")

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