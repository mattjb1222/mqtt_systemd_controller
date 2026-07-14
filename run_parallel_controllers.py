#!/usr/bin/env python3
"""
Parallel launcher for MQTT systemd controllers
This script runs both the start/stop controller and enable/disable controller in parallel threads
"""
import sys
import threading
import time
import os
from subprocess import Popen

def run_controller(script_path, controller_name, debug=False):
    """Run a controller script in a separate process"""
    try:
        cmd = [sys.executable, script_path]
        if debug:
            cmd.append('--debug')

        print(f"Starting {controller_name}...")
        process = Popen(cmd)
        return process
    except Exception as e:
        print(f"Error starting {controller_name}: {e}")
        return None

def main():
    """Main function to run both controllers in parallel"""
    # Check if we're running in debug mode
    debug = '--debug' in sys.argv

    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define the controller scripts
    start_stop_script = os.path.join(script_dir, 'mqtt_systemd_start_stop.py')
    enable_disable_script = os.path.join(script_dir, 'mqtt_systemd_enable_disable.py')

    # Validate that both scripts exist
    if not os.path.exists(start_stop_script):
        print(f"Error: Start/stop controller script not found at {start_stop_script}")
        sys.exit(1)

    if not os.path.exists(enable_disable_script):
        print(f"Error: Enable/disable controller script not found at {enable_disable_script}")
        sys.exit(1)

    print("Starting parallel MQTT systemd controllers...")

    # Start both controllers in separate threads
    start_stop_process = run_controller(start_stop_script, "Start/Stop Controller", debug)
    enable_disable_process = run_controller(enable_disable_script, "Enable/Disable Controller", debug)

    if not start_stop_process or not enable_disable_process:
        print("Failed to start one or both controllers")
        sys.exit(1)

    print("Both controllers started successfully")
    print("Press Ctrl+C to stop both controllers")

    try:
        # Wait for both processes to complete
        while True:
            # Check if processes are still running
            start_stop_running = start_stop_process.poll() is None
            enable_disable_running = enable_disable_process.poll() is None

            if not start_stop_running and not enable_disable_running:
                print("Both controllers have stopped")
                break
            elif not start_stop_running:
                print("Start/Stop controller has stopped")
                enable_disable_process.terminate()  # Terminate the other one
                break
            elif not enable_disable_running:
                print("Enable/Disable controller has stopped")
                start_stop_process.terminate()  # Terminate the other one
                break

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down controllers...")
        try:
            start_stop_process.terminate()
            enable_disable_process.terminate()
            start_stop_process.wait()
            enable_disable_process.wait()
            print("Controllers stopped successfully")
        except Exception as e:
            print(f"Error during shutdown: {e}")

if __name__ == '__main__':
    main()