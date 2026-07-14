#!/usr/bin/env python3
"""
Parallel launcher for MQTT systemd controllers
This script runs both the start/stop controller and enable/disable controller in parallel threads
"""
import sys
import subprocess
import time
import os

def graceful_stop(process, label, timeout=3):
    """Terminate a process gracefully, escalating to kill if needed."""
    if process is None or process.poll() is not None:
        return  # Already stopped
    print(f"Stopping {label}...")
    process.terminate()
    try:
        process.wait(timeout=timeout)
        print(f"{label} stopped gracefully")
    except subprocess.TimeoutExpired:
        print(f"{label} did not stop in {timeout}s, killing...")
        process.kill()
        process.wait()  # Reap zombie


def run_controller(script_path, controller_name, debug=False, verbose=False):
    """Run a controller script in a separate process"""
    try:
        cmd = [sys.executable, script_path]
        if debug:
            cmd.append('--debug')
        if verbose:
            cmd.append('--verbose')

        print(f"Starting {controller_name}...")
        process = subprocess.Popen(cmd)
        return process
    except Exception as e:
        print(f"Error starting {controller_name}: {e}")
        return None

def main():
    """Main function to run both controllers in parallel"""
    # Check if we're running in debug or verbose mode
    debug = '--debug' in sys.argv
    verbose = '--verbose' in sys.argv

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
    start_stop_process = run_controller(start_stop_script, "Start/Stop Controller", debug, verbose)
    enable_disable_process = run_controller(enable_disable_script, "Enable/Disable Controller", debug, verbose)

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
                graceful_stop(enable_disable_process, "Enable/Disable Controller")
                break
            elif not enable_disable_running:
                print("Enable/Disable controller has stopped")
                graceful_stop(start_stop_process, "Start/Stop Controller")
                break

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down controllers...")
        try:
            graceful_stop(start_stop_process, "Start/Stop Controller")
            graceful_stop(enable_disable_process, "Enable/Disable Controller")
            print("Shutdown complete")
        except Exception as e:
            print(f"Error during shutdown: {e}")
    finally:
        # Reap any remaining zombie processes
        start_stop_process.wait()
        enable_disable_process.wait()

if __name__ == '__main__':
    main()