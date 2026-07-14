#!/usr/bin/env python3
"""
Parallel launcher for MQTT systemd controllers
Runs the start/stop, enable/disable, and command executor controllers in parallel.
By default all three are launched; use --start-stop, --enable-disable, and
--command-executor to select a subset.
"""
import sys
import subprocess
import time
import os

CONTROLLERS = {
    "start_stop": {
        "script": "mqtt_systemd_start_stop.py",
        "label": "Start/Stop Controller",
    },
    "enable_disable": {
        "script": "mqtt_systemd_enable_disable.py",
        "label": "Enable/Disable Controller",
    },
    "command_executor": {
        "script": "mqtt_command_executor.py",
        "label": "Command Executor",
    },
}


def graceful_stop(process, label, timeout=10):
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


def parse_flags(argv):
    """Determine which controllers to run based on CLI flags.

    If any of --start-stop, --enable-disable, or --command-executor is given,
    only the specified controllers are launched.  If none are given, all three
    run by default.
    """
    debug = '--debug' in argv
    verbose = '--verbose' in argv

    select_keys = {
        "--start-stop": "start_stop",
        "--enable-disable": "enable_disable",
        "--command-executor": "command_executor",
    }
    requested = [key for flag, key in select_keys.items() if flag in argv]

    if requested:
        return debug, verbose, requested
    return debug, verbose, list(CONTROLLERS.keys())


def main():
    """Main function to run controllers in parallel"""
    debug, verbose, controller_keys = parse_flags(sys.argv)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build the list of (label, process) tuples
    processes = []

    for key in controller_keys:
        info = CONTROLLERS[key]
        script_path = os.path.join(script_dir, info["script"])

        if not os.path.exists(script_path):
            print(f"Error: {info['label']} script not found at {script_path}")
            sys.exit(1)

        proc = run_controller(script_path, info["label"], debug, verbose)
        if proc is None:
            print(f"Failed to start {info['label']}")
            sys.exit(1)
        processes.append((info["label"], proc))

    print(f"{len(processes)} controller(s) started successfully")
    print("Press Ctrl+C to stop all controllers")

    try:
        while True:
            running = [(label, p) for label, p in processes if p.poll() is None]

            if not running:
                print("All controllers have stopped")
                break

            # If any controller stopped early, log it and bring the others down
            stopped = [label for label, p in processes if p.poll() is not None]
            for label in stopped:
                print(f"{label} has stopped")

            if len(running) < len(processes):
                for label, p in running:
                    graceful_stop(p, label)
                break

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down controllers...")
        try:
            for label, proc in processes:
                graceful_stop(proc, label)
            print("Shutdown complete")
        except Exception as e:
            print(f"Error during shutdown: {e}")
    finally:
        # Reap any remaining zombie processes
        for _label, proc in processes:
            proc.wait()


if __name__ == '__main__':
    main()
