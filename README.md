# MQTT Systemd Controller

An enhanced MQTT-based systemd service controller that manages systemd services through MQTT commands with robust error handling, race condition protection, and polling mechanisms.

## Features

- **MQTT Integration**: Connects to MQTT broker to receive service control commands
- **Systemd Service Management**: Start, stop, and monitor systemd services
- **Polling**: Periodic state checking and publishing
- **Manual Change Detection**: Detects and reports manual service state changes
- **Race Condition Handling**: Robust deduplication to prevent loops and race conditions
- **Error Recovery**: Comprehensive error handling and retry mechanisms
- **Thread Safety**: Proper synchronization for concurrent operations
- **Configurable**: Environment-based configuration for flexibility

## Requirements

- Python 3.14+
- paho-mqtt library
- systemd (for service management)

## Installation

### Install uv (Python package manager)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your shell to make the `uv` command available in your PATH.

### Install dependencies

From within the project directory, run:

```bash
uv sync
```

This will install all dependencies defined in `pyproject.toml`.

## Configuration

Set the following environment variables:

- `MQTT_BROKER` - MQTT broker address (required)
- `MQTT_PORT` - MQTT broker port (default: 1883)
- `MQTT_USER` - MQTT username (required)
- `MQTT_PASS` - MQTT password (required)
- `MQTT_TOPIC` - MQTT topic(s) to subscribe to (comma-separated for multiple topics) - Example: `default/systemd/bluetooth`
- `POLLING_INTERVAL` - Polling interval in seconds (default: 30)
- `MANUAL_CHECK_INTERVAL` - Manual change detection interval in seconds (default: 5)
- `MESSAGE_DEDUPLICATION_WINDOW` - Message deduplication window in seconds (default: 0.1)

## Usage

```bash
uv run mqtt_systemd_controller.py [--debug] [--log-file /path/to/logfile]
```

## Message Format

### Service Control Commands

```json
{
  "hostname": "your-hostname",
  "service": "service-name",
  "state": "start|stop|started|stopped"
}
```

### Command Execution

```json
{
  "hostname": "your-hostname",
  "command": "command-to-execute"
}
```

## Example

To control a service named `bluetooth` (using the default topic):

```bash
# Subscribe to topic
MQTT_TOPIC="default/systemd/bluetooth" uv run mqtt_systemd_controller.py

# Send command to start service
mosquitto_pub -h broker -u user -P pass -t "default/systemd/bluetooth" -m '{"hostname": "your-hostname", "service": "bluetooth", "state": "start"}'
```

## Example systemd service to run mqtt_systemd_controller

- vi /lib/systemd/system/mqtt_systemd_controller.service

```
[Unit]
Description=MQTT Systemd monitor for Home Assistant
After=network-online.target

[Service]
WorkingDirectory=/home/<username>/
User=<username>
Group=<usergroup>
Type=simple
RemainAfterExit=true
Restart=on-failure
RestartSec=2s
StartLimitInterval=0
ExecStart=/home/<username>/.local/bin/uv run /home/<username>/mqtt_systemd_controller/mqtt_systemd_controller.py
PIDFile=/tmp/mqtt_systemd_controller.pid

# Environment variables
Environment=MQTT_USER=<username>
Environment=MQTT_PASS=<password>
Environment=MQTT_BROKER=<ip_or_hostname>
Environment=MQTT_PORT=1883
Environment=MQTT_TOPIC=default/systemd/bluetooth
Environment=POLLING_INTERVAL=30

# Logging
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

## Running example systemd service
```bash
sudo systemctl enable mqtt_systemd_controller
sudo systemctl start mqtt_systemd_controller
```

## Logging

The controller logs to stdout by default. Use `--log-file` to specify a log file for persistent logging.

## Error Handling

The system includes comprehensive error handling for:
- MQTT connection failures
- System command execution failures
- Service status check failures
- Network issues
- Race conditions

## Threading Model

The system uses multiple threads:
- Main thread: Handles MQTT message processing
- Polling thread: Periodically checks and publishes service states
- Manual change detection thread: Detects manual service changes