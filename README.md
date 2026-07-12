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

```bash
pip install paho-mqtt
```

## Configuration

Set the following environment variables:

- `MQTT_BROKER` - MQTT broker address (default: 192.168.7.50)
- `MQTT_PORT` - MQTT broker port (default: 1883)
- `MQTT_USER` - MQTT username
- `MQTT_PASS` - MQTT password
- `MQTT_TOPIC` - MQTT topic(s) to subscribe to (comma-separated for multiple topics)
- `POLLING_INTERVAL` - Polling interval in seconds (default: 30)
- `MANUAL_CHECK_INTERVAL` - Manual change detection interval in seconds (default: 5)
- `MESSAGE_DEDUPLICATION_WINDOW` - Message deduplication window in seconds (default: 0.1)

## Usage

```bash
python mqtt_systemd_controller.py [--debug] [--log-file /path/to/logfile]
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

To control a service named `mediamtx`:

```bash
# Subscribe to topic
MQTT_TOPIC="picam/systemd/mediamtx" python mqtt_systemd_controller.py

# Send command to start service
mosquitto_pub -h broker -u user -P pass -t "picam/systemd/mediamtx" -m '{"hostname": "your-hostname", "service": "mediamtx", "state": "start"}'
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