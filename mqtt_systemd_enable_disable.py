#!/usr/bin/env python3
"""
MQTT-based systemd service enable/disable controller
This script handles only enable/disable commands for systemd services
"""
import logging
import os
import json
import socket
import random
import signal
import sys
import time
from typing import Dict, Tuple, Any, Optional
from threading import Thread, Lock
from subprocess import PIPE, Popen
import shlex
from paho.mqtt import client as mqtt_client

# Configure logging with file output for debugging
def setup_logging(debug=False, log_file: Optional[str] = None):
    level = logging.DEBUG if debug else logging.INFO
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger("EnableDisableController")

class EnableDisableController:
    """MQTT-based systemd service enable/disable controller with robust error handling"""

    def __init__(self, debug=False, log_file: Optional[str] = None):
        # Configuration from environment variables
        self.broker = os.getenv('MQTT_BROKER')
        self.port = int(os.getenv('MQTT_PORT', 1883))
        self.topic = os.getenv('MQTT_TOPIC')
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.username = os.getenv('MQTT_USER')
        self.password = os.getenv('MQTT_PASS')
        self.polling_interval = int(os.getenv('POLLING_INTERVAL', 30))
        self.manual_check_interval = int(os.getenv('MANUAL_CHECK_INTERVAL', 5))
        self.message_deduplication_window = float(os.getenv('MESSAGE_DEDUPLICATION_WINDOW', 0.1))
        self.debug = debug
        self.log_file = log_file

        # Parse multiple topics from comma-delimited environment variable
        self.topics = [topic.strip() for topic in self.topic.split(',')] if ',' in self.topic else [self.topic]

        # Validate required environment variables
        if not self.username or not self.password or not self.broker:
            logger.error("MQTT_USER, MQTT_PASS, MQTT_BROKER, and MQTT_TOPIC must be set in environment variables")
            sys.exit(1)

        logger.info(f"Configuration loaded - Broker: {self.broker}:{self.port}")
        logger.info(f"Topics: {self.topics}")
        logger.info(f"Hostname: {socket.gethostname()}")
        logger.info(f"Polling interval: {self.polling_interval}s")
        logger.info(f"Manual check interval: {self.manual_check_interval}s")

        # Service state tracking - dynamic based on topics
        self.services: Dict[str, str] = {}
        self.changed: Dict[str, bool] = {}
        self.last_published_state: Dict[str, str] = {}
        self.last_systemd_check: Dict[str, float] = {}
        self.message_count = 0
        self.last_message_time = 0
        self.last_processed_message = {}  # Track last processed message to avoid reprocessing
        self.message_deduplication_cache = {}  # Track recent messages for deduplication

        # Initialize service tracking for each topic
        for topic in self.topics:
            # Extract service name from topic (last part after /)
            service_name = topic.split('/')[-1] if topic else 'unknown'
            self.services[service_name] = ""
            self.changed[service_name] = False
            self.last_published_state[service_name] = ""
            self.last_systemd_check[service_name] = 0.0

        # Threading synchronization
        self.lock = Lock()
        self.polling_thread = None
        self.manual_change_thread = None
        self.stop_polling = False
        self.stop_manual_check = False

        # MQTT client
        self.client: mqtt_client.Client = None

        # Circuit breaker for MQTT connection
        self.mqtt_connection_failure_count = 0
        self.max_mqtt_failures = 3
        self.mqtt_last_failure_time = 0

    def connect_mqtt(self) -> mqtt_client.Client:
        """Connect to MQTT broker with proper reconnection handling and error recovery"""
        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                logger.info("Connected to MQTT Broker!")
                self.mqtt_connection_failure_count = 0  # Reset failure counter on success
                # Subscribe to all topics
                for topic in self.topics:
                    try:
                        result = client.subscribe(topic, 0)
                        logger.info(f"Subscribed to topic: {topic}, result: {result}")
                    except Exception as e:
                        logger.error(f"Failed to subscribe to topic {topic}: {e}")
                        return
            else:
                logger.error("Failed to connect, reason code %d", reason_code)
                self.mqtt_connection_failure_count += 1
                if self.mqtt_connection_failure_count >= self.max_mqtt_failures:
                    logger.critical(f"Max MQTT connection failures reached ({self.max_mqtt_failures}), exiting")
                    sys.exit(1)
                else:
                    logger.warning(f"MQTT connection failed, attempt {self.mqtt_connection_failure_count}/{self.max_mqtt_failures}")
                return

        def on_disconnect(client, userdata, flags, reason_code, properties):
            logger.info("Disconnected from MQTT Broker, reason code: %s", reason_code)
            self.mqtt_connection_failure_count += 1
            if reason_code != 0:
                logger.warning("Unexpected MQTT disconnection, will attempt reconnection")

        def on_subscribe(client, userdata, mid, reason_code, properties):
            logger.info(f"Subscribed successfully, mid: {mid}, reason_code: {reason_code}")

        def on_message(client, userdata, msg):
            # Check if this is a duplicate message by checking timestamp
            current_time = time.time()

            # Enhanced deduplication using message hash
            message_hash = f"{msg.topic}_{msg.payload.decode()}_{current_time}"
            if message_hash in self.message_deduplication_cache:
                logger.debug("Ignoring duplicate message (hash)")
                return

            # Add to deduplication cache with expiration
            self.message_deduplication_cache[message_hash] = current_time
            # Clean up old entries
            expired = [k for k, v in self.message_deduplication_cache.items() if current_time - v > 1.0]
            for key in expired:
                del self.message_deduplication_cache[key]

            # Check timestamp-based deduplication
            if current_time - self.last_message_time < self.message_deduplication_window:  # Ignore messages within deduplication window
                logger.debug("Ignoring duplicate message (timestamp)")
                return

            self.last_message_time = current_time

            logger.debug(f"=== MESSAGE RECEIVED ===")
            logger.debug(f"Topic: {msg.topic}")
            logger.debug(f"Payload: {msg.payload.decode()}")
            logger.debug(f"Message size: {len(msg.payload)} bytes")

            try:
                msg_json = json.loads(msg.payload.decode())
                logger.debug(f"Parsed JSON: {msg_json}")
            except json.JSONDecodeError as e:
                logger.error("Failed to decode JSON: %s", e)
                return

            # Check if this is a message we just published (avoid self-loop)
            # We'll use a more robust approach: check if it's a state update with same service and state
            if msg_json.get('service') and msg_json.get('state'):
                service = msg_json['service']
                state = msg_json['state']

                # Check if this message matches our last published state for this service
                with self.lock:
                    if service in self.last_published_state:
                        if self.last_published_state[service] == state:
                            # This is likely a self-published message, ignore it
                            logger.debug(f"Ignoring self-published message for {service} with state {state}")
                            return

            # Increment message counter
            with self.lock:
                self.message_count += 1
                logger.debug(f"Message #{self.message_count} received")

            # Check hostname
            if msg_json.get('hostname'):
                logger.debug(f"Message hostname: {msg_json['hostname']}")
                logger.debug(f"Local hostname: {socket.gethostname()}")
                if msg_json['hostname'] != socket.gethostname():
                    logger.debug(f"Message not for this host: {msg_json['hostname']}")
                    return
            else:
                logger.warning("No hostname in message")
                return

            # Handle service state changes for enable/disable commands only
            if msg_json.get('service') and msg_json.get('state') in ['enable', 'disable', 'enabled', 'disabled']:
                service = msg_json['service']

                # Convert state to command format for internal processing
                # If we receive "enabled" or "disabled", convert to "enable" or "disable" for command execution
                state = msg_json['state']
                if msg_json['state'] == 'enabled':
                    state = 'enable'
                elif msg_json['state'] == 'disabled':
                    state = 'disable'

                logger.info(f"Processing service enable/disable state change: {service} -> {state}")

                # Update service state - always set changed to True for new commands
                with self.lock:
                    # Ensure service exists in our tracking
                    if service not in self.services:
                        self.services[service] = ""
                        self.changed[service] = False
                        self.last_published_state[service] = ""
                        self.last_systemd_check[service] = 0.0

                    self.services[service] = state
                    self.changed[service] = True  # Always set to True when new command arrives

                logger.info(f"Service {service} state set to {state} and changed to {self.changed[service]}")

                # Immediately check and apply the new state
                self.check_systemd_status(client)
            else:
                # Filter out messages that don't match our expected format
                if msg_json.get('service') and msg_json.get('state') not in ['enable', 'disable', 'enabled', 'disabled']:
                    pass
                else:
                    logger.info("Message doesn't match expected format")
                    logger.info("Message keys: " + str(msg_json.keys()) if isinstance(msg_json, dict) else "Not a dict")

        def on_log(client, userdata, level, buf):
            if self.debug:
                logger.debug(f"MQTT Log: {buf}")

        client = mqtt_client.Client(
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            protocol=mqtt_client.MQTTv311
        )
        client.username_pw_set(self.username, self.password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_log = on_log

        try:
            client.connect(self.broker, self.port)
            return client
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self.mqtt_connection_failure_count += 1
            if self.mqtt_connection_failure_count >= self.max_mqtt_failures:
                logger.critical(f"Max MQTT connection failures reached ({self.max_mqtt_failures}), exiting")
                sys.exit(1)
            else:
                logger.warning(f"MQTT connection failed, will retry")
                return None

    def run_cmd(self, cmd: list) -> Tuple[bytes, bytes, int]:
        """Execute a command and return output, error, and return code with comprehensive error handling"""
        try:
            if self.debug:
                logger.debug(f"Running command: {' '.join(cmd)}")
            child = Popen(cmd, stdout=PIPE, stderr=PIPE)
            out, err = child.communicate()
            rc = child.returncode

            # systemctl is-active returns 0 for active, 1 for inactive, 3 for failed
            # We want to handle all cases properly
            if self.debug:
                logger.debug(f"Command finished - rc={rc}, out={out.decode()[:100]}..., err={err.decode()[:100]}...")
            return out, err, rc
        except Exception as e:
            logger.error("Command execution failed: %s", e)
            return b'', b'', 1

    def _get_systemd_enabled_status(self, service: str) -> Tuple[bool, int]:
        """Get systemd enabled status for a service (True = enabled, False = disabled) with error handling"""
        systemd_command = ["/usr/bin/systemctl", "is-enabled", "--quiet", service]
        try:
            if self.debug:
                logger.debug(f"Checking systemd enabled status for {service}")
            child = Popen(systemd_command, stdout=PIPE, stderr=PIPE)
            out, err = child.communicate()
            rc = child.returncode

            # systemctl is-enabled --quiet returns:
            # 0 = enabled
            # 1 = disabled
            # We consider 0 as "enabled" and 1 as "disabled"
            is_enabled = (rc == 0)
            if self.debug:
                logger.debug(f"Systemd enabled status for {service}: rc={rc}, is_enabled={is_enabled}")
            return is_enabled, rc
        except Exception as e:
            logger.error("Failed to get systemd enabled status for %s: %s", service, e)
            return False, 1

    def check_systemd_status(self, client: mqtt_client.Client):
        """Check systemd status and apply desired enable/disable state changes with enhanced error handling"""
        for service in list(self.services.keys()):
            # Skip services that don't have any topics associated with them
            # This is a safety check, but should not be needed with our initialization
            if service not in self.services:
                continue

            # Find the topic for this service (based on service name)
            topic = None
            for t in self.topics:
                if t.split('/')[-1] == service:
                    topic = t
                    break

            if not topic:
                # Fallback to default topic pattern
                topic = f'picam/systemd/{service}'

            # Get desired state and changed flag inside lock
            with self.lock:
                desired_state = self.services[service]
                changed = self.changed[service]

            # If no change is needed, skip
            if not changed:
                if self.debug:
                    logger.debug(f"No changes needed for {service}")
                continue

            # For enable/disable commands, we need to check the enabled status
            if desired_state in ['enable', 'disable']:
                is_enabled, rc = self._get_systemd_enabled_status(service)
                if self.debug:
                    logger.debug(f"Current enabled status for {service}: is_enabled={is_enabled}, rc={rc}")

                # Apply desired state based on current enabled status
                if desired_state == 'enable' and not is_enabled:
                    # Service is disabled, should be enabled
                    self._apply_service_state(client, service, 'enable', topic)
                elif desired_state == 'disable' and is_enabled:
                    # Service is enabled, should be disabled
                    self._apply_service_state(client, service, 'disable', topic)
                elif desired_state == 'enable' and is_enabled:
                    # Service is already enabled
                    with self.lock:
                        self.changed[service] = False
                    if self.debug:
                        logger.info(f"Service {service} already enabled, no action needed")
                elif desired_state == 'disable' and not is_enabled:
                    # Service is already disabled
                    with self.lock:
                        self.changed[service] = False
                    if self.debug:
                        logger.info(f"Service {service} already disabled, no action needed")

    def _apply_service_state(self, client: mqtt_client.Client, service: str, desired_state: str, topic: str):
        """Apply the desired service enable/disable state with enhanced error handling"""
        # For enable/disable commands, we need to check current enabled status first
        if desired_state in ['enable', 'disable']:
            is_enabled, rc = self._get_systemd_enabled_status(service)
            if self.debug:
                logger.debug(f"Current enabled status for {service}: is_enabled={is_enabled}, rc={rc}")

            # If we're trying to enable a service that's already enabled, or disable one that's already disabled
            if (desired_state == 'enable' and is_enabled) or (desired_state == 'disable' and not is_enabled):
                with self.lock:
                    self.changed[service] = False
                if self.debug:
                    logger.info(f"Service {service} already {desired_state}d, no action needed")
                return

        systemd_command = ["/usr/bin/sudo", "/usr/bin/systemctl", desired_state, service]

        try:
            logger.info(f"Applying {desired_state} to service {service}")
            out, err, rc = self.run_cmd(systemd_command)
            logger.info(f"{desired_state} command for {service}: rc={rc}")

            # Check if command was successful
            if rc != 0:
                logger.warning(f"Systemctl command failed for {service} with rc={rc}")
                # Even if command failed, we still mark as processed to avoid infinite retries
                # In a real system, you might want to implement retry logic here
        except Exception as e:
            logger.error(f"Failed to {desired_state} service {service}: {e}")
            return

        # Mark as processed
        with self.lock:
            self.changed[service] = False

        # Publish status update
        if desired_state in ['enable', 'disable']:
            state = "enabled" if desired_state == "enable" else "disabled"
        else:
            state = desired_state

        msg = json.dumps({
            "hostname": socket.gethostname(),
            "service": service,
            "state": state,
            "command": ""
        })

        try:
            client.publish(topic, msg)
            logger.info(f"Published `{msg}` to `{topic}` topic")
        except Exception as e:
            logger.error(f"Failed to publish to MQTT topic {topic}: {e}")

        # Update last published state
        with self.lock:
            self.last_published_state[service] = state

    def _polling_worker(self, client: mqtt_client.Client):
        """Background thread for periodic state polling with enhanced error handling"""
        logger.info(f"Starting polling worker with interval {self.polling_interval} seconds")
        while not self.stop_polling:
            try:
                self._publish_current_states(client)
                time.sleep(self.polling_interval)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(self.polling_interval)

    def _publish_current_states(self, client: mqtt_client.Client):
        """Publish current state of all services with enhanced error handling"""
        for service in list(self.services.keys()):
            # Find the topic for this service (based on service name)
            topic = None
            for t in self.topics:
                if t.split('/')[-1] == service:
                    topic = t
                    break

            if not topic:
                # Fallback to default topic pattern
                topic = f'default/systemd/{service}'

            # Get current systemd status
            is_enabled, rc = self._get_systemd_enabled_status(service)
            current_state = "enabled" if is_enabled else "disabled"

            # Only publish if state has changed
            with self.lock:
                if self.last_published_state.get(service) != current_state:
                    msg = json.dumps({
                        "hostname": socket.gethostname(),
                        "service": service,
                        "state": current_state,
                        "command": ""
                    })

                    try:
                        client.publish(topic, msg)
                        logger.info(f"Published current state: {msg}")

                        # Update last published state
                        self.last_published_state[service] = current_state
                    except Exception as e:
                        logger.error(f"Failed to publish to MQTT topic {topic}: {e}")
                else:
                    if self.debug:
                        logger.debug(f"State unchanged for {service}, not publishing")

    def _detect_manual_changes(self, client: mqtt_client.Client):
        """Detect manual changes to service states and publish updates with enhanced error handling"""
        for service in list(self.services.keys()):
            # Find the topic for this service (based on service name)
            topic = None
            for t in self.topics:
                if t.split('/')[-1] == service:
                    topic = t
                    break

            if not topic:
                # Fallback to default topic pattern
                topic = f'picam/systemd/{service}'

            # Get current systemd status
            is_enabled, rc = self._get_systemd_enabled_status(service)
            current_state = "enabled" if is_enabled else "disabled"

            # Check if manual change occurred
            with self.lock:
                # If we have a last known state and it's different, publish update
                if service in self.last_published_state:
                    if self.last_published_state[service] != current_state:
                        logger.info(f"Manual state change detected for {service}: {self.last_published_state[service]} -> {current_state}")

                        msg = json.dumps({
                            "hostname": socket.gethostname(),
                            "service": service,
                            "state": current_state,
                            "command": ""
                        })

                        try:
                            client.publish(topic, msg)
                            logger.info(f"Published manual change: {msg}")

                            # Update last published state
                            self.last_published_state[service] = current_state
                        except Exception as e:
                            logger.error(f"Failed to publish manual change to MQTT topic {topic}: {e}")

    def initial_systemd_check(self, client: mqtt_client.Client):
        """Perform initial systemd status check with enhanced error handling"""
        logger.info("--- Performing initial systemd status check ---")
        try:
            self.check_systemd_status(client)
        except Exception as e:
            logger.error(f"Initial systemd check failed: {e}")

    def run(self):
        """Main execution loop with enhanced error handling and graceful shutdown"""
        # Setup signal handling for graceful shutdown
        def signal_handler(sig, frame):
            logger.info('Shutting down gracefully...')
            self.stop_polling = True
            self.stop_manual_check = True

            # Wait for threads to finish gracefully
            if self.polling_thread:
                self.polling_thread.join(timeout=5)
            if self.manual_change_thread:
                self.manual_change_thread.join(timeout=5)

            if self.client:
                try:
                    self.client.disconnect()
                except Exception as e:
                    logger.error(f"Error during MQTT disconnect: {e}")

            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Connect to MQTT
        self.client = self.connect_mqtt()

        if not self.client:
            logger.critical("Failed to establish MQTT connection, exiting")
            sys.exit(1)

        # Perform initial check
        self.initial_systemd_check(self.client)

        # Start polling thread
        self.polling_thread = Thread(target=self._polling_worker, args=(self.client,))
        self.polling_thread.daemon = True
        self.polling_thread.start()

        # Start monitoring for manual changes in a separate thread
        def manual_change_monitor():
            while not self.stop_manual_check:
                try:
                    self._detect_manual_changes(self.client)
                    time.sleep(self.manual_check_interval)
                except Exception as e:
                    logger.error(f"Manual change detection error: {e}")
                    time.sleep(self.manual_check_interval)

        self.manual_change_thread = Thread(target=manual_change_monitor)
        self.manual_change_thread.daemon = True
        self.manual_change_thread.start()

        # Start listening for messages
        logger.info("Starting MQTT listener...")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            signal_handler(signal.SIGINT, None)
        except Exception as e:
            logger.error(f"MQTT loop error: {e}")
            signal_handler(signal.SIGTERM, None)

def main():
    """Main entry point"""
    # Parse command line arguments
    debug = '--debug' in sys.argv
    log_file = None

    # Check for log file argument
    for i, arg in enumerate(sys.argv):
        if arg == '--log-file' and i + 1 < len(sys.argv):
            log_file = sys.argv[i + 1]
            break

    global logger
    logger = setup_logging(debug, log_file)
    logger.info("Starting Enable/Disable Controller")

    try:
        controller = EnableDisableController(debug, log_file)
        controller.run()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()