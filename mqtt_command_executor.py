#!/usr/bin/env python3
"""
MQTT-based command executor
Listens for JSON payloads containing a "command" field and executes them on the host
if the destination hostname matches the local hostname.
"""
import logging
import os
import json
import socket
import random
import signal
import sys
import time
from typing import Optional
from threading import Thread
from subprocess import PIPE, Popen
import shlex
from paho.mqtt import client as mqtt_client

# INFO-level messages that should always appear, even in quiet (non-verbose) mode.
_IMPORTANT_INFO = [
    "Connected to MQTT Broker",
    "Disconnected from MQTT Broker",
    "Executing command",
    "Command result",
]


class AlwaysShowImportantFilter(logging.Filter):
    """Let WARNING+ through always, plus INFO messages that look important."""

    def filter(self, record):
        if record.levelno >= logging.WARNING:
            return True
        if record.levelno >= logging.INFO:
            for keyword in _IMPORTANT_INFO:
                if keyword in record.getMessage():
                    return True
        return record.levelno >= self._min_level

    def __init__(self, min_level):
        super().__init__()
        self._min_level = min_level


def setup_logging(debug=False, verbose=False, log_file: Optional[str] = None):
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    stream = logging.StreamHandler(sys.stdout)
    stream.addFilter(AlwaysShowImportantFilter(level))

    handlers = [stream]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    # Set the root level to DEBUG so all records reach handlers — the handler
    # filter (AlwaysShowImportantFilter) is what actually gates output.
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger("CommandExecutor")


class CommandExecutor:
    """MQTT-based command executor that runs arbitrary shell commands"""

    def __init__(self, debug=False, log_file: Optional[str] = None):
        # Configuration from environment variables
        self.broker = os.getenv('MQTT_BROKER')
        self.port = int(os.getenv('MQTT_PORT', 1883))
        self.topic = os.getenv('MQTT_TOPIC')
        self.client_id = f'cmd-exec-{random.randint(0, 10000)}'
        self.username = os.getenv('MQTT_USER')
        self.password = os.getenv('MQTT_PASS')
        self.message_deduplication_window = float(os.getenv('MESSAGE_DEDUPLICATION_WINDOW', 0.1))
        self.debug = debug
        self.log_file = log_file

        # Parse multiple topics from comma-delimited environment variable
        self.topics = [
            topic.strip() for topic in self.topic.split(',')
        ] if self.topic and ',' in self.topic else [self.topic] if self.topic else []

        # Validate required environment variables
        if not self.username or not self.password or not self.broker:
            logger.error("MQTT_USER, MQTT_PASS, MQTT_BROKER, and MQTT_TOPIC must be set in environment variables")
            sys.exit(1)

        logger.info(f"Configuration loaded - Broker: {self.broker}:{self.port}")
        logger.info(f"Topics: {self.topics}")
        logger.info(f"Hostname: {socket.gethostname()}")

        # Message deduplication
        self.message_deduplication_cache = {}
        self.last_message_time = 0
        self.message_count = 0

        # MQTT client
        self.client: mqtt_client.Client = None

        # Shutdown control
        self._shutdown_requested = False

        # Circuit breaker for MQTT connection
        self.mqtt_connection_failure_count = 0
        self.max_mqtt_failures = 3

    def connect_mqtt(self) -> mqtt_client.Client:
        """Connect to MQTT broker with reconnection handling"""
        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                logger.info("Connected to MQTT Broker!")
                self.mqtt_connection_failure_count = 0
                for topic in self.topics:
                    try:
                        result = client.subscribe(topic, 0)
                        logger.info(f"Subscribed to topic: {topic}, result: {result}")
                    except Exception as e:
                        logger.error(f"Failed to subscribe to topic {topic}: {e}")
            else:
                logger.error("Failed to connect, reason code %d", reason_code)
                self.mqtt_connection_failure_count += 1
                if self.mqtt_connection_failure_count >= self.max_mqtt_failures:
                    logger.critical(f"Max MQTT connection failures reached ({self.max_mqtt_failures}), exiting")
                    sys.exit(1)
                else:
                    logger.warning(
                        f"MQTT connection failed, attempt {self.mqtt_connection_failure_count}/{self.max_mqtt_failures}"
                    )

        def on_disconnect(client, userdata, flags, reason_code, properties):
            logger.info("Disconnected from MQTT Broker, reason code: %s", reason_code)
            self.mqtt_connection_failure_count += 1
            if reason_code != 0:
                logger.warning("Unexpected MQTT disconnection, will attempt reconnection")

        def on_subscribe(client, userdata, mid, reason_code, properties):
            logger.info(f"Subscribed successfully, mid: {mid}, reason_code: {reason_code}")

        def on_message(client, userdata, msg):
            current_time = time.time()

            # Timestamp-based deduplication
            if current_time - self.last_message_time < self.message_deduplication_window:
                logger.debug("Ignoring duplicate message (timestamp)")
                return

            # Hash-based deduplication
            message_hash = f"{msg.topic}_{msg.payload.decode()}_{int(current_time * 1000)}"
            if message_hash in self.message_deduplication_cache:
                logger.debug("Ignoring duplicate message (hash)")
                return

            self.message_deduplication_cache[message_hash] = current_time
            # Clean up old entries
            expired = [
                k for k, v in self.message_deduplication_cache.items()
                if current_time - v > 1.0
            ]
            for key in expired:
                del self.message_deduplication_cache[key]

            self.last_message_time = current_time
            self.message_count += 1

            logger.debug(f"=== MESSAGE #{self.message_count} RECEIVED ===")
            logger.debug(f"Topic: {msg.topic}")
            logger.debug(f"Payload: {msg.payload.decode()}")

            try:
                msg_json = json.loads(msg.payload.decode())
            except json.JSONDecodeError as e:
                logger.error("Failed to decode JSON: %s", e)
                return

            # Check hostname match
            msg_hostname = msg_json.get('hostname')
            if not msg_hostname:
                logger.warning("No hostname in message, ignoring")
                return

            local_hostname = socket.gethostname()
            logger.debug(f"Message hostname: {msg_hostname}, Local hostname: {local_hostname}")

            if msg_hostname != local_hostname:
                logger.debug(f"Message not for this host: {msg_hostname}")
                return

            # Extract and execute command
            command = msg_json.get('command')
            if not command:
                logger.debug("No command in message, ignoring")
                return

            self._execute_command(client, msg.topic, command)

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
                logger.warning("MQTT connection failed, will retry")
                return None

    def _execute_command(self, client, topic: str, command: str):
        """Execute a shell command and optionally publish results"""
        try:
            logger.info(f"Executing command: {command}")
            cmd_parts = shlex.split(command)
            if self.debug:
                logger.debug(f"Running command: {' '.join(cmd_parts)}")

            child = Popen(cmd_parts, stdout=PIPE, stderr=PIPE)
            out, err = child.communicate(timeout=300)  # 5 minute timeout
            rc = child.returncode

            stdout_text = out.decode('utf-8', errors='replace').strip()
            stderr_text = err.decode('utf-8', errors='replace').strip()

            logger.info(
                f"Command result - rc={rc}, out={stdout_text[:200]}..., err={stderr_text[:200]}..."
            )

            # Publish result back to the same topic
            result = json.dumps({
                "hostname": socket.gethostname(),
                "command": command,
                "return_code": rc,
                "stdout": stdout_text,
                "stderr": stderr_text
            })

            try:
                client.publish(topic, result)
                logger.info(f"Published command result to {topic}")
            except Exception as e:
                logger.error(f"Failed to publish result: {e}")

        except Exception as e:
            logger.error(f"Command execution failed: {e}")

    def _shutdown(self):
        """Perform graceful shutdown — must be called from the main thread."""
        logger.info('Shutting down gracefully...')
        if self.client:
            try:
                self.client.disconnect()
            except Exception as e:
                logger.error(f"Error during MQTT disconnect: {e}")

    def run(self):
        """Main execution loop with graceful shutdown"""
        def signal_handler(sig, frame):
            self._shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Connect to MQTT
        self.client = self.connect_mqtt()

        if not self.client:
            logger.critical("Failed to establish MQTT connection, exiting")
            sys.exit(1)

        # Use loop_start() so the main thread remains responsive to signals.
        # loop_forever() blocks in select() and can swallow SIGINT/SIGTERM.
        logger.info("Starting MQTT listener...")
        self.client.loop_start()
        try:
            while not self._shutdown_requested:
                time.sleep(1)
        finally:
            self._shutdown()


def main():
    """Main entry point"""
    debug = '--debug' in sys.argv
    verbose = '--verbose' in sys.argv
    log_file = None

    for i, arg in enumerate(sys.argv):
        if arg == '--log-file' and i + 1 < len(sys.argv):
            log_file = sys.argv[i + 1]
            break

    global logger
    logger = setup_logging(debug, verbose, log_file)
    logger.info("Starting Command Executor")

    try:
        controller = CommandExecutor(debug, log_file)
        controller.run()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
