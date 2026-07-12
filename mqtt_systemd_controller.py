#!/usr/bin/env python3
"""
MQTT-based systemd service controller with polling and race condition handling
"""
import logging
import os
import json
import socket
import random
import signal
import sys
import time
from typing import Dict, Tuple, Any
from threading import Thread, Lock
from subprocess import PIPE, Popen, CalledProcessError
import shlex
from paho.mqtt import client as mqtt_client

# Configure logging with file output for debugging
def setup_logging(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # logging.FileHandler('/tmp/service_controller.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class ServiceController:
    """Manages systemd services via MQTT commands with polling and race condition handling"""
    
    def __init__(self, debug=False):
        # Configuration from environment variables
        self.broker = os.getenv('MQTT_BROKER', '192.168.7.50')
        self.port = int(os.getenv('MQTT_PORT', 1883))
        self.topic = os.getenv('MQTT_TOPIC', 'picam/systemd/mediamtx')
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.username = os.getenv('MQTT_USER')
        self.password = os.getenv('MQTT_PASS')
        self.polling_interval = int(os.getenv('POLLING_INTERVAL', 30))
        self.debug = debug
        
        # Parse multiple topics from comma-delimited environment variable
        self.topics = [topic.strip() for topic in self.topic.split(',')] if ',' in self.topic else [self.topic]
        
        # Validate required environment variables
        if not self.username or not self.password:
            logger.error("MQTT_USER and MQTT_PASS must be set in environment variables")
            sys.exit(1)
        
        logger.info(f"Configuration loaded - Broker: {self.broker}:{self.port}")
        logger.info(f"Topics: {self.topics}")
        logger.info(f"Hostname: {socket.gethostname()}")
        
        # Service state tracking - dynamic based on topics
        self.services: Dict[str, str] = {}
        self.changed: Dict[str, bool] = {}
        self.last_published_state: Dict[str, str] = {}
        self.last_systemd_check: Dict[str, float] = {}
        self.message_count = 0
        self.last_message_time = 0
        self.last_processed_message = {}  # Track last processed message to avoid reprocessing
        
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
        self.stop_polling = False
        
        # MQTT client
        self.client: mqtt_client.Client = None
        
    def connect_mqtt(self) -> mqtt_client.Client:
        """Connect to MQTT broker with proper reconnection handling"""
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT Broker!")
                # Subscribe to all topics
                for topic in self.topics:
                    result = client.subscribe(topic, 0)
                    logger.info(f"Subscribed to topic: {topic}, result: {result}")
            else:
                logger.error("Failed to connect, return code %d", rc)
                sys.exit(1)
                
        def on_disconnect(client, userdata, rc):
            logger.info("Disconnected from MQTT Broker, rc: %d", rc)
            
        def on_subscribe(client, userdata, mid, granted_qos):
            logger.info(f"Subscribed successfully, mid: {mid}, qos: {granted_qos}")
            
        def on_message(client, userdata, msg):
            # Check if this is a duplicate message by checking timestamp
            current_time = time.time()
            if current_time - self.last_message_time < 0.1:  # Ignore messages within 100ms
                logger.debug("Ignoring duplicate message (timestamp)")
                return
                
            self.last_message_time = current_time
            
            logger.info(f"=== MESSAGE RECEIVED ===")
            logger.info(f"Topic: {msg.topic}")
            logger.info(f"Payload: {msg.payload.decode()}")
            logger.info(f"Message size: {len(msg.payload)} bytes")
            
            try:
                msg_json = json.loads(msg.payload.decode())
                logger.info(f"Parsed JSON: {msg_json}")
            except json.JSONDecodeError as e:
                logger.error("Failed to decode JSON: %s", e)
                return
            
            # Check if this is a message we just published (avoid self-loop)
            # We'll use a simple approach: check if it's a state update with same service and state
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
                logger.info(f"Message #{self.message_count} received")
            
            # Check hostname
            if msg_json.get('hostname'):
                logger.info(f"Message hostname: {msg_json['hostname']}")
                logger.info(f"Local hostname: {socket.gethostname()}")
                if msg_json['hostname'] != socket.gethostname():
                    logger.info(f"Message not for this host: {msg_json['hostname']}")
                    return
            else:
                logger.warning("No hostname in message")
                return
            
            # Handle command execution
            if msg_json.get('command'):
                logger.info("Processing command...")
                self._execute_command(msg_json['command'])
            
            # Handle service state changes
            elif msg_json.get('service') and msg_json.get('state') in ['start', 'stop', 'started', 'stopped']:
                service = msg_json['service']
                
                # Convert state to command format for internal processing
                # If we receive "started" or "stopped", convert to "start" or "stop" for command execution
                if msg_json['state'] == 'started':
                    state = 'start'
                elif msg_json['state'] == 'stopped':
                    state = 'stop'
                else:
                    state = msg_json['state']
                
                logger.info(f"Processing service state change: {service} -> {state}")
                
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
                logger.info("Message doesn't match expected format")
                logger.info("Message keys: " + str(msg_json.keys()) if isinstance(msg_json, dict) else "Not a dict")
        
        def on_log(client, userdata, level, buf):
            if self.debug:
                logger.debug(f"MQTT Log: {buf}")
            
        client = mqtt_client.Client(self.client_id)
        client.username_pw_set(self.username, self.password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_log = on_log
        client.connect(self.broker, self.port)
        return client
    
    def _execute_command(self, command: str):
        """Execute a shell command and log results"""
        try:
            logger.info(f"Executing command: {command}")
            out, err, rc = self.run_cmd(shlex.split(command))
            logger.info(f"Command result - rc={rc}, out={out.decode()}, err={err.decode()}")
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
    
    def run_cmd(self, cmd: list) -> Tuple[bytes, bytes, int]:
        """Execute a command and return output, error, and return code"""
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
    
    def _get_systemd_status(self, service: str) -> Tuple[bool, int]:
        """Get systemd status for a service (True = active, False = inactive)"""
        systemd_command = ["/usr/bin/systemctl", "is-active", "--quiet", service]
        try:
            if self.debug:
                logger.debug(f"Checking systemd status for {service}")
            child = Popen(systemd_command, stdout=PIPE, stderr=PIPE)
            out, err = child.communicate()
            rc = child.returncode
            
            # systemctl is-active --quiet returns:
            # 0 = active
            # 1 = inactive  
            # 3 = failed
            # We consider 1 and 3 as "inactive"
            is_active = (rc == 0)
            if self.debug:
                logger.debug(f"Systemd status for {service}: rc={rc}, is_active={is_active}")
            return is_active, rc
        except Exception as e:
            logger.error("Failed to get systemd status for %s: %s", service, e)
            return False, 1
    
    def check_systemd_status(self, client: mqtt_client.Client):
        """Check systemd status and apply desired state changes"""
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
            
            # Check current systemd status
            is_active, rc = self._get_systemd_status(service)
            if self.debug:
                logger.debug(f"Systemd status for {service}: rc={rc}, is_active={is_active}")
            
            # Get desired state and changed flag inside lock
            with self.lock:
                desired_state = self.services[service]
                changed = self.changed[service]
            
            # If no change is needed, skip
            if not changed:
                if self.debug:
                    logger.debug(f"No changes needed for {service}")
                continue
            
            # Apply desired state based on systemd status
            if is_active and desired_state == 'stop':
                # Service is running, should be stopped
                self._apply_service_state(client, service, 'stop', topic)
            elif not is_active and desired_state == 'start':
                # Service is stopped, should be started
                self._apply_service_state(client, service, 'start', topic)
            elif is_active and desired_state == 'start':
                # Service is already running
                with self.lock:
                    self.changed[service] = False
                if self.debug:
                    logger.info(f"Service {service} already running, no action needed")
            elif not is_active and desired_state == 'stop':
                # Service is already stopped
                with self.lock:
                    self.changed[service] = False
                if self.debug:
                    logger.info(f"Service {service} already stopped, no action needed")
    
    def _apply_service_state(self, client: mqtt_client.Client, service: str, desired_state: str, topic: str):
        """Apply the desired service state"""
        systemd_command = ["/usr/bin/sudo", "/usr/bin/systemctl", desired_state, service]
        
        try:
            logger.info(f"Applying {desired_state} to service {service}")
            out, err, rc = self.run_cmd(systemd_command)
            logger.info(f"{desired_state} command for {service}: rc={rc}")
        except Exception as e:
            logger.error(f"Failed to {desired_state} service {service}: {e}")
            return
        
        # Mark as processed
        with self.lock:
            self.changed[service] = False
        
        # Publish status update
        state = "started" if desired_state == "start" else "stopped"
        msg = json.dumps({
            "hostname": socket.gethostname(),
            "service": service,
            "state": state,
            "command": ""
        })
        
        client.publish(topic, msg)
        logger.info(f"Published `{msg}` to `{topic}` topic")
        
        # Update last published state
        with self.lock:
            self.last_published_state[service] = state
    
    def _polling_worker(self, client: mqtt_client.Client):
        """Background thread for periodic state polling"""
        logger.info(f"Starting polling worker with interval {self.polling_interval} seconds")
        while not self.stop_polling:
            try:
                self._publish_current_states(client)
                time.sleep(self.polling_interval)
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(self.polling_interval)
    
    def _publish_current_states(self, client: mqtt_client.Client):
        """Publish current state of all services"""
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
            is_active, rc = self._get_systemd_status(service)
            current_state = "started" if is_active else "stopped"
            
            # Only publish if state has changed
            with self.lock:
                if self.last_published_state.get(service) != current_state:
                    msg = json.dumps({
                        "hostname": socket.gethostname(),
                        "service": service,
                        "state": current_state,
                        "command": ""
                    })
                    
                    client.publish(topic, msg)
                    logger.info(f"Published current state: {msg}")
                    
                    # Update last published state
                    self.last_published_state[service] = current_state
                else:
                    if self.debug:
                        logger.debug(f"State unchanged for {service}, not publishing")
    
    def _detect_manual_changes(self, client: mqtt_client.Client):
        """Detect manual changes to service states and publish updates"""
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
            is_active, rc = self._get_systemd_status(service)
            current_state = "started" if is_active else "stopped"
            
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
                        
                        client.publish(topic, msg)
                        logger.info(f"Published manual change: {msg}")
                        
                        # Update last published state
                        self.last_published_state[service] = current_state
    
    def initial_systemd_check(self, client: mqtt_client.Client):
        """Perform initial systemd status check"""
        logger.info("--- Performing initial systemd status check ---")
        self.check_systemd_status(client)
    
    def run(self):
        """Main execution loop"""
        # Setup signal handling for graceful shutdown
        def signal_handler(sig, frame):
            logger.info('Shutting down gracefully...')
            self.stop_polling = True
            if self.polling_thread:
                self.polling_thread.join(timeout=5)
            if self.client:
                self.client.disconnect()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Connect to MQTT
        self.client = self.connect_mqtt()
        
        # Perform initial check
        self.initial_systemd_check(self.client)
        
        # Start polling thread
        self.polling_thread = Thread(target=self._polling_worker, args=(self.client,))
        self.polling_thread.daemon = True
        self.polling_thread.start()
        
        # Start monitoring for manual changes in a separate thread
        def manual_change_monitor():
            while not self.stop_polling:
                try:
                    self._detect_manual_changes(self.client)
                    time.sleep(5)  # Check every 5 seconds for manual changes
                except Exception as e:
                    logger.error(f"Manual change detection error: {e}")
                    time.sleep(5)
        
        manual_thread = Thread(target=manual_change_monitor)
        manual_thread.daemon = True
        manual_thread.start()
        
        # Start listening for messages
        logger.info("Starting MQTT listener...")
        self.client.loop_forever()

def main():
    """Main entry point"""
    # Parse command line arguments
    debug = '--debug' in sys.argv
    
    global logger
    logger = setup_logging(debug)
    
    try:
        controller = ServiceController(debug)
        controller.run()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
