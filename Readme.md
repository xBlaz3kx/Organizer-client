# StockUp Organizer MQTT Python client for Raspberry Pi

## About

StockUp Organizer MQTT Python client is a Raspberry Pi client, that indicates the location of a container in the
Organizer ecosystem.

The client listens to incoming requests from the MQTT broker and manipulates the **WS281x** addressable RGB LED strip
according to given instructions. The logic is handled by the web server, and the client is responsible solely for
indication.

The Python client uses **asyncio-mqtt** library for asynchronous communication and RPI_WS281x library for RGB LED strip.
It is configured through a JSON file.

## Configuration

1. Edit the existing settings.json file with your values

```
{
  "info": {
    "mqtt_broker_ip": "192.168.1.10",
    "retry_interval": 3,
    "device_id": "123abc"
  }
}
```

The **MQTT broker IP** attribute should be a valid IP address, or a domain name. The Device (Raspberry Pi) should be
registered and enabled in the web server, and the MQTT broker bundled with the server should be up and running.

The client will ask for all shelves assigned to it. The MQTT broker will notify the device of it's assigned shelves even
after it has received the initial response.

## Running the client

### Option 1: Without Docker

1. Install necessary dependencies:

   > sudo pip3 install -r requirements.txt

2. If configured properly, the program will work:

   > sudo python3 main.py

### Option 2: With Docker

1. Build the image:

   > docker build -t organizer-python-client .

2. Run the container from built image:

   > docker run --device /dev/ttyAMA0:/dev/ttyAMA0 --device /dev/mem:/dev/mem --privileged organizer-python-client

Underlying libraries (rpi_WS218x) need sudo privileges and access to device memory.