import asyncio
import json
import os
from ipaddress import ip_address
from aiofiles import open
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack
from rpi_ws21x import PixelStrip, Color

all_monitored_topics = []


class LEDStrip:
    LED_PIN = 18
    LED_FREQ_HZ = 800000
    LED_DMA = 10
    LED_BRIGHTNESS = 255
    LED_INVERT = False
    LED_CHANNEL = 0  # set to '1' for GPIOs 13, 19, 41, 45 or 53

    def __init__(self, num_leds):
        self.LED_COUNT = num_leds
        self.strip = PixelStrip(self.LED_COUNT, LEDStrip.LED_PIN, LEDStrip.LED_FREQ_HZ, LEDStrip.LED_DMA,
                                LEDStrip.LED_BRIGHTNESS, LEDStrip.LED_INVERT,
                                LEDStrip.LED_CHANNEL)

        self.strip.begin()

    def initCycle(self, iterations=1):
        for j in range(256 * iterations):
            for i in range(self.strip.numPixels()):
                self.strip.setPixelColor(i, self._wheel((int(i * 256 / self.strip.numPixels()) + j) & 255))
            self.strip.show()

    def _wheel(self, pos):
        if pos < 85:
            return Color(pos * 3, 255 - pos * 3, 0)
        elif pos < 170:
            pos -= 85
            return Color(255 - pos * 3, 0, pos * 3)
        else:
            pos -= 170
            return Color(0, pos * 3, 255 - pos * 3)

    async def blink(self, index: int, duration: int, color):
        for i in range(0, 4):
            if i % 2 == 0:
                self.strip.setPixelColor(index, color)
            else:
                self.strip.setPixelColor(index, 0)
            self.strip.show()
            await asyncio.sleep(duration)

    async def blink_interval(self, index: int, duration: int, color):
        self.strip.setPixelColor(index, color)
        self.strip.show()
        await asyncio.sleep(duration)

    def static(self, index: int, color):
        self.strip.setPixelColor(index, color)
        self.strip.show()

    def off(self, index: int):
        self.strip.setPixelColor(index, 0)
        self.strip.show()


ledStrip: LEDStrip = None


class SettingsManager:
    _filepath = os.path.dirname(os.path.abspath(__file__))
    _file = f"{_filepath}/settings.json"

    @staticmethod
    async def read_settings():
        async with open(SettingsManager._file, "r") as settings_file:
            data: dict = json.loads(await settings_file.read())
            await settings_file.close()
            return data

    @staticmethod
    async def write_to_settings(data):
        async with open(SettingsManager._file, "w") as settings_file:
            await settings_file.write(json.dumps(data))
            await settings_file.close()


def check_ip_address(address):
    try:
        ip_address(address)
        return True
    except ValueError:
        return False


async def connect_to_broker(mqtt_broker_address: str, device_id: str):
    if not (check_ip_address(mqtt_broker_address)):
        # if its not a valid IP address, exit
        exit(-1)
    async with AsyncExitStack() as stack:
        # Keep track of the asyncio tasks that we create, so that we can cancel them on exit
        tasks = set()
        stack.push_async_callback(cancel_tasks, tasks)
        # Connect to the MQTT broker
        client = Client(mqtt_broker_address)
        await stack.enter_async_context(client)
        # Listen only to these topics
        topic_filters = (
            "/device/+/deviceShelves",
            "/sector/+/rack/+/shelf/+/#"
        )
        for topic_filter in topic_filters:
            await add_topic_filter(client, stack, topic_filter, tasks)
        # Subscribe to topic that responds with all the shelves of the device
        await client.subscribe(f"/device/{device_id}/deviceShelves")
        # Request shelves assigned to the device
        task = asyncio.create_task(
            post_to_topic(client, "/devices/requestDeviceShelves/", json.dumps({"deviceId": device_id})))
        tasks.add(task)
        await asyncio.gather(*tasks)


async def post_to_topic(client: Client, topic: str, message: str):
    await client.publish(topic, message, qos=1)


async def add_topic_filter(client: Client, stack: AsyncExitStack, topic_filter: str, tasks):
    manager = client.filtered_messages(topic_filter)
    messages = await stack.enter_async_context(manager)
    task = asyncio.create_task(handle_messages(client, messages))
    tasks.add(task)


async def handle_messages(client, messages):
    global all_monitored_topics
    async for message in messages:
        json_message: dict = json.loads(message.payload.decode())
        print(json_message)
        if message.topic == f"/device/{device_id}/deviceShelves":
            create_LEDStrip(len(json_message["shelves"]))
            for shelf in json_message["shelves"]:
                shelf_id = shelf["shelfId"]
                rack_id = shelf["rackId"]
                sector_id = shelf["sectorId"]
                topic: str = f"/sector/{shelf_id}/rack/{rack_id}/shelf/{sector_id}"
                all_monitored_topics.append(topic)
                await client.subscribe(f"{topic}/indicateLocation")
                await client.subscribe(f"{topic}/indicateEmpty")
        elif [True for topic in all_monitored_topics if f"{topic}" in message.topic]:
            container_index: int = json_message["container"]
            display_type: str = json_message["displayType"]
            display_duration: int = json_message["displayDuration"]
            color: int = json_message["color"]
            if "/indicateLocation" in message.topic:
                await indicate_location(container_index, display_type, display_duration, color)
            elif "/indicateEmpty" in message.topic:
                await indicate_location(container_index, display_type, display_duration, color)


def create_LEDStrip(num_leds: int):
    global ledStrip
    # Make the LED strip
    ledStrip = LEDStrip(num_leds)
    # Test the LEDs
    ledStrip.initCycle(2)


async def indicate_location(index: int, display_type: str, display_duration: int, color):
    if ledStrip is not None:
        if display_type == "blink":
            await ledStrip.blink(index, display_duration, color)
        elif display_type == "static":
            ledStrip.static(index, color)
        elif display_type == "blinkInterval":
            await ledStrip.blink_interval(index, display_duration, color)
        elif display_type == "off":
            ledStrip.off(index)


async def cancel_tasks(tasks):
    for task in tasks:
        if task.done():
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def main():
    global device_id
    # Read settings from configuration
    settings = await SettingsManager.read_settings()
    mqtt_broker_address: str = settings["info"]["mqtt_broker_ip"]
    device_id: str = settings["info"]["device_id"]
    reconnect_interval: int = settings["info"]["retry_interval"]
    while True:
        try:
            await connect_to_broker(mqtt_broker_address, device_id)
        except MqttError as error:
            print(f'Error "{error}". Reconnecting in {reconnect_interval} seconds.')
        finally:
            await asyncio.sleep(reconnect_interval)


if __name__ == "__main__":
    asyncio.run(main())
