import asyncio
import json
import os
from ipaddress import ip_address
from aiofiles import open
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack

all_monitored_topics = []


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
                pass
            elif "/indicateEmpty" in message.topic:
                pass


async def indicate_empty():
    pass


async def indicate_location():
    pass


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
