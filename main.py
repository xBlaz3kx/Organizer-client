import asyncio
import json
from ipaddress import ip_address
from aiofiles import open
from asyncio_mqtt import Client, MqttError
from contextlib import AsyncExitStack


class SettingsManager:
    _file = f"settings.json"

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
    async with AsyncExitStack() as stack:
        # Keep track of the asyncio tasks that we create, so that we can cancel them on exit
        tasks = set()
        stack.push_async_callback(cancel_tasks, tasks)

        if not (check_ip_address(mqtt_broker_address)):
            # if its not a valid IP address, exit
            exit(-1)
        # Connect to the MQTT broker, read from file
        client = Client(mqtt_broker_address)
        await stack.enter_async_context(client)

        # You can create any number of topic filters
        topic_filters = (
            "floors/+/humidity",
            "floors/rooftop/#"
        )
        for topic_filter in topic_filters:
            # Log all messages that matches the filter
            manager = client.filtered_messages(topic_filter)
            messages = await stack.enter_async_context(manager)
            template = f'[topic_filter="{topic_filter}"] {{}}'
            task = asyncio.create_task(log_messages(messages, template))
            tasks.add(task)

        # Messages that doesn't match a filter will get logged here
        messages = await stack.enter_async_context(client.unfiltered_messages())
        task = asyncio.create_task(log_messages(messages, "[unfiltered] {}"))
        tasks.add(task)

        # Subscribe to topic
        await client.subscribe(f"/device/{device_id}/deviceShelves")

        # Request shelves of the device
        # After response is gotten, listen to all the paths
        task = asyncio.create_task(post_to_topic(client, "/devices/requestDeviceShelves/", ""))
        tasks.add(task)
        while True:
            await asyncio.gather(*tasks)


async def post_to_topic(client: Client, topic: str, message: str):
    await client.publish(topic, message, qos=1)


async def log_messages(messages, template):
    async for message in messages:
        print(template.format(message.payload.decode()))


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
