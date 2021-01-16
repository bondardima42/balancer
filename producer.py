import time
import uuid
import gmqtt
import asyncio

from utils.utils import run_event_loop, STOP, init_client
from utils.consts import BALANCER_TOPIC


async def send_messages(client):
    """
    Send messages to balancer
    """
    while True:
        if STOP.is_set():
            break

        payload = str(time.time())
        client.publish(BALANCER_TOPIC, payload, qos=1)
        print('Producer. Publish.', f"Topic: '{BALANCER_TOPIC}'.", 'Payload', payload)
        await asyncio.sleep(1)


async def main(broker_host, token):
    client_id = f"producer-{uuid.uuid4().hex}"
    client = gmqtt.Client(client_id)
    client = await init_client(client, broker_host, token)

    asyncio.ensure_future(send_messages(client))
    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    run_event_loop(main)
