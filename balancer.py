import asyncio
import time
import json
import gmqtt

from utils.utils import run_event_loop, STOP, init_client
from utils.consts import BALANCER_TOPIC, WORKER_REGISTRATION_TOPIC, WORKER_REGISTRED_TOPIC, \
    WORKER_UNREGISTER_TOPIC, BALANCER_WORKER_TOPIC
from utils.worker import WorkersStorageItem, WorkersStorage


messages_queue = asyncio.Queue()
workers_storage = WorkersStorage()


async def on_message(client, topic, payload, qos, properties):
    if topic == BALANCER_TOPIC:
        messages_queue.put_nowait(payload)
        return 0

    print('Balancer. Message.', f"Topic: '{topic}'.", 'Payload:', payload)
    worker_hex = payload.decode('utf-8')
    if topic == WORKER_REGISTRATION_TOPIC:    
        worker = workers_storage.add(worker_hex)
        data = {"worker_num": worker.number, "worker_hex": worker.worker_hex}
        client.publish(WORKER_REGISTRED_TOPIC, json.dumps(data))
    elif topic == WORKER_UNREGISTER_TOPIC:
        workers_storage.delete(worker_hex)
    return 0


async def send_messages(client):
    """
    Balance messages between workers
    """
    while True:
        if STOP.is_set():
            break

        payload = await messages_queue.get()
        worker = await workers_storage.next_worker()
        client.publish(worker.balancer_topic, payload, qos=1)
        print('Balancer. Publish.', f"Topic: '{worker.balancer_topic}'.", 'Payload', payload)
        messages_queue.task_done()


async def main(broker_host, token):
    client = gmqtt.Client("balancer")
    client = await init_client(client, broker_host, token, on_message=on_message)
    client.subscribe(BALANCER_TOPIC, qos=1)
    client.subscribe(WORKER_REGISTRATION_TOPIC, qos=1)
    client.subscribe(WORKER_UNREGISTER_TOPIC, qos=1)
    asyncio.ensure_future(send_messages(client))
    
    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    run_event_loop(main)
