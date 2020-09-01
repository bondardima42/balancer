import asyncio
import os
import signal
import time
import json
import gmqtt


STOP = asyncio.Event()
workers = {}
queue = asyncio.Queue()
last_worker_num = 0


def on_connect(client, flags, rc, properties):
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    print('Topic:', topic, 'Payload:', payload)
    global last_worker_num

    if topic == 'balancer':
        queue.put_nowait(payload)
    elif topic == 'worker-register':
        worker_hex = payload.decode('utf-8')
        worker_num = last_worker_num + 1
        workers[worker_hex] = worker_num
        data = {"worker_num": worker_num, "worker_hex": worker_hex}

        client.publish('worker-registered', json.dumps(data))
        last_worker_num = worker_num
    elif topic == 'worker-unregister':
        worker_hex = payload.decode('utf-8')
        try:
            workers.pop(worker_hex)
        except KeyError:
            pass


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('Subscribed')


def ask_exit(*args):
    STOP.set()


async def send_messages(client):
    while True:
        if STOP.is_set():
            break

        if queue.empty() or not workers:
            await asyncio.sleep(1)

        for _, worker_num in list(workers.items()):
            payload = await queue.get()
            client.publish(f'balancer/worker/{worker_num}', payload, qos=1)
            queue.task_done()


async def main(broker_host, token):
    client = gmqtt.Client("balancer")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(token, None)
    await client.connect(broker_host)

    client.subscribe('balancer', qos=1)
    client.subscribe('worker-register', qos=1)
    client.subscribe('worker-unregister', qos=1)

    await send_messages(client)
    await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = 'ru-mqtt.flespi.io'
    token = os.environ.get('FLESPI_TOKEN')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)
    loop.run_until_complete(main(host, token))
