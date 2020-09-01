import asyncio
import os
import signal
import time
import uuid
import json
import gmqtt


STOP = asyncio.Event()
worker_hex = uuid.uuid4().hex
worker_num = None


def on_connect(client, flags, rc, properties):
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    global worker_num

    if topic == 'worker-registered':
        data = json.loads(payload.decode('utf-8'))
        _worker_hex = data.get("worker_hex")

        if not worker_num and _worker_hex == worker_hex:
            worker_num = data.get('worker_num')
            print(f'Subscribe to balancer/worker/{worker_num}')
            client.subscribe(f'balancer/worker/{worker_num}', qos=1)
    else:
        print('Topic:', topic, 'Payload:', payload)
        client.publish(f'result/worker/{worker_num}', payload, qos=1)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('Subscribed')


def ask_exit(*args):
    STOP.set()


async def main(broker_host, token):
    will_message = gmqtt.Message('worker-unregister', worker_hex, will_delay_interval=2)
    client = gmqtt.Client(f'worker-{worker_hex}', will_message=will_message)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(token, None)
    await client.connect(broker_host)

    client.publish('worker-register', worker_hex, qos=1)
    client.subscribe('worker-registered', qos=1)

    await STOP.wait()
    await client.disconnect(reason_code=4)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = 'ru-mqtt.flespi.io'
    token = os.environ.get('FLESPI_TOKEN')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)
    loop.run_until_complete(main(host, token))
