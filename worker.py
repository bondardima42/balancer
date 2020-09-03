import asyncio
import os
import signal
import time
import uuid
import json
import gmqtt


STOP = asyncio.Event()


class Worker():
    worker_hex = None
    number = None

    def __init__(self, number=None):
        self.worker_hex = uuid.uuid4().hex
        self.number = number

    def register(self, number):
        self.number = number

    def is_registered(self):
        return self.number is not None


worker = Worker()


def on_connect(client, flags, rc, properties):
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    if topic == 'worker-registered':
        data = json.loads(payload.decode('utf-8'))
        worker_hex = data.get("worker_hex")
        worker_num = data.get('worker_num')

        if not worker.is_registered() and worker.worker_hex == worker_hex:
            worker.register(worker_num)
            print(f'Subscribe to balancer/worker/{worker.number}')
            client.subscribe(f'balancer/worker/{worker.number}', qos=1)
    else:
        print('Topic:', topic, 'Payload:', payload)
        client.publish(f'result/worker/{worker.number}', payload, qos=1)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('Subscribed')


def ask_exit(*args):
    STOP.set()


async def main(broker_host, token):
    will_message = gmqtt.Message('worker-unregister', worker.worker_hex, will_delay_interval=2)
    client = gmqtt.Client(f'worker-{worker.worker_hex}', will_message=will_message)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(token, None)
    await client.connect(broker_host)

    client.publish('worker-register', worker.worker_hex, qos=1)
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
