import asyncio
import os
import signal
import time
import gmqtt


STOP = asyncio.Event()
workers = {}
queue = []


def on_connect(client, flags, rc, properties):
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    print(topic, 'RECV MSG:', payload)

    if topic == 'balancer':
        queue.append(payload)
    elif topic == 'worker-register':
        worker_num = len(workers.keys()) + 1
        workers[payload] = worker_num
        client.publish('worker-registered', payload, worker_num=worker_num)
    elif topic == 'worker-unregister':
        try:
            workers.pop(payload)
        except IndexError:
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

        if not queue:
            await asyncio.sleep(0.5)

        for _, worker_num in workers.items():
            try:
                payload = queue.pop(0)
            except IndexError:
                continue
            
            client.publish(f'balancer/worker/{worker_num}', payload, qos=1)


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

    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = 'ru-mqtt.flespi.io'
    # token = os.environ.get('FLESPI_TOKEN')
    token = '2ZfcrizySUWW7H8KTYAOLyYGKX5kIWQyJTB010nKK9wIEsgoeG0N4OkFAdN60tuz'

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)
    loop.run_until_complete(main(host, token))
