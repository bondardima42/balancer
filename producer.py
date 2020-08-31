import asyncio
import os
import signal
import time
import argparse
import uuid

from gmqtt import Client as MQTTClient
# gmqtt also compatibility with uvloop  
# import uvloop # другая реализация event loop (не asyncio)
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


STOP = asyncio.Event()


def on_connect(client, flags, rc, properties):
    print('Connected')
    # client.subscribe('TEST/#', qos=0)


def on_message(client, topic, payload, qos, properties):
    print('RECV MSG:', payload)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('SUBSCRIBED')


def ask_exit(*args):
    STOP.set()


async def main(broker_host, token):
    client_id = f"producer-{uuid.uuid4().hex}"
    client = MQTTClient(client_id)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(token, None)
    client.set_config({'reconnect_retries': 10, 'reconnect_delay': 60})
    await client.connect(broker_host, raise_exc=True)

    while True:
        payload = str(time.time())
        client.publish('balancer', payload, qos=1)
        await asyncio.sleep(1)
        print(payload)

        if STOP.is_set():
            # await STOP.wait()
            await client.disconnect()
            break


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    host = 'ru-mqtt.flespi.io'
    # token = os.environ.get('FLESPI_TOKEN')
    token = '2ZfcrizySUWW7H8KTYAOLyYGKX5kIWQyJTB010nKK9wIEsgoeG0N4OkFAdN60tuz'

    # signal.SIGINT is translated into a KeyboardInterrupt 
    loop.add_signal_handler(signal.SIGINT, ask_exit)

    # signal.SIGTERM - Termination signal.
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(host, token))
