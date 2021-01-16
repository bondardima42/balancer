import os
import asyncio
import signal
from contextlib import suppress

STOP = asyncio.Event()

def ask_exit(*args):
    STOP.set()

def run_event_loop(main_fucn):
    """
    Run event loop and check enviroments variables
    """
    loop = asyncio.get_event_loop()

    host = os.environ.get('FLESPI_HOST')
    if not host:
        raise RuntimeError('Environment variable FLESPI_HOST not set.')

    token = os.environ.get('FLESPI_TOKEN')
    if not token:
        raise RuntimeError('Environment variable FLESPI_TOKEN not set.')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)
    loop.run_until_complete(main_fucn(host, token))

    # Cancel all running tasks:
    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            loop.run_until_complete(task)
    return loop

def defualt_on_connect(client, flags, rc, properties):
    print('Connected')

def defualt_on_message(client, topic, payload, qos, properties):
    print('Message', topic, payload)

def defualt_on_disconnect(client, packet, exc=None):
    print('Disconnected')

def defualt_on_subscribe(client, mid, qos, properties):
    print('Subscribe')

async def init_client(
    client, broker_host, token, on_connect=None, on_message=None, on_disconnect=None, on_subscribe=None):
    """
    Set client configs and connect to mqtt broker
    """
    client.on_connect = on_connect or defualt_on_connect
    client.on_message = on_message or defualt_on_message
    client.on_disconnect = on_disconnect or defualt_on_disconnect
    client.on_subscribe = on_subscribe or defualt_on_subscribe

    client.set_auth_credentials(token, None)
    client.set_config({'reconnect_retries': 10, 'reconnect_delay': 60})
    await client.connect(broker_host, raise_exc=True)
    return client
    