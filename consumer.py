import time
import gmqtt

from utils.utils import run_event_loop, STOP, init_client
from utils.consts import ALL_WORKERS_RESULT_TOPIC


def on_message(client, topic, payload, qos, properties):
    worker_num = topic.split("/")[-1]
    print('Consumer. Message.', f"Worker: '{worker_num}'.", f"Topic: '{topic}'.", 'Payload:', payload)


async def main(broker_host, token):
    client = gmqtt.Client("consumer")
    client = await init_client(client, broker_host, token, on_message=on_message)
    client.subscribe(ALL_WORKERS_RESULT_TOPIC, qos=1)

    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    run_event_loop(main)
