import time
import json
import gmqtt

from utils.utils import run_event_loop, STOP, init_client
from utils.consts import WORKER_REGISTRED_TOPIC, WORKER_REGISTRATION_TOPIC, WORKER_UNREGISTER_TOPIC, \
    WORKER_RESULT_TOPIC, BALANCER_WORKER_TOPIC
from utils.worker import Worker


worker = Worker()


async def on_message(client, topic, payload, qos, properties):
    if topic == WORKER_REGISTRED_TOPIC:
        data = json.loads(payload.decode('utf-8'))
        worker_hex = data.get("worker_hex")
        worker_num = data.get('worker_num')

        if not worker.is_registered() and worker.worker_hex == worker_hex:
            worker.register(worker_num)
            client.subscribe(f'{worker.balancer_topic}', qos=1)
    elif topic == worker.balancer_topic and worker.is_registered():
        print(f'Worker {worker.number}. Publish.', f"Topic: '{worker.result_topic}'.", 'Payload:', payload)
        client.publish(worker.result_topic, payload, qos=1)
    return 0


async def main(broker_host, token):
    will_message = gmqtt.Message(WORKER_UNREGISTER_TOPIC, worker.worker_hex, will_delay_interval=2)
    client = gmqtt.Client(f'worker-{worker.worker_hex}', will_message=will_message)
    client = await init_client(client, broker_host, token, on_message=on_message)

    client.publish(WORKER_REGISTRATION_TOPIC, worker.worker_hex, qos=1)
    client.subscribe(WORKER_REGISTRED_TOPIC, qos=1)

    await STOP.wait()
    await client.disconnect(reason_code=4) # reason code 4 - disconnect with Will Message


if __name__ == '__main__':
    run_event_loop(main)
