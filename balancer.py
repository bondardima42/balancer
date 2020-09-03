import asyncio
import os
import signal
import time
import json
import gmqtt


STOP = asyncio.Event()
messages_queue = asyncio.Queue()


class Worker():
    worker_hex = None
    number = None
    deleted = False

    def __init__(self, worker_hex, number):
        self.worker_hex = worker_hex
        self.number = number

    def delete(self):
        self.deleted = True


class WorkersStorage():
    last_worker_num = 0
    workers = {}
    iterator = None
    to_delete = {}
    to_add = {}

    async def next_worker(self):
        while True:
            if STOP.is_set():
                return

            worker = self.next_worker_nowait()
            if worker:
                return worker
            else:
                await asyncio.sleep(1)

    def next_worker_nowait(self):
        if self.is_empty():
            return None

        if not self.iterator:
            self.iterator = iter(self.workers.values())

        try:
            worker = next(self.iterator)
            if worker.deleted:
                worker = self.next_worker_nowait()
            return worker
        except StopIteration:
            self.iterator = None
            self.update_workers()
            return self.next_worker_nowait()

    def get(self, worker_hex):
        return self.workers.get(worker_hex) or self.to_add.get(worker_hex)

    def get_all_workers(self):
        return self.workers

    def add(self, worker_hex):
        self.last_worker_num += 1
        worker = Worker(worker_hex, self.last_worker_num)
        self.to_add[worker_hex] = worker

        if self.is_empty():
            self.update_workers()        
        return worker

    def delete(self, worker_hex):
        worker = self.get(worker_hex)
        if worker:
            worker.delete()
            self.to_delete[worker_hex] = worker

    def is_empty(self):
        return not self.workers

    def update_workers(self):
        self.workers.update(self.to_add)
        self.to_add = {}

        for worker_hex, _ in self.to_delete.items():
            self.workers.pop(worker_hex)
        self.to_delete = {}


workers_storage = WorkersStorage()


def on_connect(client, flags, rc, properties):
    print('Connected')


def on_message(client, topic, payload, qos, properties):
    print('Topic:', topic, 'Payload:', payload)

    if topic == 'balancer':
        messages_queue.put_nowait(payload)
    elif topic == 'worker-register':
        worker_hex = payload.decode('utf-8')
        worker = workers_storage.add(worker_hex)
        data = {"worker_num": worker.number, "worker_hex": worker.worker_hex}
        client.publish('worker-registered', json.dumps(data))
    elif topic == 'worker-unregister':
        worker_hex = payload.decode('utf-8')
        workers_storage.delete(worker_hex)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('Subscribed')


def ask_exit(*args):
    STOP.set()


async def send_messages(client):
    while True:
        if STOP.is_set():
            return

        payload = await messages_queue.get()
        worker = await workers_storage.next_worker()
        client.publish(f'balancer/worker/{worker.number}', payload, qos=1)
        messages_queue.task_done()


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
