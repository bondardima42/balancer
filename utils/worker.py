import uuid
import asyncio

from .utils import STOP
from .consts import BALANCER_WORKER_TOPIC, WORKER_RESULT_TOPIC


class Worker:
    """
    Сontains worker data
    """
    worker_hex = None
    number = None # number of worker in sequence
    balancer_topic = None # worker listen this topic
    result_topic = None # send result to this topic

    def __init__(self, worker_hex=None):
        self.worker_hex = worker_hex or uuid.uuid4().hex

    def register(self, number):
        self.number = number
        self.balancer_topic = f'{BALANCER_WORKER_TOPIC}{number}'
        self.result_topic = f'{WORKER_RESULT_TOPIC}{number}'

    def is_registered(self):
        return self.number is not None


class WorkersStorageItem(Worker):
    deleted = False

    def delete(self):
        self.deleted = True


class WorkersStorage:
    """
    Looped queue of workers
    """
    _last_worker_num = 0
    _workers = {} # dict of all workers
    _workers_sequence = None # current sequence of workers, should be immutable
    _iterator = None

    async def next_worker(self):
        """
        Return next worker.
        If sequence of workers is empty, wait until an worker is available.
        """
        while True:
            worker = self.next_worker_nowait()
            if worker:
                return worker
            else:
                await asyncio.sleep(1)

    def next_worker_nowait(self):
        """
        Return next worker. When iteration finishes method removes workers with flag 'deleted'
        and create new iterator.
        """
        if self.is_empty():
            return None

        if not self._iterator:
            self._iterator = self._get_iterator()

        try:
            worker = next(self._iterator)
            if worker.deleted:
                worker = self.next_worker_nowait()
            return worker
        except StopIteration:
            self._iterator = self._get_iterator()
            return self.next_worker_nowait()

    def get(self, worker_hex):
        """
        Return worker by hex
        """
        return self._workers.get(worker_hex)

    def _remove_deleted(self):
        """
        Remove workers marked as deleted
        """
        self._workers = {worker_hex: worker for worker_hex, worker in self._workers.items() if not worker.deleted}

    def _get_iterator(self):
        """
        Create new iterator
        """
        self._remove_deleted()
        self._workers_sequence = tuple(worker for worker in self._workers.values()) 
        return iter(self._workers_sequence)

    def add(self, worker_hex):
        """
        Add worker to pool
        """
        worker = WorkersStorageItem(worker_hex)
        self._last_worker_num += 1
        worker.register(self._last_worker_num)
        self._workers[worker_hex] = worker
        return worker

    def delete(self, worker_hex):
        """
        Mark worker as deleted
        """
        worker = self.get(worker_hex)
        if worker:
            worker.delete()

    def is_empty(self):
        return not self._workers
