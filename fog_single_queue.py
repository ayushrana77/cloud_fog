"""
Fog Node Implementation (Single-Task, With Queue)
Each fog node can process only one task at a time. If busy, tasks are queued.
"""

from config import FOG_NODES_CONFIG
from utility import calculate_processing_time, calculate_transmission_time, validate_location, calculate_power_consumption
import time
import threading
from collections import deque
from logger import setup_logger
import logging
import os

class FogNode:
    def __init__(self, name, mips, bandwidth, memory, storage, location):
        self.name = name
        self.mips = mips
        self.bandwidth = bandwidth
        self.memory = memory
        self.storage = storage
        self.location = validate_location(location)
        self.assigned_task = None  # Only one task at a time
        self.task_queue = deque()
        self.lock = threading.Lock()
        self.completion_callbacks = []
        self.logger = setup_logger(f'fog_{name}', f'fog_{name}.log', sub_directory='fog')
        self.logger.info(f"Initialized Single-Task Fog Node: {name}")
        # We don't need to log fog node events to fcfs.log
        # The FCFS algorithm should handle its own logging

    def add_completion_callback(self, callback):
        self.completion_callbacks.append(callback)

    @property
    def current_tasks(self):
        if self.assigned_task:
            return [self.assigned_task['task']]
        return []

    def is_busy(self):
        return self.assigned_task is not None

    def queue_task(self, task):
        self.logger.info(f"Queueing task {task['Name']} on {self.name}")
        task['queue_entry_time'] = time.time()
        self.task_queue.append(task)
        self.logger.info(f"Current queue size: {len(self.task_queue)}")

    def assign_task(self, task):
        with self.lock:
            if self.is_busy():
                self.queue_task(task)
                return False, 0
            self.assigned_task = {
                'task': task,
                'processing_time': calculate_processing_time(task['Size'], self.mips),
                'transmission_time': calculate_transmission_time(
                    task['GeoLocation'], self.location, self, task.get('Size'), task.get('MIPS'), self.logger
                ),
                'start_time': time.time(),
                'queue_time': 0
            }
            self.logger.info(f"Task {task['Name']} assigned to {self.name}")
            self.logger.info(f"Resources allocated: MIPS={self.mips}, Memory={self.memory}, Bandwidth={self.bandwidth}, Storage={self.storage}")
            self.logger.info(f"Current load: 100.00%")
            t = threading.Thread(target=self._process_task, args=(self.assigned_task,))
            t.start()
            return True, self.assigned_task['processing_time']

    def _process_task(self, task_info):
        task = task_info['task']
        processing_time = task_info['processing_time']
        transmission_time = task_info['transmission_time']
        queue_time = task_info.get('queue_time', 0)
        self.logger.info(f"Processing task {task['Name']} on {self.name}")
        time.sleep(processing_time)
        with self.lock:
            completion_info = {
                'task': task,
                'processing_time': processing_time,
                'transmission_time': transmission_time,
                'completion_time': time.time(),
                'queue_time': queue_time,
                'fog_node': self.name,
                'total_time': processing_time + transmission_time + queue_time,
                'power_consumption': calculate_power_consumption(
                    transmission_time, processing_time, queue_time, 'fog', 1.0
                )
            }
            self.logger.info(f"Task {task['Name']} completed on {self.name}")
            self.logger.info(f"Resources released: MIPS={self.mips}, Memory={self.memory}, Bandwidth={self.bandwidth}, Storage={self.storage}")
            self.logger.info(f"Current load: 0.00%")
            self.assigned_task = None
            for cb in self.completion_callbacks:
                try:
                    cb(self.name, completion_info)
                except Exception as e:
                    self.logger.error(f"Error in completion callback: {str(e)}")
            self._process_queued_tasks()

    def _process_queued_tasks(self):
        while self.task_queue and not self.is_busy():
            next_task = self.task_queue.popleft()
            queue_time = time.time() - next_task['queue_entry_time']
            task_info = {
                'task': next_task,
                'processing_time': calculate_processing_time(next_task['Size'], self.mips),
                'transmission_time': calculate_transmission_time(
                    next_task['GeoLocation'], self.location, self, next_task.get('Size'), next_task.get('MIPS'), self.logger
                ),
                'start_time': time.time(),
                'queue_time': queue_time
            }
            self.assigned_task = task_info
            self.logger.info(f"Processing queued task {next_task['Name']} on {self.name}")
            self.logger.info(f"Resources allocated: MIPS={self.mips}, Memory={self.memory}, Bandwidth={self.bandwidth}, Storage={self.storage}")
            self.logger.info(f"Current load: 100.00%")
            t = threading.Thread(target=self._process_task, args=(task_info,))
            t.start()
            break

    def get_status(self):
        return {
            'name': self.name,
            'busy': self.is_busy(),
            'queue_length': len(self.task_queue)
        }

def create_fog_nodes():
    fog_nodes = {}
    for node_config in FOG_NODES_CONFIG:
        node = FogNode(
            name=node_config['name'],
            mips=node_config['mips'],
            bandwidth=node_config['bandwidth'],
            memory=node_config['memory'],
            storage=node_config['storage'],
            location=node_config['location']
        )
        fog_nodes[node_config['name']] = node
    return fog_nodes

fog_nodes = create_fog_nodes()

def get_fog_node(name):
    return fog_nodes.get(name)

def get_all_fog_nodes():
    return fog_nodes
