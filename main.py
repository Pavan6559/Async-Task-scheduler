# this is based on producer-consumer message queue system, where the producer adds tasks to the queue and the consumer (worker) executes the tasks asynchronously. The TaskScheduler class manages the tasks and workers, allowing for efficient task execution while ensuring that all tasks are completed before the program exits.

# Producer-Consumer Async Task Scheduler
#
# Architecture:
# Producer -> Async Queue -> Worker Pool -> Task Execution
#
# This project demonstrates:
# - asyncio coroutines
# - producer-consumer architecture
# - worker pools
# - async queues
# - graceful shutdown
# - task lifecycle management
# - error handling
import asyncio

from dataclasses import dataclass
from enum import Enum

class TaskStatus(Enum):

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class Task:

    id: int
    description: str
    duration: float
    status: TaskStatus = TaskStatus.PENDING

class TaskScheduler:

    def __init__(self):
        self.tasks = []
        self.queue = asyncio.Queue()

    async def produce_tasks(self):  # Producer coroutine that pushes tasks
                                    # into the asynchronous queue pipeline
        for task in self.tasks:

            await self.queue.put(task)
            print(f"Task {task.id} Added To Queue")

    def add_task(self, task):
        self.tasks.append(task)
        print(f"Task Added: {task.description}")

    async def execute_task(self, task):

        task.status = TaskStatus.RUNNING
        print(f"Task {task.id} Started")
        await asyncio.sleep(task.duration)
        task.status = TaskStatus.COMPLETED
        print(f"Task {task.id} Completed")
    

    async def worker(self, worker_id):

        while True:
            task = await self.queue.get()

            if task is None:
                print(f"Worker {worker_id} Shutting Down")
                self.queue.task_done()
                break

            print(f"Worker {worker_id} picked Task {task.id}")

            try:
                await self.execute_task(task)
            except Exception as error:
                task.status = TaskStatus.FAILED
                print(
                    f"Task {task.id} Failed "
                    f"with error: {error}"
                )
            finally:
                self.queue.task_done()

    async def start(self):

        worker_tasks = []
        number_of_workers = 3
        for worker_id in range(1, number_of_workers + 1):
            worker_task = asyncio.create_task(
                self.worker(worker_id)
            )
            worker_tasks.append(worker_task)

        await self.produce_tasks()
        await self.queue.join()
        print("All Tasks Completed")

        # this part is essential to signal the workers to stop after all tasks are completed. By putting None in the queue, we 
        # indicate to the workers that they should exit their loop and shut down gracefully. This ensures that the program can 
        # exit cleanly without leaving any worker tasks hanging.
        for _ in range(number_of_workers):
            await self.queue.put(None)
        await asyncio.gather(*worker_tasks)

async def main():

    scheduler = TaskScheduler()

    task1 = Task(
        id=1,
        description="Download File",
        duration=3
    )

    task2 = Task(
        id=2,
        description="Analyze Data",
        duration=2
    )

    task3 = Task(
        id=3,
        description="Generate Report",
        duration=4
    )

    task4 = Task(
        id=4,
        description="Train Model",
        duration=5
    )

    task5 = Task(
        id=5,
        description="Validate Results",
        duration=2
    )

    scheduler.add_task(task1)
    scheduler.add_task(task2)
    scheduler.add_task(task3)
    scheduler.add_task(task4)
    scheduler.add_task(task5)

    await scheduler.start()


asyncio.run(main())