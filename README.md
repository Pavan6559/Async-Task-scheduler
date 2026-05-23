# Async Task Scheduler

An asynchronous producer-consumer task scheduler built using Python `asyncio`.

The system uses an async queue and worker pool architecture to execute multiple tasks concurrently while tracking lifecycle states, handling failures, and shutting down gracefully.

---

# Architecture

```text
Producer
   ↓
Async Queue
   ↓
Worker Pool
   ↓
Concurrent Task Execution
```

---

# Features

- Concurrent task execution
- Async worker pool
- Producer-consumer architecture
- Task lifecycle management
- Failure handling
- Graceful worker shutdown
- Queue-based orchestration

---

# Concepts Learned

- `async` / `await`
- `asyncio.Queue`
- `asyncio.create_task()`
- `asyncio.gather()`
- Coroutines
- Event loops
- Worker pools
- Synchronization
- Orchestration systems

---

# Tech Stack

- Python
- asyncio
- dataclasses
- enums

---

# Example Output

```bash
Task 1 Added To Queue
Task 2 Added To Queue

Worker 1 picked Task 1
Worker 2 picked Task 2

Task 2 Completed
Task 1 Completed

All Tasks Processed
Scheduler Shutdown Complete
```

---

# System Design

This project simulates the foundational architecture used in:
- distributed worker systems
- orchestration engines
- async runtimes
- event-driven architectures
- AI agent execution pipelines

---

# Future Improvements

- Task priorities
- Retry systems
- DAG-based dependencies
- Monitoring dashboard
- Dynamic task spawning
- Distributed workers
- Timeout handling

---

# Project Goal

The primary goal of this project is to understand how concurrent orchestration systems manage asynchronous task execution using queues, worker pools, and coroutine scheduling.
