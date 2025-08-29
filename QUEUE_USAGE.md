# RabbitMQ Queue System Usage

This document explains how to use the RabbitMQ-based queue system that has been added to the FastAPI application.

## Overview

The queue system allows you to process long-running tasks (like HK Energy and HK TA analysis) asynchronously in the background using RabbitMQ and Celery.

## Components

1. **RabbitMQ** - Message broker with management UI
2. **Celery** - Distributed task queue
3. **Worker processes** - Process queued tasks
4. **API endpoints** - Submit, monitor, and manage tasks

## Starting the System

### Using Docker Compose (Recommended)

```bash
# Start all services (RabbitMQ, FastAPI app, and Celery worker)
docker-compose up -d

# View logs
docker-compose logs -f fastapi-app
docker-compose logs -f celery-worker
docker-compose logs -f rabbitmq
```

### Manual Setup

1. **Start RabbitMQ:**

```bash
# Using Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin \
  -e RABBITMQ_DEFAULT_PASS=password \
  rabbitmq:3-management-alpine

# Or install locally and start
sudo systemctl start rabbitmq-server
```

2. **Start the FastAPI application:**

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

3. **Start Celery worker:**

```bash
# Option 1: Using the worker script
python worker.py

# Option 2: Using celery command directly
celery -A services.queue_service worker --loglevel=info
```

## API Endpoints

### Queue Tasks

#### Queue HK Energy Analysis

```bash
POST /api/queue/hk-energy
Content-Type: application/json

{
    "stock_code": "0001",
    "trade_day": "2024-01-15"
}

Response:
{
    "task_id": "abc123-def456-789ghi",
    "status": "QUEUED",
    "stock_code": "0001",
    "trade_day": "2024-01-15",
    "message": "HK Energy analysis queued for 0001"
}
```

#### Queue HK TA Analysis

```bash
POST /api/queue/hk-ta
Content-Type: application/json

{
    "stock_code": "0001",
    "trade_day": "2024-01-15"
}

Response:
{
    "task_id": "abc123-def456-789ghi",
    "status": "QUEUED",
    "stock_code": "0001",
    "trade_day": "2024-01-15",
    "message": "HK TA analysis queued for 0001"
}
```

### Monitor Tasks

#### Get Task Status

```bash
GET /api/queue/status/{task_id}

Response:
{
    "task_id": "abc123-def456-789ghi",
    "status": "SUCCESS",  # PENDING, STARTED, SUCCESS, FAILURE, CANCELLED
    "result": {
        "status": "success",
        "message": "Analysis completed",
        "indicators": [...]
    },
    "info": null
}
```

#### Cancel Task

```bash
DELETE /api/queue/cancel/{task_id}

Response:
{
    "task_id": "abc123-def456-789ghi",
    "status": "CANCELLED",
    "error": null
}
```

## Task States

-   **PENDING** - Task is waiting to be processed
-   **STARTED** - Task is currently being processed
-   **SUCCESS** - Task completed successfully
-   **FAILURE** - Task failed with an error
-   **CANCELLED** - Task was cancelled by user

## Configuration

## RabbitMQ Management UI

RabbitMQ comes with a built-in web management interface:

-   **URL**: http://localhost:15672
-   **Username**: admin
-   **Password**: password

In the management UI you can:

-   View queues and their messages
-   Monitor worker connections
-   See message rates and statistics
-   Manage exchanges and bindings
-   View worker performance

### Environment Variables

```bash
# RabbitMQ configuration
RABBITMQ_URL=amqp://admin:password@rabbitmq:5672//

# For local development
RABBITMQ_URL=amqp://admin:password@localhost:5672//
```

### Settings (config/settings.py)

```python
# RabbitMQ settings
rabbitmq_host: str = "localhost"
rabbitmq_port: int = 5672
rabbitmq_user: str = "admin"
rabbitmq_password: str = "password"
rabbitmq_vhost: str = "/"

# Celery settings
celery_broker_url: str = "amqp://admin:password@localhost:5672//"
celery_result_backend: str = "rpc://"
```

## Monitoring

### View Worker Status

```bash
# Check active workers
celery -A services.queue_service status

# Monitor tasks in real-time
celery -A services.queue_service events
```

### RabbitMQ CLI Monitoring

```bash
# Check RabbitMQ status
sudo rabbitmqctl status

# List queues
sudo rabbitmqctl list_queues

# List exchanges
sudo rabbitmqctl list_exchanges

# List connections
sudo rabbitmqctl list_connections

# View queue details (inside Docker container)
docker exec rabbitmq rabbitmqctl list_queues name messages consumers
```

## Example Usage

1. **Submit a task:**

```bash
curl -X POST http://localhost:8000/api/queue/hk-energy \
  -H "Content-Type: application/json" \
  -d '{"stock_code": "0001", "trade_day": "2024-01-15"}'
```

2. **Check task status:**

```bash
curl http://localhost:8000/api/queue/status/abc123-def456-789ghi
```

3. **Cancel if needed:**

```bash
curl -X DELETE http://localhost:8000/api/queue/cancel/abc123-def456-789ghi
```

## Benefits

-   **Asynchronous Processing** - API responds immediately, tasks run in background
-   **Scalability** - Can run multiple worker processes
-   **Reliability** - Tasks are persisted in Redis, survive restarts
-   **Monitoring** - Full visibility into task status and results
-   **Error Handling** - Failed tasks can be retried or investigated

## Troubleshooting

### Common Issues

1. **Redis connection failed**

    - Check if Redis is running: `redis-cli ping`
    - Verify Redis URL in settings

2. **Worker not processing tasks**

    - Check worker logs: `docker-compose logs celery-worker`
    - Ensure worker is connected to same Redis instance

3. **Tasks stuck in PENDING**

    - Restart worker: `docker-compose restart celery-worker`
    - Check for worker errors in logs

4. **Memory issues**
    - Reduce worker concurrency
    - Add task time limits
    - Restart workers periodically
