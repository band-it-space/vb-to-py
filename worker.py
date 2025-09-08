#!/usr/bin/env python3
"""
Celery Worker Script

This script starts the Celery worker to process queued tasks.
Usage:
    python worker.py

Or using celery command directly:
    celery -A services.queue_service worker --loglevel=info
"""

import os
import sys
from config.logger import setup_logger
from services.queue_service import celery_app

# Setup logger
worker_logger = setup_logger("celery_worker")
logger = worker_logger

def main():
    """Start the Celery worker"""
    logger.info("Starting Celery worker...")
    
    try:
        # Start the worker with default settings
        celery_app.worker_main([
            'worker',
            '--loglevel=info',
            '--concurrency=4',  # Number of concurrent worker processes
            '--max-tasks-per-child=1000',  # Restart worker after processing N tasks
        ])
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
