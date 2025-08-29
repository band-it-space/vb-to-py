from decimal import MAX_EMAX
from celery import Celery
import os
import asyncio
import time
import requests

from services.hk_ta import HK_TA
from services.db_service import Database_Service
from config.settings import settings
from config.logger import setup_logger
from services.file_services import FileService


file_service = FileService()
# Setup logger
queue_logger = setup_logger("queue_service")
logger = queue_logger

# Get RabbitMQ URL from environment or settings
rabbitmq_url = os.getenv("RABBITMQ_URL", settings.celery_broker_url)

# Create Celery app
celery_app = Celery(
    "queue_service",
    broker=rabbitmq_url,
    backend=settings.celery_result_backend,
    include=['services.queue_service']
)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)

MAX_ATTEMPTS = 6

# DB agents
db_params = {
    "db": settings.kl_db,
    "user": settings.kl_db_user,
    "password": settings.kl_db_pass,
    "host": settings.kl_db_host,
    "port": settings.kl_db_port,
    'charset': 'utf8mb4',
    'autocommit': True
}

kl_db_service = Database_Service(db_params)


@celery_app.task(bind=True, name='process_hk_energy')
def process_hk_energy_task(self, stock_code: str, trade_day: str):
    """
    Celery task to process HK energy analysis
    """
    try:
        logger.info(f"Starting HK Energy analysis for {stock_code} on {trade_day}")
        
        # Import here to avoid circular imports
        from controllers.hk_energy import hk_energy_controller
        import asyncio
        
        # Run the async controller in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                hk_energy_controller(stock_code, trade_day)
            )
            logger.info(f"Completed HK Energy analysis for {stock_code}")
            return result
        finally:
            loop.close()
            
    except Exception as exc:
        logger.error(f"Error in HK Energy task for {stock_code}: {str(exc)}")
        # Update task state to FAILURE
        self.update_state(
            state='FAILURE',
            meta={'error': str(exc), 'stock_code': stock_code, 'trade_day': trade_day}
        )
        raise exc

@celery_app.task(bind=True, name='process_hk_ta')
def process_hk_ta_task(self, stock_code: str, trade_day: str):
    """
    Celery task to process HK TA analysis
    """
    try:
        logger.info(f"Starting HK TA analysis for {stock_code} on {trade_day}")
        
        # Run the async service in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                HK_TA.start(stock_code, trade_day)
            )
            logger.info(f"Completed HK TA analysis for {stock_code}, result: {result}")
            #TODO вибрати значення та зберегти в файли 
            data = result.get('data_from_sergio_ta', {})

            signals_hkex_ta1 = {
                'stockname': stock_code,
                'tradeDay': trade_day,
                'high20': data.get('high20'),
                'low20': data.get('low20'),
                'high50': data.get('high50'),
                'low50': data.get('low50'),
                'high250': data.get('high250'),
                'low250': data.get('low250')
            }
                        
            signals_hkex_ta2 = {
                'stockname': stock_code,
                'tradeDay': trade_day,
                'pr5': data.get('pr5'),
                'pr20': data.get('pr20'),
                'pr60': data.get('pr60'),
                'pr125': data.get('pr125'),
                'pr250': data.get('pr250'),
                'rsi14': data.get('rsi14')
            }
            logger.info(f"Signals_hkex_ta1: {signals_hkex_ta1}")
            logger.info(f"Signals_hkex_ta2: {signals_hkex_ta2}")
            file_service.add_data_to_csv('signals_hkex_ta1', [signals_hkex_ta1], ['stockname', 'tradeDay', 'high20', 'low20', 'high50', 'low50', 'high250', 'low250'])
            
            file_service.add_data_to_csv('signals_hkex_ta2', [signals_hkex_ta2], ['stockname', 'tradeDay', 'pr5', 'pr20', 'pr60', 'pr125', 'pr250', 'rsi14'])
            return result
        finally:
            loop.run_until_complete(file_service.clear_file_content("hk_ta_token"))
            loop.close()

                
                    
            
    except Exception as exc:
        logger.error(f"Error in HK TA task for {stock_code}: {str(exc)}")
        # Update task state to FAILURE
        self.update_state(
            state='FAILURE',
            meta={'error': str(exc), 'stock_code': stock_code, 'trade_day': trade_day}
        )
        raise exc


@celery_app.task(bind=True, name='check_hk_ta')
def queue_check_hk_ta(self):
    """
    Celery task to check HK TA database status with retry mechanism
    Waits 1 minute between attempts if no data found
    """
    
    try:
        
        logger.info(f"Checking HK TA database status working!")

        # Check database for data (placeholder implementation)
        sql_query = """
                SELECT COUNT(0) FROM signal_hkex_01_interface_log a
                WHERE a.tradeday > (SELECT MAX(tradeday) FROM signal_hkex_ta1 b)
                AND a.end_date IS NOT NULL;
                """

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(kl_db_service.execute_query(sql_query))
        finally:
            loop.close()
        results = [[1,]] #! REMOVE
        if results:
            if results[0][0] > 0: 
                logger.info(f"Data found and processed, {results[0][0]}")

                base_url = 'http://fastapi-app:8000'
                endpoint_url = f"{base_url}/api/queue/hk-ta"

                response = requests.get(endpoint_url, timeout=30)
                logger.info(f"Response: {response.json()}")
                if response.status_code == 200:
                    response_message = response.json()["message"]
                else:
                    response_message = "Error in queue/hk-ta"

                return {
                    "status": "success",
                    "message": response_message,
                    "data": results[0][0],
                    "attempts": getattr(self.request, 'retries', 0) + 1
                }
            else:
                # No data found - retry after 1 minute
                attempt = getattr(self.request, 'retries', 0) + 1
                
                
                if attempt >= MAX_ATTEMPTS:
                    logger.warning(f"HK TA check timeout after {MAX_ATTEMPTS} attempts")

                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        results = loop.run_until_complete(file_service.clear_file_content("hk_ta_token"))
                    finally:
                        loop.close()
                    return {
                        "status": "timeout",
                        "message": f"No data found after {MAX_ATTEMPTS} attempts",
                        "attempts": attempt
                    }
                
                logger.info(f"No data found, retrying in 1 minute (attempt {attempt}/{MAX_ATTEMPTS})")
                raise self.retry(countdown=60, max_retries=MAX_ATTEMPTS)


    except Exception as exc:
        # Check if it's a Celery retry exception
        if 'Retry' in str(exc):
            logger.info(f"Task retry scheduled: {str(exc)}")
            raise exc
        else:
            logger.error(f"Error in check_hk_ta_task: {str(exc)}")
            self.update_state(
                state='FAILURE',
                meta={'error': str(exc)}
            )
            raise exc


# # Task status helper functions
# def get_task_status(task_id: str):
#     """Get task status by task ID"""
#     try:
#         task = celery_app.AsyncResult(task_id)
#         return {
#             "task_id": task_id,
#             "status": task.status,
#             "result": task.result if task.ready() else None,
#             "info": task.info
#         }
#     except Exception as e:
#         logger.error(f"Error getting task status for {task_id}: {str(e)}")
#         return {
#             "task_id": task_id,
#             "status": "ERROR",
#             "result": None,
#             "info": {"error": str(e)}
#         }

def cancel_task(task_id: str):
    """Cancel a task by task ID"""  
    try:
        celery_app.control.revoke(task_id, terminate=True)
        logger.info(f"Task {task_id} cancelled")
        return {"task_id": task_id, "status": "CANCELLED"}
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {str(e)}")
        return {"task_id": task_id, "status": "ERROR", "error": str(e)}



