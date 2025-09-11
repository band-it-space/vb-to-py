

from typing import List, Dict
from celery import Celery, chord
import os
import asyncio
import requests
from datetime import datetime

from controllers.get_stocks_codes import get_stocks_codes
from controllers.hk_energy.hk_energy import call_get_symbol_adjusted_data, prepare_stock_data
from models.schemas import EnergyStockRecord
from services.hk_ta import HK_TA
from services.hk_energy import HK_Energy_TA
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
#720
MAX_ATTEMPTS = 10

# DB agents
kl_db_params = {
    "db": settings.kl_db,
    "user": settings.kl_db_user,
    "password": settings.kl_db_pass,
    "host": settings.kl_db_host,
    "port": settings.kl_db_port,
    'charset': 'utf8mb4',
    'autocommit': True
}
db_params_seghio = {
    "db": settings.serhio_db,
    "user": settings.serhio_db_user,
    "password": settings.serhio_db_pass,
    "host": settings.serhio_db_host,
    "port": settings.serhio_db_port,
}
kl_db_service = Database_Service(kl_db_params, pool_size=15)
db_service = Database_Service(db_params_seghio, pool_size=15)

BASE_URL = 'http://fastapi-app:8000'

# HK Energy
@celery_app.task(bind=True, name='prepare_hk_energy')
def prepare_hk_energy_task(self, trade_day: str):
    """
    Prepare data for calculation HK Energy
    """
    try:
        try:
            # Prepare 2800 data
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            results_2800 = loop.run_until_complete(
                call_get_symbol_adjusted_data('2800')
            )
            if not results_2800: 
                clear_hk_energy_token.delay([])
                return {'status': 'error', 'message': 'There is no data for 2800'}

            prepared_2800_data = prepare_stock_data(results_2800, trade_day, '2800.HK')
            if not prepared_2800_data:
                clear_hk_energy_token.delay([])
                return {'status': 'error', 'message': 'There is no data for 2800'}

            # Get stocks codes
            stocks_codes = loop.run_until_complete( get_stocks_codes())
            logger.info(f"Stocks codes received: {len(stocks_codes.get('codes', []))} codes")

            # Send tasks to process HK Energy
            stock_data_2800_dict = [record.dict() for record in prepared_2800_data]
            tasks = [process_hk_energy_task.s(code, stock_data_2800_dict, trade_day) for code in stocks_codes.get("codes", [])[:10]] #! REMOVE

            chord(tasks)(clear_hk_energy_token.s())
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"prepare_hk_energy_task error: {e}")

        # Clear token
        clear_hk_energy_token.delay([])
        # Update task state to FAILURE
        self.update_state(
                state='FAILURE',
                meta={'error': str(e)}
            )
        return {'status': 'error', 'message': str(e)}

@celery_app.task(bind=True, name='process_hk_energy_task')
def process_hk_energy_task(self, stock_code: str, stock_data_2800_dict: List[Dict], trade_day: str):
    """
    Celery task to process HK energy analysis
    """
    try:        
        #Stock data 2800 to EnergyStockRecord
        stock_data_2800 = [EnergyStockRecord(**record_dict) for record_dict in stock_data_2800_dict]

        # Get data for stock
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            stock_data_results = loop.run_until_complete(
                call_get_symbol_adjusted_data(stock_code)
            )
            if not stock_data_results: 
                # clear_hk_energy_token.delay([])
                return {'status': 'error', 'message': f'There is no data for {stock_code} at {trade_day}'}

            prepared_stock_data = prepare_stock_data(stock_data_results, trade_day, stock_code)
            if not prepared_stock_data:
                # clear_hk_energy_token.delay([])
                return {'status': 'error', 'message': f'There is no data for {stock_code} at {trade_day}'}

            
            energy_result = loop.run_until_complete( 
                HK_Energy_TA.start(stock_code.strip(), trade_day.strip(), prepared_stock_data, stock_data_2800 ))
            if energy_result["status"] == "success" and len(energy_result["indicators"]) > 0:
                
                file_service.add_data_to_csv(settings.signal_file_name, energy_result["indicators"], ['stock_code', 'date', 'E1', 'E2', 'E3', 'E4', 'E5', 'is_latest'])
                
            else:
                logger.warning(f"Not saving to CSV: status={energy_result.get('status')}, indicators_count={len(energy_result.get('indicators', []))}")
                
            return energy_result
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

@celery_app.task(name='hk_energy_token_clear')
def clear_hk_energy_token(results):
    logger.info("All HK Energy tasks completed, clearing hk_energy_token")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Clear hk_energy_token
        try:
            loop.run_until_complete(
                file_service.clear_file_content(settings.hk_energy_token_file_name)
            )
        # Update is_latest
            hkex_energy = loop.run_until_complete(
                file_service.read_data_from_csv(settings.signal_file_name)
            )
        # Save all HK Energy signals in db
            if hkex_energy:

                existing_test_data = loop.run_until_complete(
                    file_service.read_data_from_csv(settings.test_db_table_energy)
                )
                if existing_test_data:
                    hkex_stock_codes = set(row['stock_code'] for row in hkex_energy)
                    
                    updated_test_data = []
                    for row in existing_test_data:
                        if row['stock_code'] in hkex_stock_codes:
                            row['is_latest'] = '0'
                        updated_test_data.append(row)
                    
                    if updated_test_data:
                        loop.run_until_complete(file_service.clear_file_content(settings.test_db_table_energy))
                        
                        file_service.add_data_to_csv(
                        settings.test_db_table_energy, 
                        updated_test_data, 
                        ['stock_code', 'date', 'E1', 'E2', 'E3', 'E4', 'E5', 'is_latest']
                    )

                file_service.add_data_to_csv(settings.test_db_table_energy, hkex_energy, ['stock_code', 'date', 'E1', 'E2', 'E3', 'E4', 'E5', 'is_latest'])

            logger.info("Successfully cleared hk_energy_token")
            return {"status": "success", "message": "Token cleared"}
        finally:
            loop.close()
    except Exception as exc:
        logger.error(f"Error clearing hk_ta_token: {str(exc)}")
        return {"status": "error", "message": str(exc)}

#HK TA
@celery_app.task(bind=True, name='process_hk_ta')
def process_hk_ta_task(self, stock_code: str, trade_day: str, data_2800: List[Dict]):
    """
    Celery task to process HK TA analysis
    """
    try:
        logger.info(f"Starting HK TA analysis for {stock_code} on {trade_day}")
        
        # Run the async service in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result =  loop.run_until_complete(
                    HK_TA.start(stock_code, trade_day, data_2800)
            )
            logger.info(f"Completed HK TA analysis for {stock_code}")
            if result["status"] == "error":
                logger.error(f"Error in HK TA analysis for {stock_code}: {result['message']}")
                return result
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
            file_service.add_data_to_csv(settings.signals_hkex_ta1_file_name, [signals_hkex_ta1], ['stockname', 'tradeDay', 'high20', 'low20', 'high50', 'low50', 'high250', 'low250'])
            
            file_service.add_data_to_csv(settings.signals_hkex_ta2_file_name, [signals_hkex_ta2], ['stockname', 'tradeDay', 'pr5', 'pr20', 'pr60', 'pr125', 'pr250', 'rsi14'])
            return result
        finally:
            loop.close()

    except Exception as exc:
        logger.error(f"Error in HK TA task for {stock_code}: {str(exc)}")
        # Update task state to FAILURE
        self.update_state(
            state='FAILURE',
            meta={'error': str(exc), 'stock_code': stock_code, 'trade_day': trade_day}
        )
        raise exc

@celery_app.task(bind=True, name='prepare_hk_ta')
def prepare_hk_ta(self):
    """
    Celery task to check HK TA database status with retry mechanism
    Waits 1 minute between attempts if no data found
    """
    loop = None
    from services.task_scheduler import TaskScheduler

    scheduler = TaskScheduler()
    try:
        logger.info(f"Checking HK TA database status working!")
        
        init_sql_query = """
                CALL get_todays_finished_events("set_adjusted_prices");
                """

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Cancel existing retry task
        loop.run_until_complete(scheduler.cancel_existing_retry_task())

        prices_update_date = loop.run_until_complete(db_service.execute_query(init_sql_query))
        logger.info(f"Finished events: {prices_update_date}")


        # results = loop.run_until_complete(kl_db_service.execute_query(sql_query))

        #results = [[1,]] #! REMOVE
        if prices_update_date:
                logger.info(f"Data found and processed, {prices_update_date[0][0]}")
                
                # Get codes
                response_data = loop.run_until_complete(get_stocks_codes())
                if not response_data.get("date") or not response_data.get("codes"):
                    logger.warning("No data found in get_stocks_codes")
                    raise Exception("No data found in get_stocks_codes")
                
                
                trade_day_date = str(response_data["date"])
                # trade_day_date = "2025-09-04"

                # Get data for 2800
                rows_2800 = loop.run_until_complete(call_get_symbol_adjusted_data('2800'))
                if not rows_2800:
                    raise Exception(f"There is no data for 2800")

                data_2800 = []
                for row in rows_2800:
                    trade_date = row[2]
                    close_price = row[12]
                    if close_price is not None and trade_date <= datetime.strptime(trade_day_date, '%Y-%m-%d').date():
                        data_2800.append({
                            "close": float(close_price),
                            "date": trade_date
                        })



                # tasks = [process_hk_ta_task.s(code, response_data["date"]) for code in response_data["codes"][:10]] #! REMOVE
                tasks = [process_hk_ta_task.s(code, trade_day_date, data_2800) for code in response_data["codes"][:10]]  

                # chord(tasks)(clear_hk_ta_token.s(response_data["date"])) #! REMOVE
                chord(tasks)(clear_hk_ta_token.s(trade_day_date)) 
        else:
                # No data found - retry after 1 minute
                attempt = getattr(self.request, 'retries', 0) + 1
                
                
                if attempt >= MAX_ATTEMPTS:
                    logger.warning(f"HK TA check timeout after {MAX_ATTEMPTS} attempts")

                    # Set retry task after 14 hours
                    loop.run_until_complete(scheduler.schedule_retry_task(delay_hours=settings.daily_retry))

                    # Clear hk_ta_token
                    results = loop.run_until_complete(file_service.clear_file_content(settings.hk_ta_token_file_name))


                    
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
    finally: 
        if loop:
            loop.close()

@celery_app.task
def clear_hk_ta_token(results, date: str):
    logger.info("All HK TA tasks completed, clearing hk_ta_token")
    from services.task_scheduler import TaskScheduler

    scheduler = TaskScheduler()
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Set retry task after 14 hours
            loop.run_until_complete(scheduler.schedule_retry_task(delay_hours=settings.daily_retry))

            # Clear hk_ta_token
            loop.run_until_complete(
                file_service.clear_file_content(settings.hk_ta_token_file_name)
            )
            # Save all HK TA signals in db
            ta_1 = loop.run_until_complete(
                file_service.read_data_from_csv(settings.signals_hkex_ta1_file_name)
            )
            if ta_1:
                file_service.add_data_to_csv(settings.test_db_table_ta1, ta_1, ['stockname', 'tradeDay', 'high20', 'low20', 'high50', 'low50', 'high250', 'low250'])
                
            ta_2 = loop.run_until_complete(
                file_service.read_data_from_csv(settings.signals_hkex_ta2_file_name)
            )
            if ta_2:
                file_service.add_data_to_csv(settings.test_db_table_ta2, ta_2, ['stockname', 'tradeDay', 'pr5', 'pr20', 'pr60', 'pr125', 'pr250', 'rsi14'])

            # Start HK Energy
            endpoint_url = f"{BASE_URL}/api/hk-energy"

            response = requests.post(endpoint_url, timeout=30, json={"trade_day": date})
            logger.info(f"Response: {response.json()}")

            if response.status_code == 200:
                message = "Token cleared, HK Energy started"
            else:
                message = "Token cleared, HK Energy - error"

            return {"status": "success", "message": message}
        finally:
            loop.close()
    except Exception as exc:
        logger.error(f"Error clearing hk_ta_token: {str(exc)}")
        return {"status": "error", "message": str(exc)}

# scheduler task 
@celery_app.task(name='retry_hk_ta')
def retry_hk_ta_task():
    try:
        from controllers.hk_ta.hk_ta_init_task import hk_ta_initialise

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(hk_ta_initialise())
        
        return {"status": "success", "message": "HK TA scheduler initialised"}
    except Exception as exc:
        logger.error(f"Error in retry_hk_ta_task: {str(exc)}")
        return {"status": "error", "message": str(exc)}
    finally:
        if loop:
            loop.close()

def cancel_task(task_id: str):
    """Cancel a task by task ID"""  
    try:
        celery_app.control.revoke(task_id, terminate=True)
        logger.info(f"Task {task_id} cancelled")
        return {"task_id": task_id, "status": "CANCELLED"}
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {str(e)}")
        return {"task_id": task_id, "status": "ERROR", "error": str(e)}

