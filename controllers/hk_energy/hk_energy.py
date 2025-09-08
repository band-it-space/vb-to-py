
from config.logger import setup_logger
from config.settings import Settings
from models.schemas import EnergyStockRecord
from services.db_service import Database_Service
from services.hk_energy import HK_Energy_TA
from services.file_services import FileService
from asyncio import sleep

energy_logger = setup_logger("hk_energy_controller")
logger = energy_logger

settings = Settings()
file_service = FileService()


db_params_seghio = {
    "db": settings.serhio_db,
    "user": settings.serhio_db_user,
    "password": settings.serhio_db_pass,
    "host": settings.serhio_db_host,
    "port": settings.serhio_db_port,
}

db_service = Database_Service(db_params_seghio, pool_size=10)

async def call_get_symbol_adjusted_data(stock_name: str, max_retries = 3):

    query = f"""
            CALL get_symbol_adjusted_data('{stock_name}');
            """

    rows = None 
    for attempt in range(max_retries):
        try:
            logger.info(f"Executing query for {stock_name} (attempt {attempt + 1}/{max_retries})")
            rows = await db_service.execute_query(query)
                    
            if rows:
                logger.info(f"Query successful for {stock_name}: {len(rows)} rows returned")
                return rows
            else:
                logger.warning(f"Query returned empty result for {stock_name} (attempt {attempt + 1})")
                        
        except Exception as e:
                logger.error(f"Database query failed for {stock_name} (attempt {attempt + 1}): {str(e)}")
                if attempt == max_retries - 1:
                    return None
                
                # Short delay before retry
                if attempt < max_retries - 1:
                    await sleep(1)

def prepare_stock_data(stock_data, trade_day: str, stock_code: str):
    stock_data_new = []
    if not any(row[2].strftime('%Y-%m-%d') == trade_day for row in stock_data):
        return []
    for row in stock_data:
        row_date = row[2]
        if hasattr(row_date, 'strftime'):
            row_date_str = row_date.strftime('%Y-%m-%d')
        else:
            row_date_str = str(row_date)
            
        if row_date_str <= trade_day:
            stock_record = EnergyStockRecord(
                stock_code=stock_code,
                date=row_date_str,
                time=str(row[2]),
                open=float(row[4]),
                high=float(row[7]),
                low=float(row[10]),
                close=float(row[13]),
                volume=float(row[16])
                )
            stock_data_new.append(stock_record) 
    return stock_data_new


async def hk_energy_controller(stock_code: str, trade_day: str):
    logger.info(f"Starting hk_energy_controller for {stock_code} on {trade_day}")
    try:

        stock_data_serhio = await call_get_symbol_adjusted_data(stock_code)
        if stock_data_serhio:
            logger.info(f"Fetched {len(stock_data_serhio)} records from Serhio database")

            stock_data_serhio_new = prepare_stock_data(stock_data_serhio, trade_day, stock_code)
            if not stock_data_serhio_new:
                return {
                    "status": "error",
                    "message": f"There is no data for {stock_code} at day: {trade_day} in database",
                    "stockname": stock_code,
                    "tradeDay": trade_day
                }
    except Exception as e:
            logger.error(f"Error executing {stock_code} query: {e}")
            raise

    try:
        results_2800_serhio = await call_get_symbol_adjusted_data('2800')
        if results_2800_serhio:
            stock_data_2800_serhio_new = prepare_stock_data(results_2800_serhio, trade_day, '2800')
        else:
            stock_data_2800_serhio_new = []

    except Exception as e:
        logger.error(f"Error executing 2800 query: {e}")
        raise


    return await HK_Energy_TA.start(stock_code.strip(), trade_day.strip(), stock_data_serhio_new, stock_data_2800_serhio_new )
