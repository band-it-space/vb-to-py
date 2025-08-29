import aiomysql

from config.logger import setup_logger
from config.settings import Settings
from models.schemas import EnergyStockRecord
from services.db_service import Database_Service
from services.hk_energy import HK_Energy_TA

energy_logger = setup_logger("hk_energy_controller")
logger = energy_logger

settings = Settings()

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

async def hk_energy_controller(stock_code: str, trade_day: str):
    logger.info(f"DB params: {db_params}")
    logger.info(f"Starting hk_energy_controller for {stock_code} on {trade_day}")
    try:
        query = f"""SELECT CONCAT(
                        DATE_FORMAT(tradeday, '%Y-%m-%d'), ',',
                        TIME(tradeday), ',',
                        COALESCE(adj_open, adj_close), ',',
                        COALESCE(adj_high, adj_close), ',',
                        COALESCE(adj_low, adj_close), ',',
                        COALESCE(adj_close, 0), ',',
                        COALESCE(adj_volume, 0)
                    ) AS csv_row
                FROM signal_hkex_price
                WHERE code = '{stock_code}'
                    AND tradeday >= '2001-01-01'
                    AND IFNULL(adj_close, 0) > 0
                    AND tradeday <= '{trade_day}'
                ORDER BY tradeday;"""
        results = await kl_db_service.execute_query(query)
                    
    except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
    logger.info(f"Query executed successfully, results length: {len(results)}")
    stock_data = []
    if results:
        for row in results:
            if row[0] is not None:
                date, time, open_price, high, low, close, volume = row[0].split(",") 

                stock_record = EnergyStockRecord(
                        stock_code=stock_code,
                        date=date,
                        time=time,
                        open=float(open_price),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=float(volume)
                    )
                stock_data.append(stock_record) 
        logger.info(f"Fetched {len(stock_data)} records from database") 
        
    try:
        query = f"""SELECT CONCAT(
                        DATE_FORMAT(tradeday, '%Y-%m-%d'), ',',
                        TIME(tradeday), ',',
                        COALESCE(adj_open, adj_close), ',',
                        COALESCE(adj_high, adj_close), ',',
                        COALESCE(adj_low, adj_close), ',',
                        COALESCE(adj_close, 0), ',',
                        COALESCE(adj_volume, 0)
                    ) AS csv_row
                FROM signal_hkex_price
                WHERE code = '2800'
                    AND tradeday >= '2001-01-01'
                    AND IFNULL(adj_close, 0) > 0
                    AND tradeday <= '{trade_day}'
                ORDER BY tradeday;"""

        results_2800 = await kl_db_service.execute_query(query)
                    
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        logger.error(f"Query: {query}")
        raise

    logger.info(f"Query executed successfully, results_2800 length: {len(results_2800)}")

    stock_data_2800 = []
    if results_2800:
        for row in results_2800:
            if row[0] is not None:
                date, time, open_price, high, low, close, volume = row[0].split(",") 

                stock_record = EnergyStockRecord(
                        stock_code='2800.HK',
                        date=date,
                        time=time,
                        open=float(open_price),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=float(volume)
                    )
                stock_data_2800.append(stock_record) 
    logger.info(f"Fetched stock_data_2800 {len(stock_data_2800)} records from database") 

    return await HK_Energy_TA.start(stock_code.strip(), trade_day.strip(), stock_data, stock_data_2800 )