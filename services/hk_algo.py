import asyncio
import pymysql
from typing import Dict, Any
from config.logger import setup_logger
from config.settings import settings


algo_logger = setup_logger("algorithm_handler")
logger = algo_logger

db_params = {
    "db": settings.serhio_db,
    "user": settings.serhio_db_user,
    "password": settings.serhio_db_pass,
    "host": settings.serhio_db_host,
}

class HKAlgorithm:
    def __init__(self):
        self.is_running = False
        self.db= pymysql.connect(**db_params)

    async def start(self, stockname: str) -> Dict[str, Any]:

        try:
            logger.info(f"Запуск алгоритму для акції: {stockname}")
            
            query = f"SELECT symbol, high, low, close, date FROM hkex_stock_price WHERE symbol = '{stockname}' AND date >= CURDATE() - INTERVAL 10 DAY ORDER BY date DESC"

            with self.db.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                print(rows)
            
            if self.is_running:
                logger.warning("Алгоритм вже запущений")
                return {
                    "status": "error",
                    "message": "Алгоритм вже виконується",
                    "stockname": stockname
                }
            
            self.is_running = True
            

            logger.info(f"Початок обробки акції {stockname}")
            
            # Тут буде ваша логіка алгоритму
            # Симуляція обробки
            await asyncio.sleep(0.5)
            
            result = {
                "status": "success",
                "message": f"Алгоритм успішно завершений для {stockname}",
                "stockname": stockname,
                "data": {
                    
                }
            }
            
            logger.info(f"Алгоритм завершено успішно для {stockname}")
            return result
            
        except Exception as e:
            logger.error(f"Помилка під час виконання алгоритму для {stockname}: {str(e)}")
            return {
                "status": "error",
                "message": f"Помилка виконання алгоритму: {str(e)}",
                "stockname": stockname
            }
        finally:
            self.is_running = False
    

HKAlgo = HKAlgorithm()
