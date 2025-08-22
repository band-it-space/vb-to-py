import pymysql
from typing import List, Tuple, Optional
from config.logger import setup_logger
from config.settings import settings


db_logger = setup_logger("db_service")
logger = db_logger

db_params = {
    "db": settings.serhio_db,
    "user": settings.serhio_db_user,
    "password": settings.serhio_db_pass,
    "host": settings.serhio_db_host,
    "port": settings.serhio_db_port,
}


class Database_Service:
    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        try:
            if self.connection:
                self._close()
            
            self.connection = pymysql.connect(**db_params)
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _close(self) -> None:
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.connection = None

    def __ensure_connection(self) -> None:
        try:
            if self.connection is None:
                self._connect()
                return
            
            self.connection.ping(reconnect=True)
        except Exception as e:
            logger.warning(f"Connection lost: {e}. Reconnecting...")
            self._connect()

    async def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        
        self.__ensure_connection()
        
        if self.connection is None:
            raise RuntimeError("Failed to establish database connection")

        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                return list(cursor.fetchall())
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise

    def __del__(self):
        self._close()

