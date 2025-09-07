import pymysql
import threading
import asyncio
from typing import List, Tuple, Dict, Any, Optional
from config.logger import setup_logger
from contextlib import contextmanager


db_logger = setup_logger("db_service")
logger = db_logger


class Database_Service:
    def __init__(self, db_params: Dict[str, Any], pool_size: int = 10):
        self.db_params = db_params
        self.pool_size = pool_size
        self._local = threading.local()
        self._lock = threading.Lock()
        self._connection_count = 0

    def _get_connection(self):
        """Get thread-local connection"""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            try:
                with self._lock:
                    if self._connection_count >= self.pool_size:
                        logger.warning(f"Connection pool limit reached ({self.pool_size})")
                    
                    self._local.connection = pymysql.connect(**self.db_params)
                    self._connection_count += 1
                    logger.info(f"New database connection created (total: {self._connection_count})")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                raise
        return self._local.connection

    def _close_connection(self) -> None:
        """Close thread-local connection"""
        if hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.close()
                with self._lock:
                    self._connection_count -= 1
                logger.info(f"Database connection closed (remaining: {self._connection_count})")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self._local.connection = None

    def _ensure_connection(self) -> None:
        """Ensure thread-local connection is active"""
        try:
            connection = self._get_connection()
            connection.ping(reconnect=True)
        except Exception as e:
            logger.warning(f"Connection lost: {e}. Reconnecting...")
            self._close_connection()
            self._get_connection()

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic retry"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self._ensure_connection()
                connection = self._get_connection()
                
                with connection.cursor() as cursor:
                    yield cursor
                    break
                    
            except (pymysql.OperationalError, pymysql.InterfaceError) as e:
                retry_count += 1
                logger.warning(f"Database error (attempt {retry_count}/{max_retries}): {e}")
                
                if retry_count >= max_retries:
                    logger.error(f"Max retries reached for database operation")
                    raise
                
                # Force reconnection
                self._close_connection()
                import time
                time.sleep(0.5 * retry_count)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Unexpected database error: {e}")
                raise

    async def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute query with automatic retry and connection management"""
        
        try:
            with self.get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = list(cursor.fetchall())
                logger.debug(f"Query returned {len(result)} rows")
                return result
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise

    def close_all_connections(self):
        """Close all connections (useful for cleanup)"""
        self._close_connection()

    def __del__(self):
        self.close_all_connections()

