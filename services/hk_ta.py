import pandas as pd
import pymysql
import requests
from datetime import datetime
from typing import Dict, Any, List
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

class HK_TA_Algo:
    def __init__(self):
        self.is_running = False
        self.db= pymysql.connect(**db_params)

    def calculate_rsi(self, close_prices, period=14):
        """Calculate RSI using standard method"""
        # Взяти тільки останні period+1 значень для точного розрахунку
        if len(close_prices) > period + 1:
            close_prices = close_prices.tail(period + 1).reset_index(drop=True)
        
        logger.info(f"Close prices: {close_prices}")
        logger.info(f"RSI calculation - using last {len(close_prices)} prices")
        logger.info(f"Last 5 prices: {close_prices.tail(5).tolist()}")
        
        delta = close_prices.diff()
        logger.info(f"Last 5 deltas: {delta.tail(5).tolist()}")
        
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        if len(gains) >= period:
            avg_gain_first = gains.iloc[1:period+1].mean() 
            avg_loss_first = losses.iloc[1:period+1].mean()
            
            avg_gain = pd.Series(index=gains.index, dtype=float)
            avg_loss = pd.Series(index=losses.index, dtype=float)
            
            avg_gain.iloc[period] = avg_gain_first
            avg_loss.iloc[period] = avg_loss_first
            
            for i in range(period + 1, len(gains)):
                avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period-1) + gains.iloc[i]) / period
                avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period-1) + losses.iloc[i]) / period
        else:
            
            avg_gain = gains.rolling(window=period, min_periods=1).mean()
            avg_loss = losses.rolling(window=period, min_periods=1).mean()
        
        logger.info(f"Avg gain: {avg_gain.iloc[-1] if len(avg_gain) > 0 else None}, Avg loss: {avg_loss.iloc[-1] if len(avg_loss) > 0 else None}")
        
        rs = avg_gain / avg_loss.replace(0, float('inf'))
        rsi = 100 - (100 / (1 + rs))
        
        final_rsi = rsi.iloc[-1] if len(rsi) > 0 and not pd.isna(rsi.iloc[-1]) else None
        logger.info(f"Final RSI: {final_rsi}")
        
        return rsi

    async def start(self, stockname: str) -> Dict[str, Any]:

        try:
            logger.info(f"Запуск алгоритму для акції: {stockname}")
            
            
            
            if self.is_running:
                logger.warning("Алгоритм вже запущений")
                return {
                    "status": "error",
                    "message": "Алгоритм вже виконується",
                    "stockname": stockname
                }
            
            self.is_running = True
            from_chan_api = await fetch_hkex_api_data(stockname)

            logger.info(f"Початок обробки акції {stockname}")
            
            query = f"""
            SELECT symbol, high, low, close, date 
            FROM hkex_stock_price 
            WHERE symbol = '{stockname}' 
            AND date >= CURDATE() - INTERVAL 400 DAY 
            AND WEEKDAY(date) < 5 
            ORDER BY date DESC
            LIMIT 500
            """

            with self.db.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                data_for_df = []
                for row in rows:
                    data_for_df.append({
                        "symbol": row[0],
                        "high": float(row[1]),
                        "low": float(row[2]), 
                        "close": float(row[3]),
                        "date": row[4]
                    })

                if not data_for_df:
                    result = {
                        "status": "error",
                        "message": f"There is no data for {stockname}",
                        "stockname": stockname,
                        "data": {}
                    }
                else:
                    # 20D/50D/250D High Low
                    df = pd.DataFrame(data_for_df)
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    
                    df['high_20d'] = df['high'].rolling(window=20, min_periods=1).max()
                    df['low_20d'] = df['low'].rolling(window=20, min_periods=1).min()
                    df['high_50d'] = df['high'].rolling(window=50, min_periods=1).max()
                    df['low_50d'] = df['low'].rolling(window=50, min_periods=1).min()
                    df['high_250d'] = df['high'].rolling(window=250, min_periods=1).max()
                    df['low_250d'] = df['low'].rolling(window=250, min_periods=1).min()
                    
                    # RSI 14D
                    df = df.sort_values('date').reset_index(drop=True) 
                    close_prices = df['close'].tail(15)
                    logger.info(f"Using last 15 days for RSI 14-day calculation")
                    rsi_14d = self.calculate_rsi(close_prices, period=14)

                    indicators = {
                        'high20': float(df['high_20d'].iloc[-1]) if len(df) > 0 else None,
                        'low20': float(df['low_20d'].iloc[-1]) if len(df) > 0 else None,
                        'high50': float(df['high_50d'].iloc[-1]) if len(df) > 0 else None,
                        'low50': float(df['low_50d'].iloc[-1]) if len(df) > 0 else None,
                        'high250': float(df['high_250d'].iloc[-1]) if len(df) > 0 else None,
                        'low250': float(df['low_250d'].iloc[-1]) if len(df) > 0 else None, 
                        'rsi14': float(rsi_14d.iloc[-1]) if len(rsi_14d) > 0 else None,
                        'data_points_used': len(df),
            'date_range': {
                'from': df['date'].min().strftime('%Y-%m-%d') if len(df) > 0 else None,
                'to': df['date'].max().strftime('%Y-%m-%d') if len(df) > 0 else None
            }
                    }
                    
                    result = {
                        "status": "success",
                        "message": f"Algorithm successfully completed for {stockname}",
                        "stockname": stockname,
                        "data": indicators,
                        "api_data": from_chan_api
                        
                    }
            
            logger.info(f"Algorithm successfully completed for {stockname}")
            return result
            
        except Exception as e:
            logger.error(f"Algorithm error for {stockname}: {str(e)}")
            return {
                "status": "error",
                "message": f"Algorithm errors: {str(e)}",
                "stockname": stockname
            }
        finally:
            self.is_running = False
    

async def fetch_hkex_api_data(stock_code: str) -> Any:
    try:
        api_url = f"http://ete.stockfisher.com.hk/v1.1/debugHKEX/verifyData"
        clean_code = str(stock_code).replace(".HK", "")
        
        params = {
            "verifyType": "price",
            "Code": int(clean_code)
        }
        
        headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive", 
            "x-api-key": "20250702_hkex_data_v",
            "language": "en-US"
        }
        
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        api_data = response.json()
        
        if not api_data or not isinstance(api_data, list):
            logger.warning(f"API повернув пусті дані для {stock_code}")
            return {
                "status": "error",
                "message": f"Не знайдено даних для акції {stock_code}",
                "data": {}
            }
        
        
        processed_data = []
        for item in api_data:
            try:
                
                trade_date = datetime.fromisoformat(item["TradeDay"].replace("Z", "+00:00")).date()
                
                if (item["high"] is not None and item["low"] is not None and 
                    item["close"] is not None and item["open"] is not None):
                    
                    processed_data.append({
                        "symbol": f"{clean_code}.HK",
                        "high": float(item["high"]),
                        "low": float(item["low"]),
                        "close": float(item["close"]),
                        "open": float(item["open"]),
                        "volume": int(item["volume"]) if item["volume"] else 0,
                        "date": trade_date
                    })
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Помилка обробки запису API: {e}")
                continue
        
        if not processed_data:
            logger.warning(f"Не знайдено валідних даних для {stock_code}")
            return 'No data in API'
        
        df = pd.DataFrame(processed_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        df['high_20d'] = df['high'].rolling(window=20, min_periods=1).max()
        df['low_20d'] = df['low'].rolling(window=20, min_periods=1).min()
        df['high_50d'] = df['high'].rolling(window=50, min_periods=1).max()
        df['low_50d'] = df['low'].rolling(window=50, min_periods=1).min()
        df['high_250d'] = df['high'].rolling(window=250, min_periods=1).max()
        df['low_250d'] = df['low'].rolling(window=250, min_periods=1).min()
        
        # RSI 14D
        df = df.sort_values('date').reset_index(drop=True) 
        close_prices = df['close'].tail(15)
        logger.info(f"Using last 15 days for RSI 14-day calculation")
        rsi_14d = rsi_new(close_prices, period=14)

        latest_indicators = {
            'high_20d': float(df['high_20d'].iloc[-1]) if len(df) > 0 else None,
            'low_20d': float(df['low_20d'].iloc[-1]) if len(df) > 0 else None,
            'high_50d': float(df['high_50d'].iloc[-1]) if len(df) > 0 else None,
            'low_50d': float(df['low_50d'].iloc[-1]) if len(df) > 0 else None,
            'high_250d': float(df['high_250d'].iloc[-1]) if len(df) > 0 else None,
            'low_250d': float(df['low_250d'].iloc[-1]) if len(df) > 0 else None,
            'rsi14': float(rsi_14d.iloc[-1]) if len(rsi_14d) > 0 else None,
            'data_points_used': len(df),
            'date_range': {
                'from': df['date'].min().strftime('%Y-%m-%d') if len(df) > 0 else None,
                'to': df['date'].max().strftime('%Y-%m-%d') if len(df) > 0 else None
            }
        }
        
        
        logger.info(f"API дані оброблено для {clean_code}.HK: {latest_indicators}")
        
        return latest_indicators
        
    except requests.RequestException as e:
        logger.error(f"Помилка HTTP запиту для {stock_code}: {str(e)}")
        return {
            "status": "error",
            "message": f"Помилка з'єднання з API: {str(e)}",
            "stock_code": stock_code,
            "data": {}
        }
    except Exception as e:
        logger.error(f"Загальна помилка обробки API даних для {stock_code}: {str(e)}")
        return {
            "status": "error",
            "message": f"Помилка обробки даних: {str(e)}",
            "stock_code": stock_code,
            "data": {}
        }

def rsi_new(close_prices, period=14):
        """Calculate RSI using standard method"""
        # Взяти тільки останні period+1 значень для точного розрахунку
        if len(close_prices) > period + 1:
            close_prices = close_prices.tail(period + 1).reset_index(drop=True)
        
        logger.info(f"Close prices: {close_prices}")
        logger.info(f"RSI calculation - using last {len(close_prices)} prices")
        logger.info(f"Last 5 prices: {close_prices.tail(5).tolist()}")
        
        delta = close_prices.diff()
        logger.info(f"Last 5 deltas: {delta.tail(5).tolist()}")
        
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        if len(gains) >= period:
            avg_gain_first = gains.iloc[1:period+1].mean() 
            avg_loss_first = losses.iloc[1:period+1].mean()
            
            avg_gain = pd.Series(index=gains.index, dtype=float)
            avg_loss = pd.Series(index=losses.index, dtype=float)
            
            avg_gain.iloc[period] = avg_gain_first
            avg_loss.iloc[period] = avg_loss_first
            
            for i in range(period + 1, len(gains)):
                avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period-1) + gains.iloc[i]) / period
                avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period-1) + losses.iloc[i]) / period
        else:
            
            avg_gain = gains.rolling(window=period, min_periods=1).mean()
            avg_loss = losses.rolling(window=period, min_periods=1).mean()
        
        logger.info(f"Avg gain: {avg_gain.iloc[-1] if len(avg_gain) > 0 else None}, Avg loss: {avg_loss.iloc[-1] if len(avg_loss) > 0 else None}")
        
        rs = avg_gain / avg_loss.replace(0, float('inf'))
        rsi = 100 - (100 / (1 + rs))
        
        final_rsi = rsi.iloc[-1] if len(rsi) > 0 and not pd.isna(rsi.iloc[-1]) else None
        logger.info(f"Final RSI: {final_rsi}")
        
        return rsi

HK_TA = HK_TA_Algo()
