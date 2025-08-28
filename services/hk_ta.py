import pandas as pd
import requests
from datetime import datetime
from typing import Dict, Any
from config.logger import setup_logger
from config.settings import settings
from services.db_service import Database_Service


algo_logger = setup_logger("ta_algo")
logger = algo_logger


db_service = Database_Service()

class HK_TA_Algo:
    def __init__(self):
        self.is_running = False


    async def start(self, stockname: str, tradeDay: str) -> Dict[str, Any]:
        try:
            logger.info(f"Start algo for: {stockname} at day: {tradeDay}")
            
            trade_date = datetime.strptime(tradeDay, "%Y-%m-%d").date()
            today = datetime.today().date()

            if today < trade_date:
                logger.info("Trade day is in the future. Exiting.")
                return {
                    "status": "error",
                    "message": "Trade day is in the future. Exiting.",
                }

            if self.is_running:
                return {
                    "status": "error",
                    "message": "Algo is already running",
                    "stockname": stockname,
                    "tradeDay": tradeDay
                }
            
            self.is_running = True

            query = f"""
            CALL get_symbol_adjusted_data('{stockname}');
            """
            rows = await db_service.execute_query(query)
            if not rows:
                return {
                    "status": "error",
                    "message": f"There is no data for {stockname} in database",
                    "stockname": stockname,
                    "tradeDay": tradeDay
                }
            if not any(row[2].strftime('%Y-%m-%d') == tradeDay for row in rows):
                return {
                    "status": "error",
                    "message": f"There is no data for {stockname} at day: {tradeDay} in database",
                    "stockname": stockname,
                    "tradeDay": tradeDay
                    }
            data_for_df = []
            for row in rows:
                data_for_df.append({
                    "high": float(row[6]),
                    "low": float(row[9]), 
                    "close": float(row[12]),
                    "date": row[2]
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

                    # Price relative to 2800/HSI screening options, 1W/1M/3M/6M/1Y
                    pr_2800 = await calculate_pr(df, tradeDay)

                    # RSI 14D
                    df['low_250d'] = df['low'].rolling(window=250, min_periods=1).min()
                    df["RSI_14"] = rsi_multicharts(df["close"], 14)

                    indicators = {
                        'high20': float(df['high_20d'].iloc[-1]) if len(df) > 0 else None,
                        'low20': float(df['low_20d'].iloc[-1]) if len(df) > 0 else None,
                        'high50': float(df['high_50d'].iloc[-1]) if len(df) > 0 else None,
                        'low50': float(df['low_50d'].iloc[-1]) if len(df) > 0 else None,
                        'high250': float(df['high_250d'].iloc[-1]) if len(df) > 0 else None,
                        'low250': float(df['low_250d'].iloc[-1]) if len(df) > 0 else None, 
                        "pr5": pr_2800["PR_5d"],
                        "pr20": pr_2800["PR_20d"],
                        "pr60": pr_2800["PR_60d"],
                        "pr125": pr_2800["PR_125d"],
                        "pr250": pr_2800['PR_250d'],
                        'rsi14': round(float(df['RSI_14'].iloc[-1]), 3) if len(df) > 0 and not pd.isna(df['RSI_14'].iloc[-1]) else None,
                        'used_days_for_calculation': len(df),
                        'date_range': {
                            'from': df['date'].min().strftime('%Y-%m-%d') if len(df) > 0 else None,
                            'to': df['date'].max().strftime('%Y-%m-%d') if len(df) > 0 else None
                        }
                    }
                    
                    result = {
                        "status": "success",
                        "tradeDay": tradeDay,
                        "message": f"Algorithm successfully completed for {stockname}",
                        "stockname": stockname,
                        "data_from_sergio_ta": indicators,
                        
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
    

# async def fetch_hkex_api_data(stock_code: str, tradeDay: str) -> Any:
#     try:
#         api_url = f"http://ete.stockfisher.com.hk/v1.1/debugHKEX/verifyData"
#         clean_code = str(stock_code).replace(".HK", "")
#         logger.info(f"stockname: {clean_code}")
#         params = {
#             "verifyType": "price",
#             "Code": int(clean_code)
#         }
        
#         headers = {
#             'x-api-key': settings.api_key
#         }

#         response = requests.get(api_url, params=params, headers=headers, timeout=30)
#         response.raise_for_status()
        
#         api_data = response.json()
        
#         if not api_data or not isinstance(api_data, list):
#             logger.warning(f"KL API return empty data for {stock_code}")
#             return {
#                 "status": "error",
#                 "message": f"There is no data for {stock_code} at day: {tradeDay}",
#                 "data": {}
#             }
#         trade_day_date = datetime.strptime(tradeDay, '%Y-%m-%d').date()
        
#         processed_data = []
#         for item in api_data:
#             try:
                
#                 trade_date = datetime.fromisoformat(item["TradeDay"].replace("Z", "+00:00")).date()
                
#                 if (item["high"] is not None 
#                 and item["low"] is not None 
#                 and item["close"] is not None 
#                 and trade_date <= trade_day_date
#                 ):
                    
#                     processed_data.append({
#                         "symbol": f"{clean_code}.HK",
#                         "high": float(item["high"]),
#                         "low": float(item["low"]),
#                         "close": float(item["close"]),
#                         "date": trade_date
#                     })
#             except (ValueError, KeyError, TypeError) as e:
#                 logger.warning(f"KL API error: {e}")
#                 continue
        
#         if not processed_data:
#             logger.warning(f"There is no data for {stock_code} at day: {tradeDay}")
#             return 'No data in API'
        
#         df = pd.DataFrame(processed_data)
#         df['date'] = pd.to_datetime(df['date'])
#         df = df.sort_values('date')
#         df['high_20d'] = df['high'].rolling(window=20, min_periods=1).max()
#         df['low_20d'] = df['low'].rolling(window=20, min_periods=1).min()
#         df['high_50d'] = df['high'].rolling(window=50, min_periods=1).max()
#         df['low_50d'] = df['low'].rolling(window=50, min_periods=1).min()
#         df['high_250d'] = df['high'].rolling(window=250, min_periods=1).max()
#         df['low_250d'] = df['low'].rolling(window=250, min_periods=1).min()

#         # Price relative to 2800/HSI screening options, 1W/1M/3M/6M/1Y
#         pr_2800 = await calculate_pr(df, tradeDay)
#         # RSI 14D
#         df["RSI_14"] = rsi_multicharts(df["close"], 14)

#         latest_indicators = {
#             'high_20d': float(df['high_20d'].iloc[-1]) if len(df) > 0 else None,
#             'low_20d': float(df['low_20d'].iloc[-1]) if len(df) > 0 else None,
#             'high_50d': float(df['high_50d'].iloc[-1]) if len(df) > 0 else None,
#             'low_50d': float(df['low_50d'].iloc[-1]) if len(df) > 0 else None,
#             'high_250d': float(df['high_250d'].iloc[-1]) if len(df) > 0 else None,
#             'low_250d': float(df['low_250d'].iloc[-1]) if len(df) > 0 else None,
#             "pr5": pr_2800["PR_5d"],
#             "pr20": pr_2800["PR_20d"],
#             "pr60": pr_2800["PR_60d"],
#             "pr125": pr_2800["PR_125d"],
#             "pr250": pr_2800['PR_250d'],
#             'rsi14': round(float(df['RSI_14'].iloc[-1]), 3) if len(df) > 0 and not pd.isna(df['RSI_14'].iloc[-1]) else None,
#             'used_days_for_calculation': len(df),
#             'date_range': {
#                 'from': df['date'].min().strftime('%Y-%m-%d') if len(df) > 0 else None,
#                 'to': df['date'].max().strftime('%Y-%m-%d') if len(df) > 0 else None
#             }
#         }
        
        
#         logger.info(f"API дані оброблено для {clean_code}.HK: {latest_indicators}")
        
#         return latest_indicators
        
#     except requests.RequestException as e:
#         logger.error(f"Помилка HTTP запиту для {stock_code}: {str(e)}")
#         return {
#             "status": "error",
#             "message": f"Помилка з'єднання з API: {str(e)}",
#             "stock_code": stock_code,
#             "data": {}
#         }
#     except Exception as e:
#         logger.error(f"Загальна помилка обробки API даних для {stock_code}: {str(e)}")
#         return {
#             "status": "error",
#             "message": f"Помилка обробки даних: {str(e)}",
#             "stock_code": stock_code,
#             "data": {}
#         }


def rsi_multicharts(close: pd.Series, period: int = 14) -> pd.Series:

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = pd.Series(dtype="float64", index=close.index)
    avg_loss = pd.Series(dtype="float64", index=close.index)

    avg_gain.iloc[period] = gain.iloc[1:period+1].mean()
    avg_loss.iloc[period] = loss.iloc[1:period+1].mean()

    for i in range(period+1, len(close)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period

    # RS та RSI
    rs = avg_gain / avg_loss
    rsi = pd.Series(index=close.index, dtype="float64")
    # Exceptions:
    # - avg_loss == 0 & avg_gain > 0 → RSI = 100
    # - avg_gain == 0 & avg_loss > 0 → RSI = 0
    # - avg_gain == 0 & avg_loss == 0 → RSI = 50
    mask_loss_zero = (avg_loss == 0)
    mask_gain_zero = (avg_gain == 0)

    rsi[~mask_loss_zero & ~mask_gain_zero] = 100 - (100 / (1 + rs[~mask_loss_zero & ~mask_gain_zero]))
    rsi[mask_loss_zero & ~mask_gain_zero] = 100.0
    rsi[~mask_loss_zero & mask_gain_zero] = 0.0
    rsi[mask_loss_zero & mask_gain_zero] = 50.0

    return rsi

async def calculate_pr(df_stock: pd.DataFrame, tradeDay: str, periods=(5, 20, 60, 125, 250)) -> Any:
    try:
        api_url = f"http://ete.stockfisher.com.hk/v1.1/debugHKEX/verifyData"
        params = {
            "verifyType": "price",
            "Code": "2800"
        }
        
        headers = {
            'x-api-key': settings.api_key
        }

        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        
        api_data = response.json()
        
        if not api_data or not isinstance(api_data, list):
            logger.warning(f"KL API return empty data for 2800")
            return {
                "status": "error",
                "message": f"There is no data for 2800",
                "data": {}
            }
        trade_day_date = datetime.strptime(tradeDay, '%Y-%m-%d').date()
        logger.info(f"KL API 2800 data: {trade_day_date}")
        logger.info(f"API response: {len(api_data)}")
        
        data_2800 = []
        for item in api_data:
            try:
                
                trade_date = datetime.fromisoformat(item["TradeDay"].replace("Z", "+00:00")).date()
                
                if ( item["close"] is not None
                and trade_date <= trade_day_date
                ):
                    
                    data_2800.append({
                        "close": float(item["close"]),
                        "date": trade_date
                    })
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"KL API error: {e}")
                continue
        
        if not data_2800:
            logger.warning(f"There is no data for 2800")
            return 'No data in API'
        
        
        df_bench = pd.DataFrame(data_2800)
        s = df_stock[["date", "close"]].rename(columns={"close": "close_s"})
        b = df_bench[["date", "close"]].rename(columns={"close": "close_b"})


        s["date"] = pd.to_datetime(s["date"])
        b["date"] = pd.to_datetime(b["date"])

        df = pd.merge(s, b, on="date", how="inner").sort_values("date").reset_index(drop=True)

        for n in periods:
            if len(df) > n:
                stock_ratio = df["close_s"] / df["close_s"].shift(n)
                bench_ratio = df["close_b"] / df["close_b"].shift(n)
                pr_values = stock_ratio / bench_ratio
                df[f"PR_{n}d"] = pr_values
            else:
                logger.warning(f"Недостатньо даних для розрахунку PR_{n}d (потрібно {n+1}, маємо {len(df)})")
                df[f"PR_{n}d"] = None
                
            
        last = df.iloc[-1]
        PR_5d   = float(last["PR_5d"])   if pd.notna(last["PR_5d"])   else None
        PR_20d  = float(last["PR_20d"])  if pd.notna(last["PR_20d"])  else None
        PR_60d  = float(last["PR_60d"])  if pd.notna(last["PR_60d"])  else None
        PR_125d = float(last["PR_125d"]) if pd.notna(last["PR_125d"]) else None
        PR_250d = float(last["PR_250d"]) if pd.notna(last["PR_250d"]) else None
        logger.info(f"Фінальні результати PR - 5d: {PR_5d}, 20d: {PR_20d}, 60d: {PR_60d}, 125d: {PR_125d}, 250d: {PR_250d}")

        return {
            "PR_5d": round(PR_5d, 3) if PR_5d is not None else None,
            "PR_20d": round(PR_20d, 3) if PR_20d is not None else None,
            "PR_60d": round(PR_60d, 3) if PR_60d is not None else None,
            "PR_125d": round(PR_125d, 3) if PR_125d is not None else None,
            "PR_250d": round(PR_250d, 3) if PR_250d is not None else None,
        }

    except requests.RequestException as e:
        logger.error(f"Помилка HTTP запиту для 2800: {str(e)}")
        return {
            "message": f"Помилка HTTP запиту для 2800: {str(e)}",
            "data": {}
        }
    except Exception as e:
        logger.error(f"Загальна помилка обробки API даних для 2800: {str(e)}")
        return {
            "message": f"Помилка обробки даних: {str(e)}",
            "data": {}
        }


HK_TA = HK_TA_Algo()
