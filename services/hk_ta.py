import asyncio
from controllers.hk_energy.hk_energy import call_get_symbol_adjusted_data
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from config.logger import setup_logger
from config.settings import settings
from services.db_service import Database_Service


algo_logger = setup_logger("ta_algo")
logger = algo_logger

db_params = {
    "db": settings.serhio_db,
    "user": settings.serhio_db_user,
    "password": settings.serhio_db_pass,
    "host": settings.serhio_db_host,
    "port": settings.serhio_db_port,
}

db_service = Database_Service(db_params, pool_size=10)

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
            
            # Add retry mechanism for database queries
            max_retries = 3
            rows = None
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Executing query for {stockname} (attempt {attempt + 1}/{max_retries})")
                    rows = await db_service.execute_query(query)
                    
                    if rows:
                        logger.info(f"Query successful for {stockname}: {len(rows)} rows returned")
                        break
                    else:
                        logger.warning(f"Query returned empty result for {stockname} (attempt {attempt + 1})")
                        
                except Exception as e:
                    logger.error(f"Database query failed for {stockname} (attempt {attempt + 1}): {str(e)}")
                    if attempt == max_retries - 1:
                        raise
                
                # Short delay before retry
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
            
            if not rows:
                logger.error(f"No data found for {stockname} after {max_retries} attempts")
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
        trade_day_date = datetime.strptime(tradeDay, '%Y-%m-%d').date()
        rows_2800 = await call_get_symbol_adjusted_data('2800')
        if not rows_2800:
            raise Exception(f"There is no data for 2800")

        data_2800 = []
        for row in rows_2800:
            trade_date = row[2]
            close_price = row[12]
            if close_price is not None and trade_date <= trade_day_date:
                data_2800.append({
                    "close": float(close_price),
                    "date": trade_date
                })

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

    
    except Exception as e:
        logger.error(f"PR calculation error: {str(e)}")
        return {
            "message": str(e)
        }


HK_TA = HK_TA_Algo()
