from models.schemas import EnergyStockRecord
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

from typing import Dict, Any, List
from config.logger import setup_logger
from config.settings import Settings
from services.db_service import Database_Service

settings = Settings()
algo_logger = setup_logger("energy_algo")
logger = algo_logger

db_service = Database_Service()

# Constants for file paths
DATA_DIR = settings.data_dir
SIGNAL_FILE_NAME = settings.signal_file_name

class HK_Energy_Algo:
    def __init__(self):
        self.is_running = False

    async def start(self, stockname: str, trade_day: str, stock_data: List[EnergyStockRecord], stock_data_2800: List[EnergyStockRecord]) -> Dict[str, Any]:
        try:
            logger.info(f"Start algo for: {stockname} at day: {trade_day}")
            logger.info(f"stock_data: {len(stock_data)}")
            logger.info(f"stock_data_2800: {len(stock_data_2800)}")

            in_date = datetime.strptime(trade_day, "%Y-%m-%d")
            load_data_date = in_date - relativedelta(months=24)
            logger.info(f"load_data_date algo for: {load_data_date}")
            
            if self.is_running:
                return {
                    "status": "error",
                    "message": "Algo is already running",
                }
            
            self.is_running = True

            processed_data = self._process_stock_data(stock_data, load_data_date)
            processed_data_spy = self._process_stock_data(stock_data_2800, load_data_date)
            
            logger.info(f"Processed data: {processed_data}")
            logger.info(f"Processed 2800 {processed_data_spy}")
            
            
            close = processed_data['close']
            high = processed_data['high']
            low = processed_data['low']
            ddate = processed_data['dates']
            sdate = processed_data['string_dates']
            
            # SPY (reference data) arrays
            close_spy = processed_data_spy['close']
            sdate_spy = processed_data_spy['string_dates']
            
            idx = 0
            arr_idx = 0
            E1, E2, E3, E4, E5 = "", "", "", "", ""
            
            all_indicators = []
            
            for idx in range(len(close)):
                # islatest
                is_latest = "1" if idx == len(close) - 1 else "\\N"
                
                # skip the bars before in_date
                
                if not (ddate[idx] >= in_date or is_latest == "1"):
                    continue
                
                if idx >= 66: #with at least 66 bars for calculation
                    
                    #########################################################################
                    # E1. New high in the past {20D} and close is higher than [Low + {0.65} * (High - Low)]
                    # high > highest(high, 20)[1] and
                    # close > (high - low) * 0.65 + low
                    
                    # Calculate max high in the past 20 days (not including current bar)
                    # idx - 1 --> not to include the high of the last bar
                    start_idx = max(0, idx - 20)
                    end_idx = idx - 1  # not including current bar
                    
                    if end_idx >= start_idx:
                        max_high_20d = max(high[start_idx:end_idx + 1])
                    else:
                        max_high_20d = 0
                    
                    # Check E1 conditions
                    if (high[idx] > max_high_20d and 
                        close[idx] > (high[idx] - low[idx]) * 0.65 + low[idx]):
                        E1 = "1"
                    else:
                        E1 = "0"
                    
                    #########################################################################
                    # E2. StochRSI(10) > 0.5
                    # _lewis_StochRSI(10) > 0.5
                    
                    # RSI(10) - need at least 10 periods
                    
                    rsi_values = self._calculate_rsi(close, idx, 10)
                    rsi_val = rsi_values[-1]  # Current RSI value
                        
                    # Get RSI values for the past 10 periods for StochRSI calculation
                    if len(rsi_values) >= 10:
                        recent_rsi = rsi_values[-10:]  # Last 10 RSI values
                            
                        # highest(RSI, 10)
                        max_val = max(recent_rsi)
                            
                        # lowest(RSI, 10)  
                        min_val = min(recent_rsi)
                            
                        # Calculate StochRSI
                        if min_val == max_val:
                            stochrsi = 0
                        else:
                            stochrsi = (rsi_val - min_val) / (max_val - min_val)
                            
                        # Check E2 condition
                        if stochrsi > 0.5:
                            E2 = "1"
                        else:
                            E2 = "0"
                    else:
                        E2 = "0"
                    
                    #########################################################################
                    # E3. SLOPE(Close, {66}) > 0
                    # (close - close[66])/66
                    
                    if (close[idx] - close[idx - 66]) / 66 > 0:
                        E3 = "1"
                    else:
                        E3 = "0"
                    
                    #########################################################################
                    # E4. Price change over 33 days outperforming SPY
                    # close/close[33] > close data(2)/close[33] data(2)
                    
                    # Find matching date in SPY data
                    try:
                        arr_idx = sdate_spy.index(sdate[idx])
                        
                        # Check if we have enough data (33 days back) for both stock and SPY
                        if (idx >= 33 and arr_idx >= 33 and 
                            arr_idx < len(close_spy) and idx < len(close)):
                            
                            stock_performance = close[idx] / close[idx - 33]
                            spy_performance = close_spy[arr_idx] / close_spy[arr_idx - 33]
                            
                            if stock_performance > spy_performance:
                                E4 = "1"
                            else:
                                E4 = "0"
                        else:
                            E4 = "0" 
                            
                    except ValueError:
                        E4 = "0"
                    
                    #########################################################################
                    # E5. Latest price is at top half of 5-day range and current price > price of 5 days ago 
                    # and current price is less than 7% drawdown from 66D high
                    # (close - lowest(low, 5))/(highest(high, 5) - lowest(low, 5)) > 0.5
                    # close - close[5] > 0
                    # (highest(high, 66) - close)/highest(high, 66) < 0.07
                    
                    # Check if we have enough data for all calculations
                        
                    # lowest(low, 5) - get min low of last 5 days including current
                    start_idx_5 = max(0, idx - 4)  # Last 5 days including current
                    min5 = min(low[start_idx_5:idx + 1])
                        
                    # highest(high, 5) - get max high of last 5 days including current
                    max5 = max(high[start_idx_5:idx + 1])
                        
                    # lowest(low, 66) - get min low of last 66 days including current
                    start_idx_66 = max(0, idx - 65)  # Last 66 days including current
                    min66 = min(low[start_idx_66:idx + 1])
                        
                    # highest(high, 66) - get max high of last 66 days including current
                    max66 = max(high[start_idx_66:idx + 1])
                        
                    # Check all three E5 conditions
                    condition1 = (close[idx] - min5) / (max5 - min5) > 0.5 if max5 != min5 else False
                    condition2 = close[idx] - close[idx - 5] > 0 if idx >= 5 else False
                    condition3 = (max66 - close[idx]) / max66 < 0.07 if max66 != 0 else False
                        
                    if condition1 and condition2 and condition3:
                        E5 = "1"
                    else:
                        E5 = "0"
                        
                else:
                    E1 = "\\N"
                    E2 = "\\N"
                    E3 = "\\N"
                    E4 = "\\N"
                    E5 = "\\N"
                
                # If ddate(idx) >= in_date Or islatest = "1" Then
                if ddate[idx] >= in_date or is_latest == "1":
                    # Store indicators for this record
                    all_indicators.append({
                        "stock_code": stockname,
                        "date": sdate[idx],
                        "E1": E1,
                        "E2": E2,
                        "E3": E3,
                        "E4": E4,
                        "E5": E5,
                        "is_latest": is_latest
                    })
            
            # Generate CSV content from all_indicators for file saving
            if all_indicators:
                # Ensure data directory exists
                os.makedirs(DATA_DIR, exist_ok=True)
                
                # Create full file path
                signal_file_path = os.path.join(DATA_DIR, SIGNAL_FILE_NAME)
                
                csv_lines = []
                for record in all_indicators:
                    csv_line = f"{record['stock_code']},{record['date']},66,{record['E1']},{record['E2']},{record['E3']},{record['E4']},{record['E5']},\\N,{record['is_latest']}\n"
                    csv_lines.append(csv_line)
                
                output_content = "".join(csv_lines)
                with open(signal_file_path, "a", encoding="utf-8") as f:
                    f.write(output_content)
            
            result = {
                "status": "success",
                "message": f"Algorithm successfully completed for {stockname}",
                "indicators": all_indicators
            }
            
            logger.info(f"Algorithm successfully completed for {stockname}.Total: {len(all_indicators)}. Indicators: {all_indicators}")
            return result
            
        except Exception as e:
            logger.error(f"Algorithm error for {stockname}: {str(e)}")
            return {
                "status": "error",
                "message": f"Algorithm errors: {str(e)}",
            }
        finally:
            self.is_running = False
    
    def _process_stock_data(self, stock_data: List[EnergyStockRecord], load_data_date: datetime) -> Dict[str, Any]:

        ldate = []
        lsdate = []
        ltime = []
        lopen = []
        lhigh = []
        llow = []
        lclose = []
        lvolume = []
        
        min_date = datetime(2001, 1, 1)
        
        for record in stock_data:
            if not record.date or record.date.strip() == "":
                continue
                
            try:
                record_date = datetime.strptime(record.date, "%Y-%m-%d")
                
                if record_date < load_data_date or record_date < min_date:
                    continue
                
                ldate.append(record_date)
                lsdate.append(record.date)
                ltime.append(record.time)
                lopen.append(record.open)
                lhigh.append(record.high)
                llow.append(record.low)
                lclose.append(record.close)
                lvolume.append(record.volume)
                
            except ValueError as e:
                # Skip invalid date formats
                logger.warning(f"Invalid date format in record: {record.date}, error: {e}")
                continue
        
        return {
            'dates': ldate,
            'string_dates': lsdate,
            'times': ltime,
            'open': lopen,
            'high': lhigh,
            'low': llow,
            'close': lclose,
            'volume': lvolume
        }
    
    def _calculate_rsi(self, prices: List[float], current_idx: int, period: int) -> List[float]:
        if current_idx < period:
            return []
        
        # Get price data up to current index
        price_data = prices[:current_idx + 1]
        
        # Calculate price changes
        deltas = []
        for i in range(1, len(price_data)):
            deltas.append(price_data[i] - price_data[i-1])
        
        # Separate gains and losses
        gains = [max(delta, 0) for delta in deltas]
        losses = [abs(min(delta, 0)) for delta in deltas]
        
        rsi_values = []
        
        # Calculate RSI for each period
        for i in range(period - 1, len(deltas)):
            # Get the period window
            period_gains = gains[max(0, i - period + 1):i + 1]
            period_losses = losses[max(0, i - period + 1):i + 1]
            
            # Calculate average gain and loss
            avg_gain = sum(period_gains) / period
            avg_loss = sum(period_losses) / period
            
            # Calculate RSI
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(rsi)
        
        return rsi_values

HK_Energy_TA = HK_Energy_Algo()
