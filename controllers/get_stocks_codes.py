import requests
import pandas as pd
import io
import re
from datetime import datetime
from typing import List, Dict
from config.logger import setup_logger

logger = setup_logger("stocks_codes")

def extract_date_from_text(text: str) -> str | None:
    date_pattern = r'(\d{2}/\d{2}/\d{4})'
    match = re.search(date_pattern, text)
    if match:
        return match.group(1)
    return None

def is_date_today(date_str: str) -> bool:
    try:
        file_date = datetime.strptime(date_str, "%d/%m/%Y")
        today = datetime.now()
        
        return True #! REMOVE
    except ValueError:
        return False

def is_numeric_code(value: str) -> bool:
    try:
        code = int(value.strip())
        return code <= 9999
    except ValueError:
        return False

exclude_ranges = [
    (2900, 2999),
    (4000, 4199),
    (4200, 4299),
    (4300, 4329),
    (4400, 4599),
    (4600, 4699),
    (4700, 4799),
    (4800, 4999),
    (5000, 6029),
    (6200, 6299),
    (6750, 7699),
    (7800, 7999),
    (8510, 8600),
]

async def get_stocks_codes() -> Dict[str,str | List[str]]:
    try:
        url = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        excel_content = io.BytesIO(response.content)
        
        df = pd.read_excel(excel_content, skiprows=1, header=None)
        
        if len(df.columns) > 0:
            date_text = df.iloc[0, 0]
            logger.info(f"Date text: {date_text}")
        
            extracted_date = extract_date_from_text(str(date_text))
            if extracted_date:
                logger.info(f"Extracted date: {extracted_date}")
                
                if is_date_today(extracted_date):
                    dt = datetime.strptime(extracted_date, "%d/%m/%Y")
                    file_date = dt.strftime("%Y-%m-%d")
                else:
                    logger.warning(f"File date {extracted_date} is not current")
                    return {'date': '', 'codes': []}
            else:
                logger.warning("Could not extract date from text")
                return {'date': '', 'codes': []}
            
            first_column = df.iloc[:, 0]
            
            stock_codes = []
            for value in first_column:
                if pd.notna(value) and str(value).strip() and is_numeric_code(value.strip()):
                    stock_codes.append(str(value).strip())
            
            stock_codes = [code for code in stock_codes if not any(start <= int(code) <= end for start, end in exclude_ranges)]
            
            return {'date': file_date, 'codes': stock_codes}
        else:
            logger.warning("Empty file")
            return {'date': '', 'codes': []}
            
    except requests.RequestException as e:
        logger.error(f"File loading error: {str(e)}")
        return {'date': '', 'codes': []}
    except Exception as e:
        logger.error(f"Processing file error: {str(e)}")
        return {'date': '', 'codes': []}
        