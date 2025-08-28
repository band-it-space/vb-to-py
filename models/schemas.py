from pydantic import BaseModel
from typing import Dict, Any, List

class CodeesResponse(BaseModel):
    date: str
    codes: List[str]

class AlgoRequest(BaseModel):
    stock_code: str
    trade_day: str


class AlgoResponse(BaseModel):
    status: str
    stockname: str
    tradeDay: str
    message: str
    data_from_sergio_ta: Dict[str, Any] = {}
    # data_from_api_ta: Dict[str, Any] = {}


class EnergyStockRecord(BaseModel):
    stock_code: str
    date: str
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class EnergyAlgoRequest(BaseModel):
    stock_code: str
    stock_data: List[EnergyStockRecord]
    stock_data_2800: List[EnergyStockRecord]
    trade_day: str


class EnergyAlgoResponse(BaseModel):
    status: str
    message: str
    indicators: List[Dict[str, Any]]



class EnergyAlgoRequestTest(BaseModel):
    stock_code: str
    trade_day: str