from pydantic import BaseModel
from typing import Dict, Any, List, Optional

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


# Queue-related schemas
class TaskRequest(BaseModel):
    stock_code: str
    trade_day: str


class TaskQueueResponse(BaseModel):
    message: Optional[str] = None


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    info: Optional[Dict[str, Any]] = None

# HK TA
class HKTaCancelResponse(BaseModel):
    task_id: str
    status: str
    error: Optional[str] = None
class HKTaCheckResponse(BaseModel):
    task_id: str
    status: str
    message: Optional[str] = None

# HK Energy
class HKEnergyRequest(BaseModel):
    trade_day: str
class HKEnergyResponse(BaseModel):
    task_id: str
    status: str
    message: Optional[str] = None
