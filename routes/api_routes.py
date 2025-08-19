from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from services.hk_ta import HK_TA
from config.logger import setup_logger


route_logger = setup_logger("api_routes")
logger = route_logger


router = APIRouter()

class SalgoRequest(BaseModel):
    stockname: str
    tradeDay: str

class AlgoResponse(BaseModel):
    status: str
    stockname: str
    tradeDay: str
    message: str
    data_from_sergio_ta: Dict[str, Any] = {}
    # data_from_api_ta: Dict[str, Any] = {}

@router.get("/ping", response_model=Dict[str, str])
async def health_check():
    logger.info("Health check!")
    return {"status": "pong", "service": "fastapi-app"}

@router.post("/start-hk-ta", response_model=AlgoResponse)
async def process_stock(request: SalgoRequest):
    try:
        logger.info(f"Stockname: {request.stockname}")
        
        if not request.stockname or len(request.stockname.strip()) == 0 or not request.tradeDay or len(request.tradeDay.strip()) == 0:
            logger.warning("stockname and tradeDay required!")
            raise HTTPException(
                status_code=400, 
                detail="Stockname required!"
            )
        
        result = await HK_TA.start(request.stockname.strip(), request.tradeDay.strip())
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return AlgoResponse(
            status="success",
            stockname=request.stockname,
            tradeDay=request.tradeDay,
            message=result["message"],
            data_from_sergio_ta=result.get("data_from_sergio_ta", []),
            # data_from_api_ta=result.get("data_from_api_ta", [])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {request.stockname}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        )

