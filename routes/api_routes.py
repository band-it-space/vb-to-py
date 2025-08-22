from fastapi import APIRouter, HTTPException
from models.schemas import AlgoRequest, AlgoResponse, EnergyAlgoRequest, EnergyAlgoResponse

from typing import Dict
from services.hk_energy import HK_Energy_TA
from services.hk_ta import HK_TA
from config.logger import setup_logger


route_logger = setup_logger("api_routes")
logger = route_logger


router = APIRouter()



@router.get("/ping", response_model=Dict[str, str])
async def health_check():
    logger.info("Health check!")
    return {"status": "pong", "service": "fastapi-app"}

@router.post("/start-hk-ta", response_model=AlgoResponse)
async def process_stock(request: AlgoRequest):
    try:    
        if not request.stockname or len(request.stockname.strip()) == 0 or not request.tradeDay or len(request.tradeDay.strip()) == 0:
            logger.warning("stockname and tradeDay required!")
            raise HTTPException(
                status_code=400, 
                detail="Stockname and tradeDay required!"
            )
        logger.info(f"Stockname: {request.stockname}, TradeDay: {request.tradeDay}")

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

@router.post("/start-hk-energy", response_model=EnergyAlgoResponse)
async def energy_hk(request: EnergyAlgoRequest):
    try:        
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0 or not request.stock_data or len(request.stock_data) == 0 or not request.stock_data_2800 or len(request.stock_data_2800) == 0:
            logger.warning("Missing or invalid required fields!")
            raise HTTPException(
                status_code=400, 
                detail="Missing or invalid required fields!"
            )

        logger.info(f"Stockname: {request.stock_code}, TradeDay: {request.trade_day}, Stock_data: {len(request.stock_data)}")


        result = await HK_Energy_TA.start(request.stock_code.strip(), request.trade_day.strip(), request.stock_data, request.stock_data_2800 )
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return EnergyAlgoResponse(
            status=result["status"],
            message=result["message"],
            indicators=result["indicators"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {request.stock_code}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        )

