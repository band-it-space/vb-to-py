from controllers.get_stocks_codes import get_stocks_codes
from fastapi import APIRouter, HTTPException
from models.schemas import AlgoRequest, AlgoResponse, EnergyAlgoRequest, EnergyAlgoResponse, EnergyAlgoRequestTest, EnergyStockRecord, CodeesResponse

from typing import Dict
from services.hk_energy import HK_Energy_TA
from services.hk_ta import HK_TA
from config.logger import setup_logger

import aiomysql

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
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("stockname and tradeDay required!")
            raise HTTPException(
                status_code=400, 
                detail="Stockname and tradeDay required!"
            )
        logger.info(f"Stockname: {request.stock_code}, TradeDay: {request.trade_day}")

        result = await HK_TA.start(request.stock_code.strip(), request.trade_day.strip())
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return AlgoResponse(
            status="success",
            stockname=request.stock_code,
            tradeDay=request.trade_day,
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

@router.post("/start-hk-energy-test", response_model=EnergyAlgoResponse)
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

@router.post("/start-hk-energy", response_model=EnergyAlgoResponse)
async def energy_hk_test(request: EnergyAlgoRequestTest):
    try:        
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("Missing or invalid required fields!")
            raise HTTPException(
                status_code=400, 
                detail="Missing or invalid required fields!"
            )

        logger.info(f"Stockname: {request.stock_code}, TradeDay: {request.trade_day}")
        connection_params = {
            'host': 'mdbinstance-cluster.cluster-cgbcqc4g9atp.ap-southeast-1.rds.amazonaws.com',
            'port': 3306,
            'user': 'mdb_admin',
            'password': 'Gc5H9EEevfhbo16n',
            'db': 'mdb_v2',
            'charset': 'utf8mb4',
            'autocommit': True}
        try:
            pool = await aiomysql.create_pool(**connection_params)
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error(f"Error creating database pool: {e}")
            raise
        
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = f"""SELECT 
                            CONCAT(
                                DATE_FORMAT(tradeday, '%Y-%m-%d'), ',',
                                TIME(tradeday), ',',
                                COALESCE(adj_open, adj_close), ',',
                                COALESCE(adj_high, adj_close), ',',
                                COALESCE(adj_low, adj_close), ',',
                                COALESCE(adj_close, 0), ',',
                                COALESCE(adj_volume, 0)
                            ) AS csv_row
                        FROM signal_hkex_price
                        WHERE code = '{request.stock_code.strip()}'
                            AND tradeday >= '2001-01-01'
                            AND IFNULL(adj_close, 0) > 0
                            AND tradeday <= '{request.trade_day.strip()}'
                        ORDER BY tradeday;"""
                    await cursor.execute(query)
                    results = await cursor.fetchall()
                    
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise
        logger.info(f"Query executed successfully, results length: {len(results)}")
        stock_data = []
        if results:
            for row in results:
                if row[0] is not None:
                    date, time, open_price, high, low, close, volume = row[0].split(",") 

                    stock_record = EnergyStockRecord(
                        stock_code=request.stock_code.strip(),
                        date=date,
                        time=time,
                        open=float(open_price),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=float(volume)
                    )
                    stock_data.append(stock_record) 
        logger.info(f"Fetched {len(stock_data)} records from database") 
        
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    query = f"""SELECT 
                            CONCAT(
                                DATE_FORMAT(tradeday, '%Y-%m-%d'), ',',
                                TIME(tradeday), ',',
                                COALESCE(adj_open, adj_close), ',',
                                COALESCE(adj_high, adj_close), ',',
                                COALESCE(adj_low, adj_close), ',',
                                COALESCE(adj_close, 0), ',',
                                COALESCE(adj_volume, 0)
                            ) AS csv_row
                        FROM signal_hkex_price
                        WHERE code = '2800'
                            AND tradeday >= '2001-01-01'
                            AND IFNULL(adj_close, 0) > 0
                            AND tradeday <= '{request.trade_day.strip()}'
                        ORDER BY tradeday;"""
                    await cursor.execute(query)
                    results_2800 = await cursor.fetchall()
                    
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            raise

        logger.info(f"Query executed successfully, results_2800 length: {len(results_2800)}")

        stock_data_2800 = []
        if results_2800:
            for row in results_2800:
                if row[0] is not None:
                    date, time, open_price, high, low, close, volume = row[0].split(",") 

                    stock_record = EnergyStockRecord(
                        stock_code='2800.HK',
                        date=date,
                        time=time,
                        open=float(open_price),
                        high=float(high),
                        low=float(low),
                        close=float(close),
                        volume=float(volume)
                    )
                    stock_data_2800.append(stock_record) 
        logger.info(f"Fetched stock_data_2800 {len(stock_data_2800)} records from database") 

        result = await HK_Energy_TA.start(request.stock_code.strip(), request.trade_day.strip(), stock_data, stock_data_2800 )
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
    
        return EnergyAlgoResponse(
            status=result["status"] ,
            message=result["message"] ,
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

@router.get("/codes", response_model=CodeesResponse)
async def get_codes():
    codes = await get_stocks_codes()
    if codes:
        return {"date": codes["date"], "codes": codes["codes"]}
    else:
        return {"date": "", "codes": []}