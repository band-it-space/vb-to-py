import asyncio
import aiohttp
from services.hk_signal.get_db_data import get_stock_data_from_db
from services.hk_signal.sell_signals import runAllSellConditions, isSell
from services.hk_signal.buy_signals import runAllBuyConditions, isBuy, OHLCV


async def main(code, trade_date):
    print("start")
    spy_data_raw = get_stock_data_from_db("2800", trade_date, 300)
    code_data_raw = get_stock_data_from_db(code, trade_date, 300)

    spy_data = [OHLCV(bar["date"], bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]) for bar in spy_data_raw]
    code_data = [OHLCV(bar["date"], bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]) for bar in code_data_raw]
    
    if code_data_raw and len(code_data_raw) > 1:
        last_date = code_data_raw[-1]["date"]
        if last_date == trade_date:
            previous_date = code_data_raw[-2]["date"]
            
            url = f"http://ete.stockfisher.com.hk/v1.1/debugHKEX/verifyData?TradeDay={previous_date}&Code={code}&verifyType=signal"
            headers = {
                'x-api-key': '20250702_hkex_data_v',
                'Cookie': 'language=en-US'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    response_data = await response.json()
                    print(response_data)
                    if response_data and len(response_data) > 0:
                        data = response_data[0]
                        if 'position_status' in data:
                            position_status = data['position_status']
                            if position_status == 'F':
                                buySignals = runAllBuyConditions(code_data, trade_date, spy_data)
                                # print(buySignals)
                                buy = isBuy(buySignals)
                                # print(buy)

                                result = {
                                    "code": code,
                                    "tradeday": trade_date,
                                    "position_status": position_status,
                                    "next_open_action": "B" if buy else "N",
                                    "exit1": buySignals.get('stopLoss'),
                                    "entry_price": 0,
                                    "close": code_data[-1].close if code_data else None
                                }
                                print(result)
                                return result

                            elif position_status == 'I':
                                entry_date = data.get('entry_date')
                                entry_price = data.get('entry_price')
                                exit1 = data.get('exit1')

                                sellSignals = runAllSellConditions(code_data, spy_data, entry_date, entry_price, exit1)
                                print(sellSignals)
                                sell = isSell(sellSignals)
                                # print(sell)
                                
                                result = {
                                    "code": code,
                                    "tradeday": trade_date,
                                    "position_status": position_status,
                                    "next_open_action": "S" if sell else "N",
                                    "exit1": data.get('exit1'),
                                    "entry_price": data.get('entry_price'),
                                    "close": code_data[-1].close if code_data else None
                                }
                                print(result)
                                return result
    


if __name__ == "__main__":
    asyncio.run(main(2800, "2025-04-10"))