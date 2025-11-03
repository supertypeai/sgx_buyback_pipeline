from sgx_scraper.config.settings import LOGGER

from sgx_scraper.alerting.utils.send_alert_helper import get_price

import math
import json 


def filter_sgx_filings(payload: dict[str, any]) -> bool:
    shares_before = payload.get('shares_before')
    shares_after = payload.get('shares_after')
    number_of_stock = payload.get('number_of_stock')
    transaction_type = payload.get('transaction_type')
    value = payload.get('value')
    price_per_share = payload.get('price_per_share') 
    symbol = payload.get('symbol')
    transaction_date = payload.get('transaction_date')

    required_fields = [
        symbol, transaction_date, shares_before,
        shares_after, transaction_type
    ]

    # Missing required field
    if any(field is None for field in required_fields):
        payload.update({
            'reason': f'Missing one or more required fields: '
                      f'symbol={symbol}, transaction_date={transaction_date}, '
                      f'shares_before={shares_before}, shares_after={shares_after}, '
                      f'transaction_type={transaction_type}'
        })
        return True

    diff_shares = shares_after - shares_before

    # Inconsistent share count
    if number_of_stock:
        if diff_shares != number_of_stock:
            payload.update({
                'reason': (
                    f'Difference in shares (after - before = {diff_shares}) '
                    f'does not match reported number_of_stock={number_of_stock}'
                )
            })
            return True

    # Invalid transaction type for share movement
    if transaction_type:
        if diff_shares > 0 and transaction_type != 'buy':
            payload.update({
                'reason': (
                    f'Share difference is positive ({diff_shares}), '
                    f'but transaction_type="{transaction_type}" instead of "buy"'
                )
            })
            return True
        if diff_shares < 0 and transaction_type != 'sell':
            payload.update({
                'reason': (
                    f'Share difference is negative ({diff_shares}), '
                    f'but transaction_type="{transaction_type}" instead of "sell"'
                )
            })
            return True
    
    # Unrealistic or inconsistent price
    if price_per_share:
        if price_per_share > 200:
            payload.update({
                'reason': f'Unrealistic price_per_share={price_per_share} (>200)'
            })
            return True
        else:
            market_price_yfinance = get_price(symbol, transaction_date)
            if market_price_yfinance:
                deviation = abs(price_per_share - market_price_yfinance) / market_price_yfinance
                if deviation > 0.4:
                    payload.update({
                        'reason': (
                            f'Price deviation too large: filing price={price_per_share}, '
                            f'market price={market_price_yfinance}, deviation={deviation:.2%}'
                        )
                    })
                    return True
                
    # Value and price inconsistency
    if value and number_of_stock and price_per_share:
        calculated_price = value / number_of_stock 
        if not math.isclose(calculated_price, price_per_share):
            payload.update({
                'reason': (
                    f'Calculated price (value/number_of_stock={calculated_price:.2f}) '
                    f'does not match reported price_per_share={price_per_share}'
                )
            })
            return True

    return False


def get_data_alert(payload_sgx_filings: list[dict[str, any]]) -> tuple[list[dict[str, any]], list[dict[str, any]]]:
    if not payload_sgx_filings:
        LOGGER.info("No SGX filings data to filter.")
        return [], []

    data_insertable = []
    data_not_insertable = []

    for payload in payload_sgx_filings: 
        if filter_sgx_filings(payload):
            data_not_insertable.append(payload)
        else: 
            data_insertable.append(payload)
    
    LOGGER.info(f'Filtering completed. Insertable: {len(data_insertable)} | Not insertable: {len(data_not_insertable)}')
    print(f'Filtering completed. Insertable: {len(data_insertable)} | Not insertable: {len(data_not_insertable)}')
    return data_insertable, data_not_insertable


if __name__ == '__main__':
    data_path = 'data/scraper_output/sgx_filing/sgx_filings_today_alert.json' 
    with open(data_path, 'r') as file:
        data = json.load(file)
    print(len(data))
    data_insertable, data_not_insertable = get_data_alert(data)
    # print(f'check data not insertable sample: {data_not_insertable[:2]} | \ndata insertable sample: {data_insertable[:2]}')

    # uv run -m src.alerting.filter_data_alert