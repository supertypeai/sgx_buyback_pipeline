from sgx_scraper.alerting.utils.send_alert_helper import get_price

import math
import json 
import logging


LOGGER = logging.getLogger(__name__)


def filter_sgx_filings(payload: dict[str, any]) -> bool:
    holding_before = payload.get('holding_before')
    holding_after = payload.get('holding_after')
    amount_transaction = payload.get('amount_transaction')
    transaction_type = payload.get('transaction_type')
    transaction_value = payload.get('transaction_value')
    price_per_share = payload.get('price_per_share')
    symbol = payload.get('symbol')
    timestamp = payload.get('timestamp')
    holder_name = payload.get('holder_name')

    reasons = []

    basic_fields = [symbol, timestamp, holding_before, holding_after]

    if any(field is None for field in basic_fields):
        reasons.append(
            f'Missing one or more required fields: '
            f'symbol={symbol}, timestamp={timestamp}, '
            f'holding_before={holding_before}, holding_after={holding_after}'
        )

    if transaction_type is None:
        reasons.append('Missing transaction_type')

    if transaction_type:
        if not all([transaction_value, amount_transaction, price_per_share]):
            reasons.append(
                f'Transaction type is "{transaction_type}" but missing financial data: '
                f'transaction_value={transaction_value}, amount_transaction={amount_transaction}, '
                f'price_per_share={price_per_share}'
            )

    diff_shares = None

    if holding_after and holding_before:
        diff_shares = holding_after - holding_before

    if amount_transaction and diff_shares is not None:
        if abs(diff_shares) != amount_transaction:
            reasons.append(
                f'Difference in shares (after - before = {abs(diff_shares)}) '
                f'does not match reported amount_transaction={amount_transaction}'
            )

    if transaction_type:
        if transaction_type == 'transfer':
            reasons.append(
                f'Please verify the holder name for transfer type. '
                f'The format {holder_name} may not always match the current regex. '
                f'Since we need to have a sign [->]'
            )
        if transaction_type == 'award':
            reasons.append(
                f'Please double check the transaction type: {transaction_type}. '
                f'The document may contain multiple descriptions that affect the classification.'
            )
        
    # Unrealistic or inconsistent price
    if price_per_share:
        if price_per_share > 200:
            reasons.append(
                f'Unrealistic price_per_share={price_per_share} (>200)'
            )
        else:
            market_price_yfinance = get_price(symbol, timestamp)
            if market_price_yfinance:
                deviation = abs(price_per_share - market_price_yfinance) / market_price_yfinance
                if deviation > 0.4:
                    reasons.append(
                        f'Price deviation too large: filing price={price_per_share}, '
                        f'market price={market_price_yfinance}, deviation={deviation:.2%}'
                    )
                    
    # Value and price inconsistency
    if transaction_value and amount_transaction and price_per_share:
        calculated_price = transaction_value / amount_transaction
        if not math.isclose(calculated_price, price_per_share, rel_tol=0.05):
            reasons.append(
                f'Calculated price (transaction_value/amount_transaction={calculated_price:.2f}) '
                f'does not match reported price_per_share={price_per_share}'
            )

        expected_value = amount_transaction * price_per_share
        if not math.isclose(expected_value, transaction_value, rel_tol=0.05):
            reasons.append(
                f'Inconsistent total value: expected {expected_value:.2f} '
                f'(amount_transaction * price_per_share), but got {transaction_value:.2f}'
            )
           
    if reasons:
        payload['reasons'] = reasons 
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
    return data_insertable, data_not_insertable


if __name__ == '__main__':
    data_path = 'data/scraper_output/sgx_filing/sgx_filings_today_alert.json' 
    with open(data_path, 'r') as file:
        data = json.load(file)
    print(len(data))
    data_insertable, data_not_insertable = get_data_alert(data)
    print(f'check data not insertable sample: {data_not_insertable[:2]} | \ndata insertable sample: {data_insertable[:2]}')
    
    # uv run -m src.alerting.filter_data_alert