from .base_parser import BaseFormParser 
from .transaction_record_builder import TransactionRecordBuilder
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import classify_holder_type


class Form6Parser(BaseFormParser): 
    def parse_records(self) -> list[dict]:
        symbol = self.extract_symbol()
        company_name, sector, sub_sector = self.get_sector_and_sub_sector(symbol=symbol)

        holder_name = self.find_value_below_label("Name of Trustee-Manager/Responsible Person")
        holder_type = classify_holder_type(holder_name)

        return TransactionRecordBuilder(self).build(
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            sub_sector=sub_sector,
            holder_name=holder_name,
            holder_type=holder_type,
        )
        
