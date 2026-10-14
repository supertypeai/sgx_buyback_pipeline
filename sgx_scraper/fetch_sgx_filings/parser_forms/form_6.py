from .base_parser import BaseFormParser 
from .transaction_record_builder import TransactionRecordBuilder


class Form6Parser(BaseFormParser):
    FILING_TYPE = "form_6"

    def parse_records(self) -> list[dict]:
        symbol = self.extract_symbol()
        company_name, sector, sub_sector = self.get_sector_and_sub_sector(symbol=symbol)

        holder_name = self.find_value_below_label("Name of Trustee-Manager/Responsible Person")

        return TransactionRecordBuilder(self).build(
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            sub_sector=sub_sector,
            holder_name=holder_name,
        )
        
