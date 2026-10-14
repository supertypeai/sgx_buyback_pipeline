from ..base_parser import BaseFormParser
from ..transaction_record_builder import TransactionRecordBuilder


class Form3PartII(BaseFormParser):
    FILING_TYPE = "form_3_part_ii"

    def get_holder_name(self) -> str | None:
        return self.find_value_below_label('Name of Substantial Shareholder/Unitholder')

    def parse_records(self) -> list[dict]:
        symbol = self.extract_symbol()
        company_name, sector, sub_sector = self.get_sector_and_sub_sector(symbol=symbol)

        holder_name = self.get_holder_name()
        
        return TransactionRecordBuilder(self).build(
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            sub_sector=sub_sector,
            holder_name=holder_name,
        )

