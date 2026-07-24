from ..base_parser import BaseFormParser
from ..transaction_record_builder import TransactionRecordBuilder
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import classify_holder_type


class Form3PartII(BaseFormParser):
    def get_holder_name(self) -> str | None:
        return self.find_value_below_label('Name of Substantial Shareholder/Unitholder')

    def parse_records(self) -> list[dict]:
        symbol = self.extract_symbol()
        company_name, sector, sub_sector = self.get_sector_and_sub_sector(symbol=symbol)

        holder_name = self.get_holder_name()
        holder_type = classify_holder_type(holder_name)

        return TransactionRecordBuilder(self).build(
            symbol=symbol,
            company_name=company_name,
            sector=sector,
            sub_sector=sub_sector,
            holder_name=holder_name,
            holder_type=holder_type,
        )


if __name__ == '__main__':
    pdf_url = "https://links.sgx.com/FileOpen/_Uni-Asia%202026-07-16%20-%20Changes%20in%20Interest%20of%20Substantial%20Shareholder.ashx?App=Announcement&FileID=896596"

    form3parser = Form3PartII(pdf_url=pdf_url)
    result = form3parser.parse_records()
    print(result)
