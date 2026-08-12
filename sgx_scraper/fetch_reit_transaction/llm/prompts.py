from sgx_scraper.fetch_reit_transaction.constant import TRANSACTION_BASIS


class ReitTransactionPrompt:

    @staticmethod
    def get_system_prompt() -> str:
        return f"""You are reading an SGX regulatory announcement filed by a Singapore
REIT under "Asset Acquisitions and Disposals".

Extract every PROPERTY the announcement transacts. One object per property.

Fields, per property:
- property_name: the property's own name as the document writes it.
- transaction_type: "acquisition" or "divestment".
- status: "announced" if this proposes or agrees a deal, "completed" if it
  reports the deal has completed, "terminated" if it reports a deal called off.
- counterparty: the buyer or seller, named exactly as disclosed. If unnamed,
  use the document's own descriptor (e.g. "unrelated third party").
- transaction_price: the consideration, in FULL UNITS of currency. Documents
  abbreviate, so "S$200.4 million" is 200400000, "A$52.5m" is 52500000 and
  "US$6.93M" is 6930000. Expand every abbreviation. Never return the
  abbreviated number. Null if not disclosed.
- currency: the ISO code of that consideration (SGD, USD, JPY, ...).
- basis_value: the figure the price is measured against, if disclosed, in the
  same FULL UNITS as transaction_price.
- basis: what basis_value is, one of {", ".join(TRANSACTION_BASIS)}. Null if no
  basis_value.
- interest_pct: the stake transacted as a fraction, 1 for a whole interest.
- completed_date: the completion date as YYYY-MM-DD, only if the document says
  the deal HAS completed. Null for a proposed deal.

Rules:
- Take only what the document discloses. Never derive, infer or estimate a
  number that is not written down.
- A single consideration covering several properties is repeated on each
  property, unchanged. Do not split it.
- Use null, not a guess, for anything absent.

Reply with ONLY JSON: {{"properties": [{{...}}, ...]}}"""

    @staticmethod
    def get_user_prompt(document_text: str) -> str:
        return document_text
