from pydantic import Field, BaseModel
from typing import Literal


TransactionType = Literal['buy', 'sell', 'award', 'transfer', 'others']


class RawTransactionExtraction(BaseModel):
    amount_transaction: str | None = Field(
        default=None,
        description=(
            "The number of shares/units/rights acquired or disposed of, copied "
            "exactly as printed (e.g. '9,000,000'). null if not present in the text."
        ),
    )
    amount_transaction_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field LABEL/heading the number of shares sits under, "
            "copied verbatim and WITHOUT the value itself "
            "(e.g. '... acquired or disposed of by Substantial Shareholder/Unitholder:'). "
            "null if not found."
        ),
    )
    consideration: str | None = Field(
        default=None,
        description=(
            "The amount of consideration paid or received, copied exactly as "
            "printed including currency prefix (e.g. 'S$2,700,000'). null if not present. "
            "Note: this is the consideration amount, NOT a price per share, do not divide."
        ),
    )
    consideration_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field LABEL/heading the consideration sits under, copied "
            "verbatim and WITHOUT the value itself "
            "(e.g. 'Amount of consideration paid or received ... :'). null if not found."
        ),
    )
    transaction_type: TransactionType | None = Field(
        default=None,
        description=(
            "The nature of the transaction, classified ONLY from the circumstance "
            "description provided to you. Choose exactly one of: 'buy' (acquisition/"
            "purchase), 'sell' (disposal), 'award' (share award/grant/vesting), "
            "'transfer' (off-market transfer, gift, inheritance, trust/spousal "
            "arrangement), 'others' (a corporate action or anything not fitting the "
            "above). null if no circumstance description is provided."
        ),
    )
    transaction_type_reasoning: str = Field(
        description="Explain the reasoning why you assign the transaction type?"
    )


SYSTEM_FALLBACK_PARSER_PROMPT = """
            You are a precise data-extraction tool for SGX substantial-shareholder filings.
            For numeric values you copy exactly what is printed and nothing else: you never
            calculate, convert, sum, divide, or guess a number, and you return null when a
            number is not literally present.
            The one exception is transaction_type: you classify it from the 'Others ( please
            specify )' circumstance free text into one fixed set of labels. You still never
            invent the underlying text, your classification must be grounded in wording that
            actually appears in the section, and you return null when there is no such text.
        """

USER_FALLBACK_PARSER_PROMPT = """
            Extract the requested raw values for the shareholder/unitholder named below
            from the single filing section provided.

            Rules for the numeric values (amount_transaction, consideration):
            - Copy each value exactly as printed, including thousand separators and any
                currency prefix (e.g. 'S$2,700,000', '9,000,000').
            
            - Do NOT compute a price per share, do NOT convert currencies, do NOT sum or
                divide anything. Copy only.
            
            - For every value you return, also return its source: the exact printed field
                LABEL/heading the value sits under, copied verbatim and WITHOUT the value
                appended. Do NOT stitch the label and the value into one sentence.
            
            - If a value is not present in the text below, return null for both the value
                and its source. Returning null is correct and expected.

            Rule for transaction_type:
            - Classify using ONLY the circumstance description below. Do NOT read the filing
                section for this, ignore checkboxes.
            
            - If a circumstance description is provided, classify it into exactly one label:
                -buy: securities acquired through a purchase for consideration.
                
                -sell: securities disposed of through a sale for consideration.
                
                -award: securities granted, vested, or transferred as compensation under
                an explicit share/unit award, incentive, restricted share/unit, or named
                remuneration plan.

                - transfer:  a non-sale movement of securities between identifiable parties,
                including gifts, inheritance, and internal ownership transfers.
                Both the transferor and transferee must be identifiable.
                                
                - others: anything that does not satisfy the definitions above, including
                bonus issues, dividends in specie, rights-related corporate actions,
                director-fee payments without an explicit award-plan context, securities
                lending or returns with an unidentified counterparty, and reclassification
                between direct and deemed interest.
            
            - If the circumstance description is empty, return null for transaction_type.

            Circumstance description (for transaction_type only):
            {circumstances_desc}

            Holder: {holder_name}

            Filing section (for the numeric values only):
            {window}

            Return the data in the following JSON schema:
            {format_instructions}
        """
