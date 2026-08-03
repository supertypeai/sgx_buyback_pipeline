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


class TransferParties(BaseModel):
    transferor: str | None = Field(
        default=None,
        description=(
            "The party whose holding is transferred AWAY (the 'from' side), taken "
            "from the circumstance description, e.g. \"CICTML's unitholding\" or a "
            "person's name. null if it cannot be identified from the description."
        ),
    )
    transferee: str | None = Field(
        default=None,
        description=(
            "The party RECEIVING the holding (the 'to' side), taken from the "
            "circumstance description, e.g. 'its key management personnel and "
            "eligible employees'. null if it cannot be identified from the description."
        ),
    )
    reasoning: str = Field(
        description="Briefly explain how you identified the transferor and transferee."
    )


class TitleBodyGeneration(BaseModel):
    title: str = Field(
        description='News title for the filing transaction'
    )
    body: str = Field(
        description='One or two paragraph news body summarizing the filing with context'
    )


class PromptCollections: 
    @staticmethod
    def get_system_prompt():
        return """ 
            You are a financial news writer expert covering the Singapore stock market (SGX).
            Your job is to write a concise, factual news entry for a Form insider filing transaction.
            You will be given only the current filing data. Write solely based on what is provided.
            Write in English. Be direct and specific. Do not use generic filler phrases.
        """
    
    @staticmethod
    def get_user_prompt():
        return """ 
            Write a professional financial news entry for the following SGX insider filing transaction.

            Current filing:
            {current_filing}

            Title format. Use data from the current filing only:
            - If transaction type is buy or sell:
                (Holder name) (Transaction Type) Shares of (Company name)
            - If transaction type is award:
                (Holder name) Reports Share Award Distribution in (Company name)
            - If transaction type is others:
                (Company name) Insider (Holder name) Reports Shareholding Change

            Body instructions:
            - Maximum two to four sentences.
            - Written from the perspective of a financial journalist covering SGX insider transactions.
            - Lead with the most significant aspect of the transaction: size, ownership impact, or price.
            - price_per_share and transaction_value may be null. When they are null, omit all monetary
            figures entirely. Quantify using share count and ownership percentage before and after only.
            Do not estimate, infer, or approximate a value.
            - Quantify where possible given available fields: share count, transaction value if not null,
            ownership percentage before and after, price per share if not null.
            Do not enumerate individual transaction blocks.
            - Do not restate the same fact twice in different phrasing.
            - Currency: SGD. Comma as thousands separator. Dot for decimals.
            - Ownership percentage fields are stored as decimals on a 0-1 scale. Multiply by 100 to
            get the display percentage, then round to two decimal places
            (e.g. 0.0699 displays as 6.99%, not 0.07%).
            - If both the before and after display percentages are identical after rounding,
            omit the percentage figures entirely and rely on share counts only.
            - If transaction type is award, one sentence describing the share count change and
            ownership impact is sufficient. Do not add interpretive statements about the
            nature of the award beyond what the data explicitly states.
            - If transaction type is others, identify and describe the specific corporate action
            (e.g. rights issue, private placement, transfer) rather than labeling it as others.
            - tags provides context labels for the nature of the transaction. Use these only to
            inform the framing and word choice of the body — do not invent details not present
            in the other fields.
            - circumstances contains the filer's own free-text description of why the transaction
            occurred. If present and not '-', use it to add specific context to the body. Quote or paraphrase it faithfully — do not
            contradict or expand beyond what it states.
            - Do not speculate. Do not editorialize. Do not use filler phrases like
            "it is worth noting" or "this is significant because".
            - Do not use informal shorthands like 'the buy' or 'the sell'. 
            Use 'the purchase', 'the acquisition', or 'the disposal' instead
            
            Ensure return in the following JSON format.
            {format_instructions}
        """

    @staticmethod
    def get_system_fallback_parser_prompt():
        return """
            You are a precise data-extraction tool for SGX substantial-shareholder filings.
            For numeric values you copy exactly what is printed and nothing else: you never
            calculate, convert, sum, divide, or guess a number, and you return null when a
            number is not literally present.
            The one exception is transaction_type: you classify it from the 'Others ( please
            specify )' circumstance free text into one fixed set of labels. You still never
            invent the underlying text, your classification must be grounded in wording that
            actually appears in the section, and you return null when there is no such text.
        """
    
    @staticmethod
    def get_user_fallback_parser_prompt():
        return """
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

    @staticmethod
    def get_system_transfer_prompt():
        return """
            You identify the two parties of an off-market transfer reported in an SGX filing.
            Use the circumstance description and the filing party provided: identify who the
            securities moved FROM (the transferor) and who they moved TO (the transferee).
            When the description plainly says securities were gifted or transferred TO a
            recipient but does not name the giver, use the filing party as the transferor.
            Use the wording as it appears in the description as closely as possible and keep
            each side concise. Never invent a party. If either side is not identifiable from
            the description, return null for that side.
        """

    @staticmethod
    def get_user_transfer_prompt():
        return """
            A transfer transaction was reported by the filing party below. From the
            circumstance description, identify the two sides of the transfer.

            - transferor: the party whose holding is transferred away (the 'from' side).
            - transferee: the party receiving the holding (the 'to' side).
            - Name only the party itself. Stop at the recipient and drop trailing
              qualifiers (plan/scheme names, purpose, "pursuant to ..." or "under the ..."
              clauses).
            - If the description says the filing party gave, gifted, or transferred securities
              TO a recipient but omits the giver's name, use the filing party as transferor.
              For example, with filing party "Jane Tan" and description "gift to daughter",
              return transferor "Jane Tan" and transferee "daughter".
            - Do not use the filing party as transferor when the description says securities
              were received FROM someone else or does not establish the direction of transfer.
            - If the description does not clearly identify both sides, return null for the
              side you cannot determine. Do not guess.

            Filing party (holder on the form): {holder_name}

            Circumstance description:
            {circumstances_desc}

            Return the data in the following JSON schema:
            {format_instructions}
        """

    @staticmethod
    def get_system_form_3_transfer_prompt():
        return """
            You determine whether one Form 3 filing proves an off-market transfer.
            Use all shareholder records provided together. Return the transferor and
            transferee only when the filing clearly identifies both parties. Use the
            holder names from the records and never invent a party.
        """

    @staticmethod
    def get_user_form_3_transfer_prompt():
        return """
            The following shareholder records came from one Form 3 filing. Their
            circumstances may identify transfer parties in another shareholder's
            Item 6, Item 8, or Item 9 text.

            Determine whether the filing proves one transfer between the holders
            whose direct holdings changed. If it does, return its transferor and
            transferee. Otherwise return null for both parties.

            Shareholder records:
            {records}

            Return the data in the following JSON schema:
            {format_instructions}
        """
