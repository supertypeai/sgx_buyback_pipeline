from pydantic import Field, BaseModel


class TitleBodyGeneration(BaseModel): 
    title: str = Field(
        description='News title for the filing transaction'
    )
    body: str = Field(
        description='One or two paragraph news body summarizing the filing with context'
    )


class PomptCollections: 
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
    