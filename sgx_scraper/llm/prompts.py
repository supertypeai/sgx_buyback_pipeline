from enum import Enum
from pydantic import Field, BaseModel


class TransactionType(str, Enum):
    BUY = "buy"
    SELL = "sell"
    OTHERS = "others"


class TransactionTag(str, Enum):
    TRANSFER = "transfer"
    AWARD = "award"
    WARRANT_EXERCISE = "warrant-exercise"
    DIRECTOR_FEES = "director-fees"
    MANAGEMENT_FEES = "management-fees"
    RIGHTS_OFFERING = "rights-offering"
    OPTION_EXERCISE = "option-exercise"
    MARRIED_DEAL = "married-deal"
    INTERNAL_RESTRUCTURING = "internal-restructuring"
    TAKEOVER = "takeover"
    DIVIDEND_IN_SPECIE = "dividend-in-specie"
    INHERITANCE = "inheritance"
    


class RawTransactionExtraction(BaseModel):
    amount_transaction: str | None = Field(
        default=None,
        description=(
            "The number of shares, units, or rights acquired or disposed of, "
            "copied exactly as printed using digits and separators only, for "
            "example '9,000,000'. Do not include units, a field label, or "
            "surrounding text. "
            "null if it is not present."
        ),
    )

    amount_transaction_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field label or heading associated with "
            "amount_transaction, copied without the value. "
            "null if it is not present."
        ),
    )

    consideration: str | None = Field(
        default=None,
        description=(
            "The amount of consideration paid or received, copied exactly as "
            "printed, including any currency prefix. null if it is not present."
        ),
    )

    consideration_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field label or heading associated with "
            "consideration, copied without the value. "
            "null if it is not present."
        ),
    )

    transaction_type: TransactionType | None = Field(
        default=None,
        description=(
            "The transaction classification supported by explicit filing "
            "wording. Choose only 'buy', 'sell', or 'others'. "
            "null when the available wording is insufficient."
        ),
    )

    tags: list[TransactionTag] = Field(
        default_factory=list,
        description=(
            "Zero or more explicitly supported transaction-mechanism tags. "
            "Allowed values are transfer, award, warrant-exercise, "
            "director-fees, management-fees, rights-offering, "
            "option-exercise, married-deal, internal-restructuring, takeover, "
            "dividend-in-specie, and inheritance. "
            "Return [] when none is supported."
        ),
    )

    classification_evidence: str | None = Field(
        default=None,
        description=(
            "The shortest exact wording from the circumstance description or "
            "filing section that supports transaction_type and tags. "
            "Do not provide speculative reasoning. null when no classification "
            "is returned."
        ),
    )

    classification_reasoning: str = Field(
        description="Explanation why are you assign the transaction type value and tags."
    )


class RegularManagementFeePriceRecovery(BaseModel):
    price_per_share: float | None = Field(
        default=None,
        description=(
            "The explicit price per share, unit, or security for the current "
            "management-fee transaction. Return null when the price is missing "
            "or cannot be linked to the current transaction without guessing."
        ),
    )

    price_evidence: str | None = Field(
        default=None,
        description=(
            "The shortest exact contiguous phrase from the supplied transaction "
            "context that contains the recovered price and its per-share, "
            "per-unit, or equivalent wording. Return null when price_per_share "
            "is null."
        ),
    )

    source_transaction_number: int | None = Field(
        default=None,
        description=(
            "The 1-based transaction number whose supplied context contains "
            "price_evidence. Return null when price_per_share is null."
        ),
    )

    reasoning: str | None = Field(
        default=None,
        description=(
            "A brief explanation of the explicit wording or cross-reference "
            "that links the price to the current transaction. Return null when "
            "price_per_share is null."
        ),
    )


class Form3PartIIIIVTransactionExtraction(BaseModel):
    amount_transaction: str | None = Field(
        default=None,
        description=(
            "The number of shares, units, rights, options, warrants, or "
            "principal amount of convertible debentures acquired or disposed "
            "of, copied exactly as printed in Part IV Item 2, for example "
            "'230,832,500 Units'. null if it is not present."
        ),
    )

    amount_transaction_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field label or heading associated with "
            "amount_transaction, copied without the value. "
            "null if it is not present."
        ),
    )

    consideration: str | None = Field(
        default=None,
        description=(
            "The consideration paid or received, copied exactly as printed "
            "in Part IV Item 3, including currency and wording such as "
            "'per Unit'. Do not calculate or normalize the value. "
            "null if it is not present."
        ),
    )

    consideration_source: str | None = Field(
        default=None,
        description=(
            "The exact printed field label or heading associated with "
            "consideration, copied without the value. "
            "null if it is not present."
        ),
    )

    transaction_type: TransactionType | None = Field(
        default=None,
        description=(
            "The transaction classification for the current shareholder, "
            "supported by the shared Part IV transaction and the complete "
            "Part III context. Choose only 'buy', 'sell', or 'others'. "
            "null when the available wording is insufficient."
        ),
    )

    tags: list[TransactionTag] = Field(
        default_factory=list,
        description=(
            "Zero or more explicitly supported transaction-mechanism tags. "
            "Allowed values are transfer, award, warrant-exercise, "
            "director-fees, management-fees, rights-offering, "
            "option-exercise, married-deal, internal-restructuring, takeover, "
            "dividend-in-specie, and inheritance. "
            "Return [] when none is supported."
        ),
    )

    classification_evidence: str | None = Field(
        default=None,
        description=(
            "The shortest exact wording from Part IV or the labelled Part III "
            "shareholder sections that supports transaction_type and tags. "
            "Do not provide interpretation or speculative reasoning here. "
        ),
    )

    classification_reasoning: str | None = Field(
        default=None,
        description=(
            "A concise explanation of why transaction_type and tags were "
            "assigned, including how the shared transaction applies to the "
            "current shareholder. Do not introduce facts absent from the filing."
        ),
    )


class AmbiguousTransactionClassification(BaseModel): 
    tags: list[TransactionTag] = Field(
        default_factory=list,
        description=(
            "Transaction tags explicitly supported by the deemed-interest "
            "description or remarks. Return an empty list when no approved "
            "tag can be identified."
        ),
    )

    tag_reasoning: str | None = Field(
        default=None,
        description=(
            "Reasoning why you assign or not assign the tag"
        ),
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


class BoardPageNumber(BaseModel):
    page_start: int | None = Field(
        default=None,
        description=(
            "The printed report page where the Board of Directors biographies, "
            "CVs, resumes, profiles, or equivalent director-information section "
            "starts. Return null when no matching section is found."
        ),
    )
    page_end: int | None = Field(
        default=None,
        description=(
            "The last printed report page belonging to the matching section. "
            "This must be one page before the next separate section begins. "
            "Return null when no matching section is found or when the end page "
            "cannot be determined from the table of contents."
        ),
    )
    explanation: str = Field(
        description=(
            "explain why you assign the value of page start and page end"
        )
    )


class BoardMemberDirectorItems(BaseModel):
    name: str = Field(
        description=(
            "Full name of a current member of the reporting company's Board of "
            "Directors, as stated in the provided annual report text."
        )
    )
    position: str | None = Field(
        description=(
            "Current board designation or position of the director at the reporting "
            "company, such as 'Chairman, Non-Executive and Independent Director' "
            "or 'Executive Director'. Return None when the board membership is clear "
            "but an explicit position cannot be determined from the provided text."
        )
    )
    age: int | None = Field(
        description=(
            "Director's age as explicitly stated in the annual report. "
            "Return only the stated age as an integer. Do not calculate age from "
            "a birth date, birth year, annual-report year, or any other information. "
            "Return None when age is not explicitly stated or cannot be reliably "
            "associated with this director."
        )
    ) 
    start_date: str | None = Field(
        description=(
            "Explicitly stated start or appointment date for this director's "
            "current board appointment or board position at the reporting company. "
            "Examples include 'Date of Appointment', 'Appointed as Director on', "
            "'Director since', or 'Joined the Board'. Return a full date as "
            "YYYY-MM-DD, a month-year date as YYYY-MM, and a year-only date as YYYY. "
            "Do not invent a missing day or month. Do not use company joining dates, management-role "
            "appointment dates, committee appointment dates, re-election dates when "
            "an original appointment date is available, external directorship dates, "
            "or inferred/calculated dates. Return None when no reliable board "
            "appointment/start date is explicitly stated."
        )
    )


class BoardMemberDirector(BaseModel):
    bod_payload: list[BoardMemberDirectorItems] = Field(
        description=(
            "Current members of the reporting company's Board of Directors extracted "
            "from the provided annual report section. Excludes management-only personnel, "
            "board committee memberships, external mandates, and former directors."
        )
    )


class PriceComponent(BaseModel):
    amount: int = Field(
        description="Number of securities to which this price applies."
    )
    price: float = Field(
        description="Explicit per-security price applicable to this component."
    )
    currency: str | None = Field(
        default=None,
        description=(
            "Three-letter ISO 4217 currency code for this explicit price, such as "
            "SGD, USD, or HKD. Return null when the filing does not explicitly "
            "identify the price currency or uses an ambiguous currency symbol."
        ),
    )


class SinglePrice(BaseModel):
    price: float = Field(
        description=(
            "Explicit per-security price applying to the entire target transaction."
        )
    )
    currency: str | None = Field(
        default=None,
        description=(
            "Three-letter ISO 4217 currency code for this explicit price, such as "
            "SGD, USD, or HKD. Return null when the filing does not explicitly "
            "identify the price currency or uses an ambiguous currency symbol."
        ),
    )
    

class MultiplePrices(BaseModel):
    components: list[PriceComponent] = Field(
        description=(
            "Explicit pricing components when different portions of the target "
            "transaction have different per-security prices."
        )
    )


class RecoverPricePerShare(BaseModel):
    price_per_share: SinglePrice | MultiplePrices | None = Field(
        description=(
            "Pricing information for the target transaction. Use SinglePrice when "
            "one price applies to the entire transaction, MultiplePrices when "
            "different portions have different explicit prices, and null when "
            "pricing cannot be reliably recovered."
        )
    )
    explanation: str = Field(
        description=(
            "Brief explanation of where the pricing information was found and "
            "why it applies to the target transaction."
        )
    )


class PromptCollections: 
    @staticmethod
    def get_system_news_prompt():
        return """ 
            You are a financial news writer expert covering the Singapore stock market (SGX).
            Your job is to write a concise, factual news entry for a Form insider filing transaction.
            You will be given only the current filing data. Write solely based on what is provided.
            Write in English. Be direct and specific. Do not use generic filler phrases.
        """
    
    @staticmethod
    def get_user_news_prompt():
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
            You are a financial data-extraction expert for SGX substantial-shareholder filings.

            You are processing ONE transaction only.

            You receive three transaction-scoped inputs:

            1. transaction_window
            - Contains the current transaction section before the circumstance section.
            - Use it only to extract raw transaction values and their printed labels.

            2. circumstances_desc
            - Contains the current transaction's selected circumstance and associated
                description.
            - This is the primary source for transaction classification.

            3. remarks
            - Contains remarks associated with the same current transaction.
            - Use it to clarify the circumstance and understand the complete transaction
                mechanism.

            All three inputs describe the SAME current transaction.

            RAW VALUE EXTRACTION

            For amount_transaction:
            - return digits and separators only, exactly as printed, without
                units, a field label, or surrounding text;

            For consideration:
            - copy exactly what is printed, including currency and per-unit wording;

            - never calculate, convert, sum, subtract, divide, normalize, or guess;
            - never calculate the transaction amount from holding changes;
            - return null when the requested value is not literally present.

            Copy the exact printed field label associated with every extracted numeric value.

            CLASSIFICATION

            The only classification fields are transaction_type and tags.

            Allowed transaction_type values:
            - buy
            - sell
            - others

            Allowed tags:
            - transfer
            - award
            - warrant-exercise
            - director-fees
            - management-fees
            - rights-offering
            - option-exercise
            - married-deal
            - internal-restructuring
            - takeover 
            - dividend-in-specie 
            - inheritance 

            Classification evidence priority:

            1. circumstances_desc
            2. remarks

            Use remarks to clarify or complete the transaction described by
            circumstances_desc.

            Do NOT use transaction_window to determine transaction_type or tags.

            Do not infer classification from:
            - holding increases or decreases;
            - direct or deemed interest changes;
            - numeric transaction amounts;
            - consideration values;
            - null or zero consideration;
            - holder identity;
            - ownership relationships;
            - assumptions about economic intent.

            A transaction may contain multiple procedural steps. Do not create a separate
            tag for every procedural step. Return only approved tags whose definitions are
            explicitly satisfied.

            Do not invent tags outside the allowed list.

            Return transaction_type = null when circumstances_desc and remarks do not
            support a classification.

            Return tags = [] when no approved tag is explicitly supported.
        """
    
    @staticmethod
    def get_user_fallback_parser_prompt():
        return """
            Extract the requested raw values and classify the CURRENT transaction.

            All supplied inputs belong to the same transaction.

            RAW VALUES

            Use Transaction window only for amount_transaction and consideration.

            Rules:

            - Return amount_transaction as the number only, exactly as printed.
              Include separators such as commas, but do not include units, a field
              label, or surrounding text. For example: "29,786".

            - Copy consideration exactly as printed, including currency symbols,
            currency codes, and wording such as "per Share" or "per Unit".

            - Do not calculate, normalize, sum, subtract, divide, or convert values.

            - Do not derive the transaction amount from holding-before and holding-after.

            - For every extracted value, copy the exact printed field label or heading
            associated with it.

            - If a requested value is not literally present, return null for both the
            value and its source.

            TRANSACTION CLASSIFICATION

            Use Circumstance description as the primary classification source.

            Use Remarks as supporting context for the SAME transaction. Remarks may explain
            additional steps or details of the mechanism.

            Do not use Transaction window, holding movements, or numeric values to infer
            transaction_type or tags.

            TRANSACTION TYPE
            transaction_type must be one of:

            - buy:
            Explicit purchase, acquisition, or subscription of securities.

            - sell:
            Explicit sale or disposal of securities. 
            Use sell only when the CURRENT holder explicitly sells, disposes of,
            tenders, or accepts an offer for the securities as an investment transaction.

            - others:
            An explicitly described mechanism that is neither an ordinary purchase nor
            ordinary sale, including transfer, award, warrant exercise, payment of fees
            using securities, internal restructuring, gift, inheritance, or another
            explicitly described mechanical transaction.

            Do not classify as buy or sell solely because holdings increased or decreased.

            TAG RULES

            - transfer:

            Use only when the transaction explicitly describes movement of EXISTING
            securities from one holder to another without an ordinary sale.

            Examples that support transfer:
            - "A transferred 100,000 shares to B"
            - "transfer of units from A to B"
            - gift of existing securities
            - inheritance of existing securities

            The securities must already exist as securities being moved from one holder
            to another.

            Do NOT use transfer merely because:
            - another entity ultimately receives newly issued securities;
            - a party nominates another entity to receive securities payable to it;
            - a party directs another entity to receive securities;
            - an entitlement to receive securities is redirected or assigned;
            - direct or deemed holdings change;
            - the recipient of newly issued securities differs from the party to whom
                they were originally payable.

            Example:

            "A is entitled to receive newly issued Units but nominates B to receive
            those Units"

            does NOT support the transfer tag because A did not first hold existing
            Units and then transfer those Units to B.

            - award:

            Explicit grant, award, vesting, restricted-share/unit plan, incentive plan,
            or other named securities-award mechanism.

            - warrant-exercise:

            Explicit exercise or conversion of warrants.

            Generic wording such as "conversion/exercise of rights, options, warrants
            or other convertibles" is insufficient unless warrants are specifically
            identified.

            - director-fees:

            Explicit payment or satisfaction of directors' fees using securities.

            - management-fees:

            Explicit payment or satisfaction of management, acquisition, or divestment
            fees using securities.

            - rights-offering:

            Explicit rights issue, rights offering, rights subscription, or exercise
            of rights.

            - option-exercise:

            Explicit exercise of employee share options or other options.

            - married-deal:

            Use only when the transaction explicitly identifies a married deal.

            A generic off-market transaction is not sufficient.

            - internal-restructuring:

            Use only when the transaction explicitly describes an internal
            restructuring, group reorganisation, trust restructuring, or equivalent
            restructuring mechanism.

            Do not infer internal-restructuring merely because entities are related,
            parent/subsidiary, or within the same corporate group.

            - takeover

            Use when the SAME transaction explicitly involves a takeover, take-over offer,
            general offer, mandatory offer, voluntary offer, privatisation, or a scheme of
            arrangement whose stated purpose is the acquisition of existing shareholders'
            or unitholders' securities.

            Do not use takeover merely because:
            - takeover regulations or the Code on Take-overs and Mergers are mentioned;
            - a scheme of arrangement is mentioned;
            - ownership or control changes.

            A scheme of arrangement supports takeover only when its stated purpose is the
            acquisition of existing shareholders' or unitholders' securities.

            The takeover tag does not by itself determine transaction_type.

            Determine transaction_type from the explicitly described action of the
            CURRENT holder:

            - explicit acquisition/purchase by the CURRENT holder
            -> buy

            - explicit sale/disposal, tender, or acceptance of a takeover/general offer
            by the CURRENT holder
            -> sell

            - automatic, compulsory, or scheme-driven transfer resulting from an effective
            or binding takeover, privatisation, or acquisition scheme, without an explicit
            sale/disposal/tender/acceptance action by the CURRENT holder
            -> others
            
            - dividend-in-specie 

            Use when securities are explicitly received, distributed, transferred, or
            allotted as a dividend in specie or distribution in specie.

            This is a non-cash distribution mechanism and should not be classified as
            buy merely because the recipient's holdings increase.

            Do not use dividend-in-specie for:
            - ordinary cash dividends;
            - ordinary purchases of securities using dividend proceeds;
            - distributions where securities are not explicitly identified as being
            distributed in specie.

            - inheritance

            Use when securities are explicitly acquired, received, transmitted, or
            transferred as a result of inheritance, succession, estate distribution,
            or transmission upon death.

            Inheritance is a more specific mechanism than transfer.

            When the same securities movement is explicitly described as inheritance,
            use inheritance rather than adding transfer for that same movement.

            Do not infer inheritance merely because:
            - securities move between family members;
            - the holder is an executor, administrator, trustee, or beneficiary;
            - an estate is mentioned without explicitly linking the securities movement
            to inheritance or succession.

            SPECIFIC MECHANISM PRECEDENCE

            When an actual securities movement is explicitly described but the movement
            occurs as part of a more specific approved mechanism, use the more specific
            tag rather than adding transfer for the same action.

            Examples:

            "transferred Units to directors as payment of directors' fees"
            -> transaction_type = "others"
            -> tags = ["director-fees"]

            NOT:
            ["director-fees", "transfer"]

            "transferred Units pursuant to an employee award plan"
            -> transaction_type = "others"
            -> tags = ["award"]

            NOT:
            ["award", "transfer"]

            Important distinction:

            Nomination or direction of another party to receive newly issued securities
            is NOT a transfer in the first place.

            Example:

            "Units payable to A as management fees. A nominated B to receive the Units."

            -> transaction_type = "others"
            -> tags = ["management-fees"]

            NOT:
            ["management-fees", "transfer"]

            CLASSIFICATION COMBINATIONS

            - transfer               -> others
            - award                  -> others
            - warrant-exercise       -> others
            - director-fees          -> others
            - management-fees        -> others
            - internal-restructuring -> others
            - dividend-in-specie     -> others
            - inheritance            -> others
            - rights-offering        -> buy
            - option-exercise        -> buy

            married-deal does not determine direction by itself:
            - explicit acquisition/purchase -> buy
            - explicit disposal/sale -> sell
            - no explicit direction -> others 

            takeover does not determine direction by itself:
            - explicit acquisition/purchase -> buy
            - explicit disposal/sale -> sell
            - no explicit direction -> others

            MULTIPLE TAGS

            Multiple tags are allowed only when the SAME transaction explicitly contains
            genuinely independent approved mechanisms.

            Do not assign multiple tags merely because several procedural steps are
            described.

            Do not assign a generic transfer tag to a movement already represented by a
            more specific approved mechanism.

            EVIDENCE

            classification_evidence must be the shortest exact contiguous wording from
            Circumstance description or Remarks that directly supports transaction_type
            and tags.

            - Do not paraphrase.
            - Do not summarize.
            - Do not insert "...".
            - Do not combine separate non-contiguous passages.
            - Do not use numeric holding changes as evidence.

            REASONING

            classification_reasoning must briefly explain:
            - what explicit transaction mechanism is described;
            - why it maps to transaction_type;
            - why each returned tag applies.

            It may mention other procedural steps from the same transaction when needed
            to explain why they do or do not satisfy an approved tag.

            Do not introduce facts absent from Circumstance description or Remarks.

            If classification is unsupported:
            - transaction_type: null
            - tags: []

            Transaction window:
            {window}

            Circumstance description:
            {circumstances_desc}

            Remarks:
            {remarks}

            Holder Name:
            {holder_name}

            Return the data using this JSON schema:
            {format_instructions}
        """

    @staticmethod
    def get_system_form3_part_iii_iv_prompt() -> str:
        return """
        You are financial expert, your objective is to extract one normalized transaction record for one current shareholder from
        an SGX Form 3 containing Parts III and IV.

        Form structure:

        - Part IV contains transaction details shared by all substantial
        shareholders/unitholders named in Part III.
        - Each Part III section contains information specific to one shareholder,
        including direct and deemed interests, deemed-interest circumstances,
        relationship information, and remarks.
        - The same remarks may be repeated across multiple shareholder sections.
        Treat identical repeated text as one piece of evidence.

        Your output must describe the transaction for the current shareholder, while
        using the complete filing context to understand the shared event.

        EXTRACTION RULES

        1. amount_transaction

        Copy Part IV Item 2 exactly as printed.
        Do not calculate it from holding-before and holding-after values.

        2. consideration

        Copy Part IV Item 3 exactly as printed.

        Preserve currencies, units, and wording such as:
        - S$0.88 per Unit
        - US$0.196 per Unit
        - Nil
        - Not applicable

        Do not calculate or normalize the value.

        3. transaction classification

        Allowed transaction types:
        - buy
        - sell
        - others

        Classify the shared underlying transaction described by the resolved Part IV
        circumstance.

        Resolve the Part IV circumstance as follows:

        1. If Part IV Item 4 contains an explicit selected circumstance, classify from
        that circumstance.

        2. If "Others (please specify)" is selected and its free text directly describes
        the transaction, classify from that text.

        3. If the Part IV free text only contains a cross-reference, such as
        "See Paragraph 12 of Shareholders A, B and C", follow that reference and use
        the transaction-related wording in the referenced remarks.

        4. Do not treat every sentence in the remarks as transaction evidence. Ignore
        text that only explains:
        - percentage calculations;
        - direct or deemed ownership;
        - shareholder relationships;
        - legal consequences;
        - arranger, underwriter, or adviser identities.

        5. Circumstances giving rise to deemed interests and relationship descriptions
        may explain why a shareholder reports the shared transaction, but they do
        not determine transaction_type unless they explicitly describe the
        transaction action.

        6. Classify the shared transaction once. Apply the resulting transaction_type
        and tags to every shareholder record covered by the same Part IV.

        Use "buy" for explicit:
        - acquisition or purchase;

        Use "sell" for explicit:
        - sale or disposal;

        Use "others" for explicit mechanical, compensation, or corporate-action
        transactions such as:

        - non-sale transfer or gift;
        - inheritance;
        - awards or vesting;
        - director-fee or management-fee payment using securities;
        - warrant exercise;
        - internal restructuring;
        - dividend in specie;
        - automatic, compulsory, or scheme-driven takeover/acquisition mechanics
          where no explicit investment sale decision by the holder is described;
        - non-participated corporate actions or mechanical ownership changes that are
          not purchases or sales.

        The presence of cash consideration does not by itself make a transaction
        buy or sell.

        Another party acquiring the shareholder's securities does not by itself make
        the shareholder's transaction a conviction sell when the movement is automatic,
        compulsory, or mechanically effected under a corporate-action scheme.

        4. tags

        Allowed tags:
        - transfer
        - award
        - warrant-exercise
        - director-fees
        - management-fees
        - rights-offering
        - option-exercise
        - married-deal
        - internal-restructuring
        - takeover
        - dividend-in-specie
        - inheritance

        Tags describe the explicit mechanism of the shared underlying transaction.

        Use the most specific approved mechanism.

          TAG RULES


        - transfer

        Use for an explicit non-sale movement of EXISTING securities from one holder
        to another when no more specific approved tag represents the same movement.

        Examples:
        - gift of existing securities;
        - explicit transfer between holders without a sale or another specific
          mechanism.

        Do not use transfer merely because securities are ultimately issued,
        allotted, or delivered to another entity.

        Do not use transfer when the same movement is more specifically described as:
        - award;
        - director-fees;
        - management-fees;
        - inheritance;
        - dividend-in-specie;
        - takeover;
        - internal-restructuring.


        - award

        Use for an explicit grant, award, vesting, restricted-share/unit plan,
        performance-share/unit plan, incentive plan, or equivalent securities-award
        mechanism.


        - warrant-exercise

        Use only when warrants are explicitly exercised or converted.

        Generic conversion/exercise wording is insufficient unless warrants are
        specifically identified.


        - director-fees

        Use when securities are explicitly used as payment or satisfaction of
        directors' fees.


        - management-fees

        Use when securities are explicitly used as payment or satisfaction of:
        - management fees;
        - acquisition fees;
        - divestment fees.

        The tag may coexist with buy or sell when the same securities in the current
        transaction are explicitly linked to such fees.


        - rights-offering

        Use for an explicit:
        - rights issue;
        - rights offering;
        - rights subscription;
        - exercise of rights.


        - option-exercise

        Use only when options are explicitly exercised.


        - married-deal

        Use only when the transaction explicitly identifies a married deal.

        A generic off-market transaction is not sufficient.


        - internal-restructuring

        Use only when the underlying transaction explicitly describes:
        - internal restructuring;
        - group restructuring;
        - corporate reorganisation;
        - trust restructuring;
        - equivalent internal ownership restructuring.

        Do not infer internal-restructuring merely because:
        - entities are related;
        - one entity is a parent or subsidiary;
        - entities belong to the same corporate group;
        - securities move between related entities;
        - the transaction is off-market;
        - the transaction uses a scheme or scheme of arrangement.

        A scheme of arrangement is a legal mechanism and does not by itself mean
        internal restructuring.

        Classify the scheme according to its explicitly stated purpose.


        - takeover

        Use when the shared transaction explicitly involves an acquisition mechanism
        under which an acquirer seeks to acquire securities held by existing
        shareholders or unitholders.

        This may include:
        - takeover or take-over offer;
        - general offer;
        - mandatory offer;
        - voluntary offer;
        - privatisation;
        - scheme of arrangement whose stated purpose is acquisition of existing
          shareholders' or unitholders' securities;
        - trust scheme of arrangement whose stated purpose is such an acquisition;
        - another explicitly equivalent acquisition mechanism.

        Do not use takeover merely because:
        - the Code on Take-overs and Mergers is mentioned;
        - takeover regulations appear in boilerplate;
        - control or ownership changes;
        - a scheme is mentioned without an acquisition of existing holders'
          securities.

        The takeover tag does not by itself determine transaction_type.

        Determine transaction_type from the economic nature of the underlying
        transaction:

        - explicit purchase/acquisition by the relevant holder
          -> buy + takeover

        - explicit sale/disposal or explicit acceptance of takeover offer
          -> sell + takeover

        - automatic, compulsory, or scheme-driven movement of securities caused by
          the takeover/acquisition mechanism, without an explicit investment sale
          decision
          -> others + takeover


        - dividend-in-specie

        Use when securities are explicitly:
        - distributed;
        - received;
        - allotted;
        - transferred

        as a dividend in specie or distribution in specie.

        This is a non-cash corporate-action mechanism.

        -> transaction_type = others

        Do not classify as buy merely because the recipient receives additional
        securities.

        Do not use dividend-in-specie for:
        - ordinary cash dividends;
        - purchases made using dividend proceeds;
        - distributions where securities are not explicitly distributed in specie.


        - inheritance

        Use when securities are explicitly received, transmitted, or transferred
        because of:
        - inheritance;
        - succession;
        - estate distribution;
        - transmission upon death.

        -> transaction_type = others

        Inheritance is more specific than generic transfer.

        When the same securities movement is explicitly described as inheritance:

        tags = [inheritance]

        NOT:

        tags = [transfer, inheritance]

        Do not infer inheritance merely because:
        - securities move between family members;
        - an executor, administrator, trustee, beneficiary, or estate is mentioned;
        - inheritance or succession is not explicitly connected to the securities
          movement.

         SPECIFIC MECHANISM PRECEDENCE

        When wording describes a transfer or movement of securities but the same
        movement occurs through a more specific approved mechanism, use the more
        specific tag rather than generic transfer.

        Examples:

        - securities transferred as payment of directors' fees:

          transaction_type = others
          tags = [director-fees]

        - securities transferred pursuant to an employee award:

          transaction_type = others
          tags = [award]

        - securities transmitted through inheritance:

          transaction_type = others
          tags = [inheritance]

        - securities distributed by way of dividend in specie:

          transaction_type = others
          tags = [dividend-in-specie]

        - securities automatically transferred to an acquirer pursuant to an
          effective takeover/acquisition scheme:

          transaction_type = others
          tags = [takeover]

        Do not add transfer for the same movement in these cases.


        OTHER EXAMPLES

        - generic gift or non-sale transfer:

          transaction_type = others
          tags = [transfer]

        - exercise of warrants:

          transaction_type = others
          tags = [warrant-exercise]

        - exercise of options:

          transaction_type = buy
          tags = [option-exercise]

        - subscription pursuant to a rights offering:

          transaction_type = buy
          tags = [rights-offering]

        - explicit married deal involving an acquisition:

          transaction_type = buy
          tags = [married-deal]

        - explicit married deal involving a disposal:

          transaction_type = sell
          tags = [married-deal]

        - explicit acceptance of takeover offer by the holder:

          transaction_type = sell
          tags = [takeover]

        - compulsory or scheme-driven transfer to an acquirer after the acquisition
          scheme becomes effective:

          transaction_type = others
          tags = [takeover]

        Do not assume every off-market transaction is a married deal.

        5. multiple shareholders and deemed interests

        Use relationship and deemed-interest descriptions to understand why the
        shared underlying transaction applies to the current shareholder.

        For example, when Entity A directly purchases securities and Entities B and C
        have deemed interests through ownership of Entity A, the same underlying
        investment transaction may be classified as "buy" for A, B, and C.

        This reflects the shared underlying transaction reported by the Form 3,
        rather than treating the deemed-interest relationship itself as a separate
        transaction.

        However:

        - ownership-chain wording does not itself mean transfer;

        - a parent-subsidiary relationship does not itself mean
          internal-restructuring;

        - deemed-interest wording does not itself establish a transaction mechanism;

        - wording such as "deemed interested by virtue of ownership" only explains
          why the current shareholder reports the underlying transaction;

        - takeover wording appearing only in regulatory or ownership explanations
          does not establish the takeover tag.


        6. multiple tags

        Multiple tags are allowed only when genuinely independent approved mechanisms
        are explicitly part of the SAME underlying transaction.

        Example:

        "restricted share awards issued as payment of directors' fees"

        transaction_type = others
        tags = [award, director-fees]

        Do not assign multiple tags merely because several procedural steps appear in
        the filing.

        Do not add generic transfer when a more specific tag already represents the
        same securities movement.


        7. evidence and reasoning

        classification_evidence must contain the shortest exact filing wording that
        supports the classification.

        The evidence must contain enough context to identify the actual transaction
        mechanism.

        Do not use an isolated word such as:
        - acquisition;
        - transfer;
        - scheme;
        - restructuring

        when surrounding wording is required to understand what transaction actually
        occurred.

        classification_reasoning must briefly explain:
        - the underlying shared transaction;
        - whether it represents an investment purchase/sale or a mechanical/corporate
          action;
        - how it applies to the current shareholder;
        - why the chosen tags apply or why tags is empty.

        When a scheme, offer, transfer, or restructuring mechanism is mentioned,
        classify it from its explicitly stated economic purpose rather than the legal
        label alone.

        Return:
        - transaction_type = null;
        - tags = [];
        - classification_evidence = null;
        - classification_reasoning = null;

        when the filing does not provide enough explicit evidence.
    """

    @staticmethod
    def get_user_form3_part_iii_iv_prompt() -> str:
        return """
            Extract the normalized transaction for the current shareholder.

            Current shareholder:
            {current_shareholder}

            Shared Part IV transaction details:
            {part_iv_context}

            Labelled Part III sections for all notifying shareholders:
            {all_shareholder_context}

            Important:

            - Produce one output for the current shareholder only.

            - Use the complete shareholder context to understand the shared event.

            - Part IV amount and consideration may be repeated in the output for each
            shareholder because Part IV applies to all shareholders in this filing.

            - Classify the shared underlying transaction, not the deemed-interest
            relationship itself.

            - Do not merge ownership relationships with transaction mechanisms.

            - Do not treat a deemed-interest relationship as a transfer.

            - Do not infer internal-restructuring merely because parties are related.

            - Do not infer takeover merely from takeover-related regulatory boilerplate.

            - Ignore duplicated remarks when the same wording appears in multiple
            shareholder sections.

            - Copy amount_transaction, consideration, and their source headings exactly
            as printed.

            - Do not calculate values from the holding tables.

            Return the result using this JSON schema:
            {format_instructions}
        """

    @staticmethod
    def get_system_ambiguous_tag_prompt() -> str:
        return """
            You are a financially precise classifier for ambiguous SGX transaction mechanisms.

            The transaction direction and baseline transaction type have already been
            determined from the selected checkbox. Your only task is to recover applicable
            tags from the supplied deemed-interest description and remarks.

            Allowed tags:
            - transfer
            - award
            - warrant-exercise
            - director-fees
            - management-fees
            - rights-offering
            - option-exercise
            - married-deal
            - internal-restructuring
            - takeover
            - dividend-in-specie
            - inheritance

            Return only tags explicitly supported by the supplied wording.

            Tags must describe a mechanism that is explicitly part of the CURRENT
            transaction.

            Remarks may provide additional transaction mechanisms even when the selected
            circumstance describes only the execution method or transaction direction.

            Do not treat the selected circumstance as excluding a tag explicitly supported
            by Remarks when Remarks clearly connect that mechanism to the same securities
            and the same current transaction.

            Prefer a specific approved mechanism over the generic "transfer" tag when both
            describe the same securities movement.

            Do not infer tags from:
            - holding increases or decreases;
            - consideration values;
            - the holder's identity;
            - a spouse, parent, subsidiary, affiliate, or controlled-company relationship
            by itself;
            - parties being related corporations or members of the same corporate group
            by itself;
            - the name of a security without wording describing the transaction action.

            Return an empty list when no tag is clearly supported.
        """

    @staticmethod
    def get_user_ambiguous_tag_prompt() -> str:
        return """
            Recover transaction tags for the ambiguous selected circumstance below.

            Selected circumstance:
            {selected_circumstance}

            SAME-TRANSACTION RULE

            The selected circumstance may describe only how the transaction was executed,
            while Remarks may explicitly describe an additional approved mechanism.

            A tag is supported when the Remarks clearly link that mechanism to the SAME
            securities and SAME current transaction.

            Do not reject an otherwise explicit tag merely because the selected
            circumstance is "Securities via off-market transaction".

            Example:

            Remarks:
            "A is entitled to receive 3,000,000 Units as payment of management fees.
            A has sold the 3,000,000 Units to B and directed that the Units be issued
            directly to B."

            -> tags = ["management-fees"]

            The management-fee mechanism applies because the same 3,000,000 Units being
            sold in the current transaction are explicitly identified as securities
            payable as management fees.

            This does NOT support "transfer" merely because the Units are issued directly
            to B.

            This does NOT support "internal-restructuring" merely because B is related
            to A.

            Rules for "Securities via off-market transaction":

            - Add "married-deal" only when the text explicitly says "married deal" or
            explicitly identifies the transaction as a married deal.

            - Add "transfer" only when the text explicitly describes a non-sale movement
            of EXISTING securities from one holder to another and no more specific
            approved mechanism describes the same movement.

            - Do not add "transfer" when the text explicitly describes a sale, purchase,
            subscription, or another more specific approved mechanism for the same
            securities.

            - Add "management-fees" when the SAME current transaction explicitly involves
            securities payable or receivable as payment or satisfaction of management,
            acquisition, or divestment fees.

            This tag may apply to an off-market sale when the Remarks explicitly state
            that the same securities being sold are the securities the holder is
            entitled to receive as such fees.

            - Add "director-fees" when the SAME current transaction explicitly involves
            securities as payment or satisfaction of directors' fees.

            - Add "award" when the SAME current transaction explicitly involves a grant,
            award, vesting, restricted-share/unit plan, performance-share/unit plan,
            incentive plan, or another named securities-award mechanism.

            - Add "internal-restructuring" only when the wording explicitly describes an
            internal restructuring, group reorganisation, corporate reorganisation,
            trust restructuring, or equivalent restructuring mechanism.

            - Do NOT infer "internal-restructuring" merely because:
            - the buyer and seller are related corporations;
            - the entities are parent and subsidiary;
            - the entities belong to the same corporate group;
            - securities are sold or moved between related entities.

            Example:

            "A has sold the securities to B, a related corporation of A."

            does NOT support "internal-restructuring" by itself.

            - Add "takeover" when the SAME current transaction is explicitly described as
            being pursuant to or part of a takeover, take-over offer, general offer,
            mandatory offer, voluntary offer, or equivalent takeover mechanism.

            - Add "dividend-in-specie" when the SAME current transaction explicitly
            describes securities being distributed or received as a dividend in specie
            or distribution in specie.

            - Add "inheritance" when the SAME current transaction explicitly describes
            securities being received, transmitted, or transferred through inheritance,
            succession, estate distribution, or transmission upon death.

            - When inheritance describes the securities movement, use "inheritance"
            rather than also adding the generic "transfer" tag for the same movement.

            - When dividend-in-specie describes the securities movement, use
            "dividend-in-specie" rather than also adding the generic "transfer" tag for
            the same movement.

            - Do not assume every off-market transaction is a married deal.

            - A placement or vendor placement does not match any approved tag unless
            another approved mechanism is explicitly stated.

            Rules for "Securities following conversion/exercise":

            - Add "warrant-exercise" only when warrants are explicitly exercised or
            converted.

            - Add "option-exercise" only when options are explicitly exercised.

            - Add "rights-offering" only when rights, a rights issue, rights offering, or
            subscription through rights is explicitly described.

            - A generic conversion or exercise statement is not enough to choose a tag.

            - A convertible bond or convertible security does not match an approved tag
            unless the text explicitly identifies rights, options, or warrants.


            Deemed-interest descriptions may merely explain why a person has deemed
            ownership. For example, being deemed interested in a convertible bond held by
            a spouse does not prove that the person exercised or converted it and does not
            support a tag.


            Circumstances giving rise to deemed interest:
            {circumstances_deemed_desc}

            Remarks:
            {remarks}

            Return the result using this JSON schema:
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

    # Board Page Detection Prompt
    @staticmethod
    def get_system_board_page_prompt(is_fallback: bool = False):
        if is_fallback:
            section_selection_rules = """
                FALLBACK SECTION SELECTION

                Prefer a separate director biography, profile, CV, resume, or
                equivalent person-level section over a general "Board of Directors"
                section. Examples include:
                - "Board Profiles"
                - "Directors' Biographies"
                - "Information on Directors"

                Do not select a general "Board of Directors" section during this
                fallback pass.

                Do not select a disclosure about directors seeking re-election.
                It may cover only a subset of the current Board.
            """
        else:
            section_selection_rules = """
                PRIMARY SECTION SELECTION

                Select the general "Board of Directors" section when it exists.
                Do not replace it with a separate director biography, profile, CV,
                resume, or "Information on Directors" section.

                Only when no general "Board of Directors" section exists, select
                the best available director section.
            """

        return (
            """
            You identify the page range containing person-level information about
            the Board of Directors from an annual report table of contents.

            The goal is to locate the section most likely to contain detailed
            information about individual directors, such as their name, position,
            age, appointment/start date, professional background, qualifications,
            career history, or biography.

            """
            + section_selection_rules
            + """

            Do NOT choose sections that only discuss:
            - board composition;
            - corporate governance;
            - board meetings or attendance;
            - board responsibilities or practices;
            - committee structure;
            - a simple list of director names without person-level profile content.

            PAGE NUMBER RULES

            Use the printed report page numbers shown in the table of contents,
            not PDF page indexes.

            page_start is the printed page number associated with the selected
            section.

            page_end is exclusive of the next separate section. Find the next
            heading at the same or higher table-of-contents level and subtract one
            from its printed page number.

            Example:
            - selected section starts on page 7
            - next separate section starts on page 11

            Return:
            page_start = 7
            page_end = 10

            Do not include page 11.

            If there is no matching section, return null for both fields.

            During the fallback pass, return null for both fields when a complete
            page range cannot be determined.

            Return only the requested structured output.
            Never invent page numbers.
            """
        )

    @staticmethod
    def get_user_board_page_prompt(is_fallback: bool = False):
        if is_fallback:
            section_instruction = """
                This is the fallback pass. Prefer a separate director biography,
                profile, CV, resume, or "Information on Directors" section.

                Do not use a general "Board of Directors" section or a disclosure
                about directors seeking re-election. If a complete page range cannot
                be determined, return null for both fields.
            """
        else:
            section_instruction = """
                This is the primary pass. Select the general "Board of Directors"
                section when it exists.
            """

        return (
            """
            Find the page range containing person-level information about the
            company's Board of Directors.

            """
            + section_instruction
            + """

            Table of contents:
            {table_of_contents}

            Note:
                A contents entry may place its printed page number immediately
                before or above the section heading, or immediately after or below
                it.

                Associate an adjacent number with the heading only when both belong
                to the same visual column.

                Do not use numbers from a neighbouring column.

            Return the data in the following JSON schema:
            {format_instructions}
            """
        )

    @staticmethod 
    def get_system_board_page_vision_prompt():
        return """
            You extract a printed page range from an annual report's table of contents image.

            Find the section containing person-level information about the Board of Directors,
            such as director profiles, biographies, qualifications, or appointment details.

            Use only printed report page numbers visibly associated with the section headings.
            Do not use PDF page indexes, dates, years, table values, or numbers from another
            visual column.

            page_start is the first printed page of the selected section.
            page_end is one page before the next separate section begins.

            Do not guess. Return null when the section or its page number cannot be determined.
            Return only the requested structured output.
        """

    @staticmethod 
    def get_user_board_page_vision_prompt():
        return """ 
            Inspect the attached table of contents image.

            Identify the Board of Directors or director-profile section and return its printed
            page range. Use the next separate section to determine page_end.

            If the page numbers or section relationship are unclear, return null instead of
            guessing. Briefly explain the page selection.
            
            Return the data in the following JSON schema:
            {format_instructions}
        """

    @staticmethod
    def get_system_management():
        return """
            You extract CURRENT members of the reporting company's Board of Directors
            from annual-report text taken from a Board of Directors, director profile,
            biography, CV, resume, or equivalent section.

            Return only information explicitly supported by the provided text.

            For each current director, extract:
            - name
            - position
            - age
            - start_date

            RULES

            1. Extract only current members of the reporting company's Board of Directors.

            2. Preserve the director's current board designation as stated in the report.

            If the profile heading contains both a board designation and a current
            executive role, preserve both when they clearly belong to the same person.

            Example:
            "Executive Director and Chief Executive Officer"
            should remain:
            "Executive Director and Chief Executive Officer"

            3. If a person is clearly presented as a current director but no more
            specific board designation is given, set `position` to "Director".

            4. Do not use board committee roles such as Audit Committee Chairman,
            Nominating Committee Member, or Remuneration Committee Member as the
            person's main board position.

            5. Do not use directorships or positions held at OTHER companies as the
            person's position at the reporting company.

            6. AGE

            Extract `age` only when the person's age is explicitly stated.

            Examples:
            - "Age: 58"
            - "Aged 63"

            Do not calculate age from:
            - date of birth
            - birth year
            - annual-report year
            - employment dates
            - any other information

            Return null when age is not explicitly stated.

            7. START DATE

            `start_date` is the explicitly stated date when the person joined or
            was appointed to the Board of the reporting company.

            Valid wording may include:
            - "Date of Appointment"
            - "Date of appointment as Director"
            - "Appointed as Director"
            - "Director since"
            - "Joined the Board"
            - "First appointed to the Board"
            - equivalent wording that clearly refers to the person's board appointment

            Do not use:
            - date the person joined the company as an employee
            - CEO, CFO, COO, or other management appointment dates unless the text
                explicitly states that the same date is also the board appointment date
            - board committee appointment dates
            - re-election dates when an original appointment date is available
            - external directorship dates
            - employment-history dates
            - calculated or inferred dates

            Normalize dates using the available precision:
            - full day, month, and year: YYYY-MM-DD
            - month and year only: YYYY-MM
            - year only: YYYY

            Do not invent a missing day or month.

            Examples:
            "Director since 2018"
            -> start_date = "2018"

            "Appointed as Director in May 2019"
            -> start_date = "2019-05"

            "Date of Appointment: 15 March 2021"
            -> start_date = "2021-03-15"

            Return null when no reliable board appointment or start date is stated.

            8. NAME FORMATTING

            Preserve normal mixed-case spelling from the report.

            If PDF extraction makes the entire name uppercase or lowercase,
            convert it to natural display capitalization.

            Preserve:
            - initials
            - accents
            - apostrophes
            - hyphens
            - name order
            - non-Latin characters

            9. Exclude:
            - former directors
            - retired directors
            - directors-designate who have not yet taken office
            - management-only personnel
            - external board appointments

            10. Deduplicate the same director when they appear multiple times.

            11. PDF text may contain broken lines, flattened columns, or mixed profile
                text. Associate information with a director only when the relationship
                is clear. Do not guess.

            12. If no current directors can be reliably identified, return an empty
                `bod_payload`.

            Your output must follow the supplied structured response schema.
        """

    @staticmethod
    def get_user_management():
        return """
            Reporting company:
            {company_name}

            Extract the current members of the Board of Directors from the annual-report
            text below.

            For each director, extract their:
            - name, remove Mr/Ms if present 
            - current position
            - age, when explicitly available
            - board start date, when explicitly available, 
            in this format: 'day month year' for example '5 August 2005'

            Annual report text:
            {pdf_text}

            Return the output in the following JSON:
            {format_instructions}
        """

    @staticmethod
    def get_system_recover_price_per_share() -> str:
        return """
            You are a precise financial data recovery assistant for SGX filings.

            Your only task is to recover missing price_per_share information for ONE
            target transaction.

            You will receive multiple transaction contexts from the SAME filing because
            the target transaction may refer to another transaction or shared remarks.

            The other transactions are supporting context only.

            PRICE RECOVERY

            Recover pricing only when the filing explicitly provides a per-security
            price that can be reliably linked to the target transaction.

            A valid price may appear in:
            - raw_consideration;
            - remarks;
            - text referenced by raw_consideration;
            - another supplied transaction context when the target transaction
            explicitly cross-references it.

            Valid examples include:
            - "at an issue price of S$0.8968 per Unit"
            - "at an issue price of S$0.88 per stapled security"
            - "S$1.25 per Share"
            - "issued at US$0.196 per Unit"

            Extract the numeric per-security price and its explicit currency. Return
            both the price and currency fields when a price is recovered.

            CURRENCY

            Return the three-letter ISO 4217 currency code for every returned price:
            - S$ or SGD -> SGD
            - US$ or USD -> USD
            - HK$ or HKD -> HKD
            - another explicitly printed currency code -> that code

            Return currency = null when the price currency is absent or an unqualified
            "$" is ambiguous. Do not assume SGD from an unqualified "$".

            OUTPUT FORMAT

            price_per_share can have one of two structures.

            1. SINGLE PRICE

            When ONE explicit price applies to the entire target amount_transaction:

            price_per_share = {{
                "price": <numeric price>,
                "currency": <ISO 4217 code or null>
            }}

            Do not repeat amount_transaction inside price_per_share when one price
            applies to the entire transaction.


            2. MULTIPLE PRICES

            When different explicitly identified portions of the SAME target transaction
            have different prices:

            price_per_share = {{
                "components": [
                    {{
                        "amount": <component amount>,
                        "price": <component price>,
                        "currency": <ISO 4217 code or null>
                    }}
                ]
            }}

            Use multiple components only when:
            - multiple different prices are explicitly stated;
            - each price is explicitly linked to a corresponding amount of securities;
            - all components belong to the SAME target transaction.

            The sum of component amounts must equal the target amount_transaction.

            Do not:
            - choose only one component price;
            - calculate a weighted-average price;
            - combine different prices into a synthetic single price;
            - invent component amounts.

            TRANSACTION MATCHING

            Always determine whether the pricing information actually applies to the
            TARGET transaction.

            Use:
            - transaction_number;
            - explicit cross-references;
            - remarks;
            - references to the same securities or entitlement.

            Pricing information appearing in another transaction may be used only when
            the filing explicitly links that information or the same securities to the
            target transaction.

            because the filing explicitly links the same securities and price to the
            target transaction.

            MANAGEMENT-FEE TRANSACTIONS

            Management-fee transactions may involve securities that a manager is
            entitled to receive as payment of management, acquisition, or divestment
            fees.

            An explicitly stated issue price may be used when the filing clearly links
            that price to the SAME securities involved in the target transaction.

            This remains valid when those same securities are subsequently sold,
            nominated, directed, or issued directly to another entity as part of the
            SAME reported transaction.

            Do not reject applicable pricing merely because the target transaction also
            contains an off-market sale, nomination, or direct issuance step.

            DO NOT USE

            Do not use:
            - management-fee percentages;
            - ownership percentages;
            - shareholding percentages;
            - number of securities as a price;
            - total fee amounts as a price;
            - NAV or NTA;
            - unrelated market prices;
            - closing prices;
            - unrelated average trading prices;
            - another transaction's price without explicit linkage;
            - prices inferred from holding movements;
            - calculated or estimated prices.

            Do not divide transaction values or fee amounts by the number of securities
            to derive a missing price.

            The per-security price itself must be explicitly stated in the filing.

            FAILURE CASE

            Return:

            price_per_share = null

            when:
            - no explicit applicable per-security price can be identified;
            - pricing cannot be reliably linked to the target transaction;
            - multiple prices are stated but their corresponding component amounts
            cannot be reliably identified;
            - only part of the target transaction can be explicitly priced.

            Do not guess or estimate.

            Explanation

            Keep concise.

            Explain:
            - where the pricing information was found;
            - how it is linked to the target transaction;
            - if multiple components are returned, why each amount and price belongs
            to the target transaction.

            Do not include unrelated filing details.
        """

    @staticmethod 
    def get_user_recover_price_per_share() -> str:
        return """
            Recover price_per_share for ONE target transaction.

            Target transaction:
            {target_transaction}

            All transaction contexts from the same filing:
            {transaction_context}

            Important:
            - Return a result for the target transaction only.
            - Other transactions are supporting context only.
            - Follow explicit cross-references between transactions.
            - If one price applies to the entire transaction, 
            - If different portions of the transaction have different explicit prices,
            return component amounts and prices separately.
            - Component amounts must together equal the target amount_transaction.
            - Use another transaction's pricing only when the filing explicitly links it
            to the target transaction.
            - Do not calculate weighted-average prices.
            - Do not estimate or infer a missing price.

            Return the output in the following JSON format:
            {format_instructions}
        """

    @staticmethod
    def get_system_recover_price_per_share_form3_part_iii_iv() -> str:
        return """
            You are a precise financial data recovery assistant for SGX Form 3 filings
            containing Parts III and IV.

            Your only task is to recover a missing price_per_share for ONE target
            normalized record.

            FORM STRUCTURE

            Part IV describes ONE shared underlying transaction that applies to multiple
            substantial shareholders/unitholders identified in Part III.

            Each Part III shareholder section may contain shareholder-specific:
            - remarks;
            - awareness explanations;
            - circumstances giving rise to deemed interests;
            - shareholder relationship information;
            - direct interest before and after the transaction.

            Information relevant to the shared transaction may appear in the Part III
            section of one shareholder and be repeated or cross-referenced by other
            shareholders.

            You will therefore receive the contexts for ALL shareholders covered by the
            same shared Part IV transaction.

            The other shareholder contexts are supporting evidence only.

            PRICE RECOVERY

            Recover price_per_share only when the filing explicitly states a
            per-security price that can be reliably linked to the SAME shared Part IV
            transaction represented by the target record.

            A valid price may appear in:
            - the target shareholder's remarks;
            - another shareholder's remarks when the wording describes the same shared
            Part IV transaction;
            - awareness_explanation;
            - deemed_interest_circumstances;
            - shareholder_relationship;

            but only when that text explicitly states a per-security price and clearly
            links it to the shared transaction.

            Valid examples include:
            - "at S$0.88 per Unit"
            - "at an issue price of S$0.8968 per Unit"
            - "S$1.25 per Share"
            - "at US$0.196 per stapled security"

            Extract the numeric per-security price and its explicit currency.

            Return the three-letter ISO 4217 currency code for every returned price:
            - S$ or SGD -> SGD
            - US$ or USD -> USD
            - HK$ or HKD -> HKD
            - another explicitly printed currency code -> that code

            Return currency = null when the price currency is absent or an unqualified
            "$" is ambiguous. Do not assume SGD from an unqualified "$".

            Examples:

            "at an issue price of S$0.8968 per Unit"
            -> price_per_share = 0.8968

            "US$0.196 per Unit"
            -> price_per_share = 0.196

            SHARED-TRANSACTION MATCHING

            The shareholder contexts may describe different ownership relationships,
            direct interests, and deemed interests, but they refer to the same underlying
            Part IV transaction.

            A price found in another shareholder's context may be used for the target
            record only when the wording clearly describes the SAME shared transaction.

            Repeated identical remarks across multiple shareholders should be treated as
            one piece of evidence.

            Do not require the applicable price to appear specifically in the target
            shareholder's own section when another shareholder section explicitly states
            the price for the same shared transaction.

            HOLDER-SPECIFIC CONTEXT

            Use holder_name and shareholder-specific context to understand why the shared
            transaction applies to each shareholder.

            However, ownership relationships do NOT establish a price.

            In particular:

            - direct_before and direct_after are holdings, not price evidence;
            - changes in direct interest must not be used to derive price;
            - deemed-interest relationships must not be used to calculate price;
            - shareholder relationships must not be used to infer price;
            - awareness explanations must not be treated as price evidence unless they
            explicitly state an applicable per-security price.

            Example:

            Holder A directly acquires Units.

            Holder B is deemed interested in those Units because Holder A is its
            subsidiary.

            If Holder A's remarks explicitly state that the shared acquisition occurred
            at S$0.88 per Unit, and the filing establishes that Holder B reports the same
            shared Part IV transaction:

            -> price_per_share = 0.88

            for the target normalized record representing that shared transaction.

            MULTIPLE PRICES

            The shared Part IV transaction may contain multiple separately priced
            components.

            Recover price_per_share only when ONE explicit per-security price applies to
            the entire target transaction amount.

            If different portions of the shared transaction have different prices and
            there is no single explicit price applicable to the complete target
            transaction:

            - price_per_share = null

            Do not:
            - choose one component price;
            - calculate a weighted-average price;
            - create a synthetic price.

            If the normalized target record represents only one explicitly separated
            component, then the explicit price for that component may be recovered.

            DO NOT USE

            Do not use:
            - direct_before;
            - direct_after;
            - changes between before and after holdings;
            - ownership percentages;
            - deemed-interest percentages;
            - number of securities as a price;
            - transaction value divided by number of securities;
            - management-fee percentages;
            - total consideration without an explicit per-security price;
            - NAV or NTA;
            - unrelated market prices;
            - closing prices;
            - average trading prices;
            - prices from a different transaction;
            - calculated, estimated, or inferred prices.

            The price itself must be explicitly stated in the supplied filing context.

            FAILURE CASE

            If no explicit applicable per-security price can be reliably identified:

            - price_per_share = null

            Do not guess, calculate, or estimate.

            REASONING

            Keep reasoning concise.

            Explain:
            - where the explicit price was found;
            - which shareholder context contained it;
            - why that wording applies to the SAME shared Part IV transaction represented
            by the target record.

            If price_per_share is null, briefly explain why the available contexts do
            not provide one explicit applicable per-security price.

            Do not include unrelated ownership-chain details.
        """


    @staticmethod
    def get_user_recover_price_per_share_form3_part_iii_iv() -> str:
        return """
            Recover price_per_share for ONE target normalized record from an SGX
            Form 3 Part III/IV shared transaction.

            Target record:
            {target_transaction}

            Contexts for all shareholders covered by the same shared Part IV transaction:
            {shareholder_context}

            Important:
            - Return a result for the target record only.
            - All shareholder contexts relate to the same shared Part IV transaction.
            - Other shareholders are supporting context only.
            - A price found in another shareholder's context may be used only when the
            wording explicitly applies to the same shared transaction.
            - Repeated identical remarks are one piece of evidence.
            - Do not derive price from direct_before, direct_after, deemed interests,
            ownership relationships, or holding changes.
            - Recover only an explicitly stated per-security price.
            - When a price is recovered, return its three-letter ISO 4217 currency
            code. Return currency = null for an absent or ambiguous currency.
            - Do not calculate, estimate, average, or infer a missing price.

            Return the output in the following JSON format:
            {format_instructions}
        """

    @staticmethod
    def get_system_recover_price_general_announcement() -> str:
        return """
            You are a precise financial data recovery assistant for SGX announcements.

            Your only task is to recover price_per_share from the supplied general
            announcement text.

            Recover a price only when the announcement explicitly states a
            per-share, per-unit, per-stapled-security, or equivalent per-security price
            for the transaction described.

            Valid examples:

            - "at an issue price of S$0.8968 per Unit"
            -> price_per_share = 0.8968

            - "at an average issue price of approximately S$4.031 per Unit"
            -> price_per_share = 4.031

            - "issued at US$0.196 per Unit"
            -> price_per_share = 0.196

            Extract the numeric per-security price and its explicit currency.

            Return the three-letter ISO 4217 currency code for every returned price:
            - S$ or SGD -> SGD
            - US$ or USD -> USD
            - HK$ or HKD -> HKD
            - another explicitly printed currency code -> that code

            Return currency = null when the price currency is absent or an unqualified
            "$" is ambiguous. Do not assume SGD from an unqualified "$".

            MULTIPLE PRICES

            If the announcement describes multiple portions of the transaction at
            different prices, do not choose one price arbitrarily.

            Example:

            - 299,091 Units at S$0.8156 per Unit
            - 1,378,000 Units at S$0.8146 per Unit

            If no single explicit price applies to the entire transaction:

            -> price_per_share = null

            However, if the announcement explicitly states one average issue price for
            the entire transaction, that explicit average price may be returned.

            DO NOT:
            - calculate a weighted-average price;
            - divide transaction value by number of securities;
            - infer price from holdings;
            - use NAV, NTA, ownership percentages, or fee percentages as price;
            - use unrelated market or closing prices;
            - estimate a missing price.

            The price itself must be explicitly stated in the announcement.

            If no single applicable explicit per-security price is available:

            -> price_per_share = null

            Keep reasoning concise and explain the announcement wording that supports
            the result.
        """

    @staticmethod
    def get_user_recover_price_general_announcement() -> str:
        return """
            Recover price_per_share from the following SGX general announcement.

            Announcement text:
            {announcement_text}

            Return only an explicitly stated applicable per-security price.

            Return null when:
            - no explicit per-security price is stated; or
            - multiple different prices apply and no single explicit price represents
            the entire transaction.

            Return the output using this JSON schema:
            {format_instructions}
        """
