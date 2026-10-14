# Output shapes the LLM is asked to return for a filing.
from enum import Enum
from typing import Literal

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
    SHARE_BUYBACK = "share-buyback"
    SCRIP_DIVIDEND = "scrip-dividend"
    BONUS_ISSUE = "bonus-issue"


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
            "dividend-in-specie, inheritance, share-buyback, scrip-dividend, and "
            "bonus-issue. "
            "Return [] when none is supported."
        ),
    )

    holder_side: Literal["receives", "gives"] | None = Field(
        default=None,
        description=(
            "Which side of the transaction the current holder is on, taken only "
            "from the filing's wording. 'receives' when the wording states the "
            "holder received, was issued, was allotted, or was paid the "
            "securities. 'gives' when the wording states the holder transferred, "
            "paid out, delivered, or passed on the securities to another party, "
            "or nominated another party to receive them. null when the wording "
            "does not state it. Never decide this from the holdings table."
        ),
    )

    change_reason: str | None = Field(
        default=None,
        description=(
            "A short phrase stating why the interest changed, taken only from "
            "what the filing states in the circumstance description, the "
            "circumstances giving rise to deemed interests, or the remarks. "
            "Write it so it completes the sentence 'The filing attributes the "
            "change to ...', for example 'the receipt of scrip shares in lieu "
            "of a cash dividend'. At most 20 words, no trailing full stop, no "
            "holder or issuer names unless needed to identify the acting party. "
            "null when the filing does not state a reason."
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
            "dividend-in-specie, inheritance, share-buyback, scrip-dividend, and "
            "bonus-issue. "
            "Return [] when none is supported."
        ),
    )

    holder_side: Literal["receives", "gives"] | None = Field(
        default=None,
        description=(
            "Which side of the transaction the current holder is on, taken only "
            "from the filing's wording. 'receives' when the wording states the "
            "holder received, was issued, was allotted, or was paid the "
            "securities. 'gives' when the wording states the holder transferred, "
            "paid out, delivered, or passed on the securities to another party, "
            "or nominated another party to receive them. null when the wording "
            "does not state it. Never decide this from the holdings table."
        ),
    )

    change_reason: str | None = Field(
        default=None,
        description=(
            "A short phrase stating why the interest changed, taken only from "
            "what the filing states in the circumstance description, the "
            "circumstances giving rise to deemed interests, or the remarks. "
            "Write it so it completes the sentence 'The filing attributes the "
            "change to ...', for example 'the receipt of scrip shares in lieu "
            "of a cash dividend'. At most 20 words, no trailing full stop, no "
            "holder or issuer names unless needed to identify the acting party. "
            "null when the filing does not state a reason."
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


class HolderNameCasing(BaseModel):
    name: str = Field(
        description=(
            "The same name with standard capitalisation. Only upper and lower "
            "case may change. Every letter, digit, space, and punctuation mark "
            "must stay exactly where it is."
        ),
    )


class ActingParty(BaseModel):
    acting_party: str | None = Field(
        default=None,
        description=(
            "The person or entity the filing states carried out the transaction "
            "that changed the current holder's deemed interest, with the name "
            "copied exactly as the filing writes it. For a relative given only "
            "by relationship, write \"the holder's spouse\", \"the holder's "
            "son\" and so on. null when the filing does not state who carried "
            "out the transaction, or when the current holder did."
        ),
    )

    evidence: str | None = Field(
        default=None,
        description=(
            "The shortest exact wording from the supplied filing text that "
            "states this party carried out the transaction. null when "
            "acting_party is null."
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
