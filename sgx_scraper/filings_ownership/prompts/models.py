from pydantic import BaseModel, Field
from typing import Literal


class OwnershipLink(BaseModel):
    parent: str = Field(description="The owning, controlling or deemed-interested party, or LISTED ISSUER.")
    child: str = Field(description="The owned, controlled or holding party, or LISTED ISSUER.")
    relation: Literal[
        "owns",
        "controls",
        "deemed_via",
        "spouse",
        "family",
        "beneficiary",
        "trustee",
        "nominee",
        "fund_manager",
        "agreement",
    ]
    percentage: float | None = Field(
        default=None,
        description="The percentage exactly as written, e.g. 16.1767 for 16.1767%. null when none is stated.",
    )
    counts_toward_parent: bool = Field(
        default=False,
        description=(
            "Spouse or family only: true when the text says the parent is deemed "
            "interested in securities the relative holds or has an interest in."
        ),
    )
    evidence: str = Field(description="The shortest exact wording from the text that states this link.")


class RoutePercentage(BaseModel):
    via: str = Field(description="The party the route of the deemed interest goes through.")
    percentage: float = Field(description="The percentage stated for this route, as written.")


class ShortName(BaseModel):
    short: str = Field(description="The short name as the text defines it, e.g. Fullerton.")
    full: str = Field(description="The full name it stands for.")


class TraderRead(BaseModel):
    record: int = Field(description="The record number, as listed.")
    traders: list[str] = Field(
        default_factory=list,
        description=(
            "Every party that bought, sold, received or gave up the securities in "
            "this change, or the fund manager that dealt for its portfolios. Empty "
            "when the filing does not say."
        ),
    )
    basis: Literal["text", "figures"] | None = Field(
        default=None,
        description="'text' when the filing's wording names the trader, 'figures' when only the share figures show it.",
    )
    evidence: str | None = Field(
        default=None,
        description="For 'text', the shortest exact wording that says the traders dealt. For 'figures', the figures relied on.",
    )


class EntityType(BaseModel):
    name: str = Field(description="The name exactly as written elsewhere in this answer or as listed.")
    entity_type: Literal["person", "company", "fund", "trust", "nominee", "state", "not_a_party"]


class FilingRead(BaseModel):
    links: list[OwnershipLink] = Field(default_factory=list)
    direct_holders: list[str] = Field(
        default_factory=list,
        description="Parties the text says hold the listed issuer's securities directly.",
    )
    route_percentages: list[RoutePercentage] = Field(default_factory=list)
    short_names: list[ShortName] = Field(default_factory=list)
    acting_party: str | None = Field(
        default=None,
        description="The acting party's full name, or null when there is none or it is not a named party.",
    )
    entity_types: list[EntityType] = Field(
        default_factory=list,
        description="Every party named in this answer and every holder listed, once each.",
    )


class FilingTraders(BaseModel):
    records: list[TraderRead] = Field(
        default_factory=list,
        description="One entry per record marked answer.",
    )


class ReportHolder(BaseModel):
    name: str = Field(
        description=(
            "Holder name exactly as the substantial shareholders table writes it, "
            "without footnote marks such as (1) or (2),(3)."
        )
    )
    direct_shares: float | None = Field(
        default=None,
        description="Shares or units held in the holder's own name. Null when the table gives none.",
    )
    direct_pct: float | None = Field(
        default=None,
        description="Direct interest as a percentage out of 100. Null when the table gives none.",
    )
    deemed_shares: float | None = Field(
        default=None,
        description="Shares or units the holder is deemed interested in. Null when the table gives none.",
    )
    deemed_pct: float | None = Field(
        default=None,
        description="Deemed interest as a percentage out of 100. Null when the table gives none.",
    )
    total_pct: float | None = Field(
        default=None,
        description=(
            "Total interest as a percentage out of 100. When the table gives only share or unit "
            "counts, divide the holder's total by the issued shares or units stated on the same "
            "pages and multiply by 100. Null when neither is given."
        ),
    )
    deemed_through: list[str] = Field(
        default_factory=list,
        description=(
            "Names of the other holders in this table whose shares this holder is deemed "
            "interested in, as the footnotes state. Empty when the holder has no deemed interest "
            "or the footnotes do not say."
        ),
    )
    held_through_others: list[str] = Field(
        default_factory=list,
        description=(
            "Companies, trusts or people NOT in this table that the footnotes say this holder "
            "holds through, in the order of the chain. Empty when every route is through holders "
            "in the table or the footnotes do not say."
        ),
    )


class AnnualReportHolders(BaseModel):
    as_at: str | None = Field(
        default=None,
        description="The date the shareholding statistics are stated as at, as YYYY-MM-DD. Null when not stated.",
    )
    page: int | None = Field(
        default=None,
        description="The PDF page number, from the '--- PDF page N ---' marker, that has the substantial shareholders table.",
    )
    holders: list[ReportHolder] = Field(
        default_factory=list,
        description="Every row of the substantial shareholders table. Empty when the pages have no such table.",
    )
    notes: str = Field(
        description="Anything unclear: a flattened table, missing footnotes, a percentage worked out from share counts.",
    )


class CompanyOwner(BaseModel):
    company: str = Field(
        description="The company from the list, exactly as given.",
    )
    owned_by: list[str] = Field(
        default_factory=list,
        description=(
            "Companies, trusts or people NOT in the table that the footnotes say own or control "
            "this company or are deemed interested in the shares it holds, from the closest to the "
            "furthest up the chain. Names exactly as the footnotes write them. Empty when the "
            "footnotes name none."
        ),
    )
    quote: str = Field(
        default="",
        description="The footnote sentence that names them, copied word for word. Empty when owned_by is empty.",
    )


class CompanyOwners(BaseModel):
    companies: list[CompanyOwner] = Field(
        default_factory=list,
        description="One entry for each company in the list.",
    )


class HolderPart(BaseModel):
    kind: Literal["spouse", "child", "nominee", "joint_account", "assigned_votes", "family_company", "other"] = Field(
        description=(
            "spouse or child: shares held by the holder's spouse or child. nominee: shares held for "
            "the holder in a nominee or custodian account. joint_account: shares held jointly with "
            "someone. assigned_votes: some of the holder's OWN shares whose voting rights the holder "
            "assigned to someone else. family_company: a company NOT in the table that holds shares "
            "the holder is deemed interested in. other: any other stated part."
        )
    )
    held_by: str = Field(
        description="Who holds the part, or for assigned_votes who received the votes, exactly as the footnote writes it.",
    )
    shares: float | None = Field(
        default=None,
        description="The number of shares or units, exactly as printed. Null when the footnote does not give it.",
    )
    quote: str = Field(
        description="The footnote sentence that states the part, copied word for word.",
    )


class HolderParts(BaseModel):
    holder: str = Field(description="The holder from the list, exactly as given.")
    parts: list[HolderPart] = Field(default_factory=list)


class MemberParts(BaseModel):
    holders: list[HolderParts] = Field(
        default_factory=list,
        description="One entry for each listed holder whose footnotes state any part; leave out holders with none.",
    )
