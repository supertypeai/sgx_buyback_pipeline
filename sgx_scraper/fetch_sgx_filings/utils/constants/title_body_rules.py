from sgx_scraper.fetch_sgx_filings.utils.constants.form_options import (
    CORPORATE_ACTION_CHECKBOX_KEY,
)


# Title and body rules.
#
# A rule with a verb is an action by the holder:
#   title  -> the rule's own title
#   body   -> "{holder} {verb} {amount} of {company}{detail}"
# When only the deemed interest moved, the holder did not act personally, so
# the title becomes "{holder}'s deemed interest in {company} {direction}{reason}".
#
# A rule without a verb has no stated direction. Its title reports the stake
# percentages as filed, followed by {reason}; its body uses {detail}.
#
# Placeholders: {holder}, {company}, {units} ("shares" or "units") and {unit}.
#
# A title also states how much was traded when the filing gives it: the total
# value for a purchase or sale ("buys S$894K of C shares"), otherwise the
# number of securities ("receives 58,700 C shares"). See build_action_title
# and build_deemed_reason in utils/payload_helper.py.
def _title_body_rule(
    title: str | None,
    verb: str | None,
    detail: str,
    reason: str,
    direction: str | None = None,
    received: bool = False,
    title_with_quantity: str | None = None,
) -> dict:
    return {
        "title": title,
        # Used in place of "title" when the amount is known, for a title that
        # has no "{units} of {company}" or "{company} {units}" to put it in
        "title_with_quantity": title_with_quantity,
        "verb": verb,
        "detail": detail,
        "reason": reason,
        "direction": direction,
        # Received securities are quoted as "valued at", not as a price paid
        "received": received,
    }


# Tags are the most specific signal, checked in this order. Each tag maps a
# transaction_type to a rule; "default" covers the remaining types.
TITLE_BODY_TAG_PRIORITY = [
    "share-buyback",
    "takeover",
    "director-fees",
    "management-fees",
    "award",
    "option-exercise",
    "warrant-exercise",
    "rights-offering",
    "dividend-in-specie",
    "scrip-dividend",
    "bonus-issue",
    "inheritance",
    "internal-restructuring",
    "married-deal",
    "transfer",
]

# Tags whose default rule describes the holder receiving securities. They are
# skipped for a "sell" unless the tag defines its own "sell" rule.
TITLE_BODY_RECEIPT_TAGS = {
    "director-fees",
    "management-fees",
    "award",
    "option-exercise",
    "warrant-exercise",
    "rights-offering",
    "dividend-in-specie",
    "scrip-dividend",
    "inheritance",
}

TITLE_BODY_TAG_RULES = {
    "share-buyback": {
        "default": _title_body_rule(
            None,
            None,
            " after the company bought back {units}",
            " after {unit} buyback",
        ),
    },
    "takeover": {
        "buy": _title_body_rule(
            "{holder} acquires {units} of {company} under takeover offer",
            "acquired",
            " under a takeover offer",
            " after acquisition under takeover offer",
            direction="rises",
        ),
        "sell": _title_body_rule(
            "{holder} accepts takeover offer for {units} of {company}",
            "disposed of",
            " by accepting a takeover offer",
            " after acceptance of takeover offer",
            direction="falls",
        ),
        "default": _title_body_rule(
            None,
            None,
            " under a takeover offer",
            " under takeover offer",
        ),
    },
    "director-fees": {
        "default": _title_body_rule(
            "{holder} receives {company} {units} as part of director's fees",
            "received",
            " as part of director's fees",
            " after payment of director's fees in {units}",
            direction="rises",
            received=True,
        ),
    },
    "management-fees": {
        "sell": _title_body_rule(
            "{holder} sells {units} of {company} received as management fees",
            "sold",
            " received as management fees",
            " after sale of management fee {units}",
            direction="falls",
        ),
        "default": _title_body_rule(
            "{holder} receives {company} {units} as management fees",
            "received",
            " as payment of management fees",
            " after payment of management fees in {units}",
            direction="rises",
            received=True,
        ),
    },
    "award": {
        "default": _title_body_rule(
            "{holder} receives {unit} award from {company}",
            "received",
            " under a {unit} award",
            " after {unit} award",
            direction="rises",
            received=True,
            title_with_quantity=(
                "{holder} receives {quantity} {company} {units} as award"
            ),
        ),
    },
    "option-exercise": {
        "default": _title_body_rule(
            "{holder} exercises {unit} options in {company}",
            "acquired",
            " through the exercise of {unit} options",
            " after exercise of {unit} options",
            direction="rises",
        ),
    },
    "warrant-exercise": {
        "default": _title_body_rule(
            "{holder} exercises warrants in {company}",
            "acquired",
            " through the exercise of warrants",
            " after exercise of warrants",
            direction="rises",
        ),
    },
    "rights-offering": {
        "default": _title_body_rule(
            "{holder} subscribes for {units} of {company} in rights issue",
            "subscribed for",
            " under a rights issue",
            " after rights issue subscription",
            direction="rises",
        ),
    },
    "dividend-in-specie": {
        "default": _title_body_rule(
            "{holder} receives {company} {units} as dividend in specie",
            "received",
            " as a dividend in specie",
            " after dividend in specie",
            direction="rises",
            received=True,
        ),
    },
    "scrip-dividend": {
        "default": _title_body_rule(
            "{holder} receives {company} {units} under scrip dividend scheme",
            "received",
            " under a scrip dividend scheme",
            " after scrip dividend",
            direction="rises",
            received=True,
        ),
    },
    # Every holder gets bonus securities in proportion, so the stake does not
    # move and the rule claims no direction
    "bonus-issue": {
        "default": _title_body_rule(
            None,
            None,
            " following a bonus issue by the company",
            " after bonus issue",
        ),
    },
    "inheritance": {
        "default": _title_body_rule(
            "{holder} inherits {units} of {company}",
            "inherited",
            "",
            " after inheritance",
            direction="rises",
            received=True,
        ),
    },
    "internal-restructuring": {
        "default": _title_body_rule(
            None,
            None,
            " following an internal restructuring",
            " after internal restructuring",
        ),
    },
    "married-deal": {
        "buy": _title_body_rule(
            "{holder} buys {units} of {company} in married deal",
            "bought",
            " in a married deal",
            " after married deal purchase",
            direction="rises",
        ),
        "sell": _title_body_rule(
            "{holder} sells {units} of {company} in married deal",
            "sold",
            " in a married deal",
            " after married deal sale",
            direction="falls",
        ),
        "default": _title_body_rule(
            None,
            None,
            " through a married deal",
            " after married deal",
        ),
    },
    "transfer": {
        "default": _title_body_rule(
            None,
            None,
            " following a transfer of {units}",
            " after {unit} transfer",
        ),
    },
}

# A tag names a mechanism, not which side the holder is on. When one of these
# tags comes from free text, the LLM's holder_side picks the wording:
#   "receives" -> the tag's rule in TITLE_BODY_TAG_RULES
#   "gives"    -> the "gives" rule below
#   not stated -> the "unstated" rule below, which claims no direction
# A tag set by a ticked box keeps the TITLE_BODY_TAG_RULES wording, because
# the box itself fixes the side (acquisition, or a director's own award).
# A tag with no "gives" rule uses its "unstated" rule for that side too: a
# dividend paid out by a company in the holder's chain is not the holder's.
TITLE_BODY_SIDED_TAG_RULES = {
    "dividend-in-specie": {
        "unstated": _title_body_rule(
            None,
            None,
            " following a dividend in specie",
            " after dividend in specie",
        ),
    },
    "scrip-dividend": {
        "unstated": _title_body_rule(
            None,
            None,
            " under a scrip dividend scheme",
            " after scrip dividend",
        ),
    },
    "award": {
        "gives": _title_body_rule(
            "{holder} transfers {company} {units} under award plan",
            "transferred",
            " under an award plan",
            " after transfer of {units} under award plan",
            direction="falls",
            received=True,
        ),
        "unstated": _title_body_rule(
            None,
            None,
            " under an award plan",
            " under award plan",
        ),
    },
    "director-fees": {
        "gives": _title_body_rule(
            "{holder} transfers {company} {units} as part of director's fees",
            "transferred",
            " as part of director's fees",
            " after transfer of {units} as part of director's fees",
            direction="falls",
            received=True,
        ),
        "unstated": _title_body_rule(
            None,
            None,
            " in payment of director's fees",
            " in payment of director's fees",
        ),
    },
    "management-fees": {
        "gives": _title_body_rule(
            "{holder} passes on {company} {units} received as management fees",
            "passed on",
            " received as management fees",
            " after passing on {units} received as management fees",
            direction="falls",
            received=True,
        ),
        "unstated": _title_body_rule(
            None,
            None,
            " in payment of management fees",
            " in payment of management fees",
        ),
    },
}

# Checkbox labels that carry no tag of their own, keyed the same way as tags.
TITLE_BODY_CIRCUMSTANCE_RULES = {
    "Securities via market transaction": {
        "buy": _title_body_rule(
            "{holder} buys {units} of {company}",
            "bought",
            " in a market transaction",
            " after {unit} purchase",
            direction="rises",
        ),
        "sell": _title_body_rule(
            "{holder} sells {units} of {company}",
            "sold",
            " in a market transaction",
            " after {unit} sale",
            direction="falls",
        ),
    },
    "Securities via off-market transaction": {
        "buy": _title_body_rule(
            "{holder} buys {units} of {company} in off-market deal",
            "bought",
            " in an off-market transaction",
            " after off-market purchase",
            direction="rises",
        ),
        "sell": _title_body_rule(
            "{holder} sells {units} of {company} in off-market deal",
            "sold",
            " in an off-market transaction",
            " after off-market sale",
            direction="falls",
        ),
    },
    "Securities via physical settlement": {
        "buy": _title_body_rule(
            "{holder} acquires {units} of {company} via physical settlement",
            "acquired",
            " through physical settlement of derivatives or other securities",
            " after physical settlement",
            direction="rises",
        ),
    },
    "Securities via a placement": {
        "buy": _title_body_rule(
            "{holder} acquires {units} of {company} via placement",
            "acquired",
            " through a placement",
            " after placement",
            direction="rises",
        ),
    },
    "Securities following conversion/exercise": {
        "buy": _title_body_rule(
            "{holder} acquires {units} of {company} via conversion or exercise",
            "acquired",
            " following the conversion or exercise of securities",
            " after conversion or exercise",
            direction="rises",
        ),
    },
    "Acceptance of take-over offer for Listed Issuer": {
        "sell": TITLE_BODY_TAG_RULES["takeover"]["sell"],
    },
    CORPORATE_ACTION_CHECKBOX_KEY: {
        "default": _title_body_rule(
            None,
            None,
            " following a corporate action by the company",
            " after corporate action",
        ),
    },
}

# Last resort, when neither a tag nor a checkbox label gives a rule.
TITLE_BODY_TYPE_RULES = {
    "buy": _title_body_rule(
        "{holder} buys {units} of {company}",
        "bought",
        "",
        " after {unit} purchase",
        direction="rises",
    ),
    "sell": _title_body_rule(
        "{holder} sells {units} of {company}",
        "sold",
        "",
        " after {unit} sale",
        direction="falls",
    ),
    "default": _title_body_rule(None, None, "", ""),
}
