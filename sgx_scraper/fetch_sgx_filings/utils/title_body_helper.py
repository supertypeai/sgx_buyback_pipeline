from sgx_scraper.fetch_sgx_filings.utils.holder_helper import get_display_holder_name
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    OTHER_CIRCUMSTANCES_RULES,
    ACQUISITION_RULES,
    TITLE_BODY_SIDED_TAG_RULES,
    TITLE_BODY_TAG_PRIORITY,
    TITLE_BODY_RECEIPT_TAGS,
    TITLE_BODY_TAG_RULES,
    TITLE_BODY_CIRCUMSTANCE_RULES,
    TITLE_BODY_TYPE_RULES,
    UNIT_ISSUER_PATTERN,
)

import re


def format_percentage(value: float | None) -> str | None:
    if value is None:
        return None

    # A real but tiny stake would otherwise print as 0.00%
    if 0 < value * 100 < 0.005:
        return "less than 0.01%"

    return f"{value * 100:.2f}%"


def format_price(value: float) -> str:
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def get_interest_change_scope(record: dict[str, any]) -> str | None:
    # Which interest moved: "direct", "deemed", "both", or None when neither
    # moved or the share table is incomplete
    direct_before = record.get("direct_before")
    direct_after = record.get("direct_after")
    deemed_before = record.get("deemed_before")
    deemed_after = record.get("deemed_after")

    if None in (direct_before, direct_after, deemed_before, deemed_after):
        return None

    direct_changed = direct_before != direct_after
    deemed_changed = deemed_before != deemed_after

    if direct_changed and deemed_changed:
        return "both"

    if deemed_changed:
        return "deemed"

    if direct_changed:
        return "direct"

    return None


def get_percentage_change(
    record: dict[str, any],
    prefix: str,
) -> tuple[str, str] | None:
    # The before and after percentages as filed, or None when either is
    # missing or they are equal once formatted
    before = format_percentage(record.get(f"{prefix}_percentage_before"))
    after = format_percentage(record.get(f"{prefix}_percentage_after"))

    if before is None or after is None or before == after:
        return None

    return before, after


def get_consistent_holder_side(record: dict[str, any]) -> str | None:
    # The side comes from the filing's wording. The share table never chooses
    # it, but it can veto it: "gives" while the total rose, or "receives"
    # while it fell, contradicts the form, so no side is asserted. This
    # happens when one remark describes two transactions of the same filing
    holder_side = record.get("_holder_side")
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    if holder_side is None or holding_before is None or holding_after is None:
        return holder_side

    contradicts_table = (
        (holder_side == "gives" and holding_after > holding_before)
        or (holder_side == "receives" and holding_after < holding_before)
    )

    return None if contradicts_table else holder_side


def select_sided_tag_rule(
    tag: str,
    tag_rules: dict[str, any],
    transaction_type: str | None,
    circumstance_key: str | None,
    holder_side: str | None,
) -> dict[str, any] | None:
    # Returns the wording for a tag whose side depends on the filing's free
    # text, or None when the tag's ordinary rule applies
    sided_rules = TITLE_BODY_SIDED_TAG_RULES.get(tag)

    if not sided_rules or transaction_type in tag_rules:
        return None

    checkbox_rule = (
        ACQUISITION_RULES.get(circumstance_key)
        or OTHER_CIRCUMSTANCES_RULES.get(circumstance_key)
        or {}
    )

    # A ticked box fixes the side, so only free-text tags are looked at
    if tag in checkbox_rule.get("tags", []) or holder_side == "receives":
        return None

    if holder_side == "gives" and "gives" in sided_rules:
        return sided_rules["gives"]

    return sided_rules["unstated"]


def select_title_body_rule(
    transaction_type: str | None,
    tags: list[str],
    circumstance_key: str | None,
    holder_side: str | None = None,
) -> dict[str, any]:
    for tag in TITLE_BODY_TAG_PRIORITY:
        if tag not in tags:
            continue

        tag_rules = TITLE_BODY_TAG_RULES[tag]

        if (
            transaction_type == "sell"
            and "sell" not in tag_rules
            and tag in TITLE_BODY_RECEIPT_TAGS
        ):
            continue

        sided_rule = select_sided_tag_rule(
            tag, tag_rules, transaction_type, circumstance_key, holder_side
        )

        if sided_rule:
            return sided_rule

        return tag_rules.get(transaction_type) or tag_rules["default"]

    circumstance_rules = TITLE_BODY_CIRCUMSTANCE_RULES.get(circumstance_key) or {}
    rule = (
        circumstance_rules.get(transaction_type)
        or circumstance_rules.get("default")
    )

    if rule:
        return rule

    return (
        TITLE_BODY_TYPE_RULES.get(transaction_type)
        or TITLE_BODY_TYPE_RULES["default"]
    )


def get_security_nouns(
    security_noun: str | None,
    company_name: str | None,
) -> tuple[str, str]:
    # Plural and singular words for the securities. The parser reads the noun
    # from the form (stapled securities, units, or shares); the company name is
    # only a fallback when the form could not be read
    if security_noun is None:
        is_unit_issuer = re.search(
            UNIT_ISSUER_PATTERN, company_name or "", re.IGNORECASE
        )
        security_noun = "units" if is_unit_issuer else "shares"

    if security_noun == "stapled securities":
        return security_noun, "stapled security"

    return security_noun, security_noun[:-1]


def build_notification_title_and_sentence(
    record: dict[str, any],
    holder_name: str,
    company_name: str,
    unit: str,
) -> tuple[str | None, str]:
    # Form 3 states when a holder becomes or ceases to be substantial, which
    # is the event the filing reports
    notification_type = record.get("notification_type")
    role = f"substantial {unit}holder"

    if notification_type == "becoming":
        title = f"{holder_name} becomes {role} of {company_name}"
        stake = format_percentage(record.get("share_percentage_after"))

        if stake is not None:
            title += f" with {stake} stake"

        return title, f" With this, the holder becomes a {role} of {company_name}."

    if notification_type == "ceasing":
        return (
            f"{holder_name} ceases to be {role} of {company_name}",
            f" With this, the holder ceases to be a {role} of {company_name}.",
        )

    return None, ""


NUMBER_WORDS = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}


def build_transaction_sentence(
    record: dict[str, any],
    units: str,
    unit: str,
    received: bool,
) -> tuple[str, str]:
    # Returns the amount text and the price clause that follows it.
    # A form that states the amount as 0 or "Nil" gives no amount
    amount = record.get("amount_transaction") or None
    price_per_share = record.get("price_per_share")
    transaction_value = record.get("transaction_value")

    amount_text = f"{amount:,} {units}" if amount is not None else units

    if not price_per_share:
        return amount_text, ""

    tranches = record.get("_price_components") or []

    # Several tranches at different prices: the first sentence gives the
    # total, and a second sentence, closed by the caller's full stop, lists
    # each tranche
    if len(tranches) > 1 and all(
        tranche.get("price") is not None and tranche.get("amount") is not None
        for tranche in tranches
    ):
        total_text = ""

        if transaction_value:
            total_text = (
                f", valued at S${transaction_value:,} in total"
                if received
                else f", for a total of S${transaction_value:,}"
            )

        tranche_texts = [
            f"{tranche['amount']:,} at S${format_price(tranche['price'])} per {unit}"
            for tranche in tranches
        ]
        tranche_count = NUMBER_WORDS.get(len(tranches), str(len(tranches)))

        return amount_text, (
            f"{total_text}. The {units} came in {tranche_count} tranches: "
            f"{', '.join(tranche_texts[:-1])} and {tranche_texts[-1]}"
        )

    price = f"S${format_price(price_per_share)} per {unit}"

    if received:
        price_text = f", valued at {price}"

        if transaction_value:
            price_text += f", or S${transaction_value:,} in total"

        return amount_text, price_text

    price_text = f" at {price}"

    if transaction_value:
        price_text += f", for a total of S${transaction_value:,}"

    return amount_text, price_text


def build_holdings_sentence(
    record: dict[str, any],
    units: str,
    stake_already_stated: bool,
) -> str:
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    if holding_before is None or holding_after is None:
        return ""

    percentage_before = format_percentage(record.get("share_percentage_before"))
    percentage_after = format_percentage(record.get("share_percentage_after"))
    percentage_change = get_percentage_change(record, "share")

    if holding_after == holding_before:
        sentence = f" The number of {units} held is unchanged at {holding_after:,}"

        if percentage_change and not stake_already_stated:
            sentence += (
                f", while the total stake moves from {percentage_change[0]} "
                f"to {percentage_change[1]}"
            )

        return sentence + "."

    direction = "rises" if holding_after > holding_before else "falls"
    before_text = f"{holding_before:,} {units}"
    after_text = f"{holding_after:,} {units}"

    if percentage_before is None or percentage_after is None:
        return f" Total interest {direction} from {before_text} to {after_text}."

    if percentage_before == percentage_after:
        # A move too small to change the rounded percentage. A stake the
        # form gives as 0% is not quoted, since "unchanged at 0.00%" says less
        stake_text = "" if percentage_after == "0.00%" else f" at {percentage_after}"

        return (
            f" Total interest {direction} from {holding_before:,} to "
            f"{after_text}, with the stake unchanged{stake_text}."
        )

    return (
        f" Total interest {direction} from {before_text} ({percentage_before}) "
        f"to {after_text} ({percentage_after})."
    )


def format_compact_number(value: float) -> str:
    # Headline figures: exact below 100,000, then 250K, 1.8M, 1.2B
    if value < 100_000:
        return f"{round(value):,}"

    if round(value / 1_000) < 1_000:
        return f"{value / 1_000:.0f}K"

    if round(value / 1_000_000, 1) < 1_000:
        return f"{value / 1_000_000:.1f}M"

    return f"{value / 1_000_000_000:.1f}B"


def get_title_quantity(
    record: dict[str, any],
    rule: dict[str, any],
) -> tuple[str | None, bool]:
    # What a title says about the size of the transaction, and whether it is
    # a value. A purchase or sale is stated by its total value when the
    # filing gives one. Securities that were received, and trades with no
    # price, are stated by the number of securities
    amount = record.get("amount_transaction") or None
    transaction_value = record.get("transaction_value") or None

    if (
        transaction_value
        and not rule["received"]
        and record.get("transaction_type") in ("buy", "sell")
    ):
        return f"S${format_compact_number(transaction_value)}", True

    if amount:
        return format_compact_number(amount), False

    return None, False


def build_action_title(
    rule: dict[str, any],
    names: dict[str, str],
    quantity: str | None,
    is_value: bool,
) -> str:
    # Title for a holder who acted directly, with the quantity worked into
    # the rule's own title
    template = rule["title"]

    if quantity is None:
        return template.format(**names)

    if "{units} of {company}" in template:
        replacement = (
            f"{quantity} of {{company}} {{units}}"
            if is_value
            else f"{quantity} {{units}} of {{company}}"
        )
        template = template.replace("{units} of {company}", replacement, 1)

    elif "{company} {units}" in template:
        template = template.replace(
            "{company} {units}", f"{quantity} {{company}} {{units}}", 1
        )

    elif rule.get("title_with_quantity") and not is_value:
        template = rule["title_with_quantity"].replace("{quantity}", quantity)

    return template.format(**names)


def build_deemed_reason(
    rule: dict[str, any],
    names: dict[str, str],
    quantity: str | None,
    is_value: bool,
) -> str:
    # The "after ..." part of a deemed-interest title, with the quantity:
    # "after S$8.0M share sale", "after purchase of 1.8M shares"
    template = rule["reason"]

    if quantity is None:
        return template.format(**names)

    if is_value:
        # "after S$8.0M share sale", but "after exercise of warrants worth
        # S$37.3M" where the value cannot sit in front
        if re.search(r"(?:purchase|sale|subscription)$", template):
            template = template.replace(" after ", f" after {quantity} ", 1)

        else:
            template += f" worth {quantity}"

    elif "{unit} purchase" in template or "{unit} sale" in template:
        template = (
            template
            .replace("{unit} purchase", f"purchase of {quantity} {{units}}")
            .replace("{unit} sale", f"sale of {quantity} {{units}}")
        )

    elif "{unit} award" in template:
        template = template.replace("{unit} award", f"award of {quantity} {{units}}")

    elif re.search(r" (?:in|of|on) \{units\}", template):
        template = template.replace("{units}", f"{quantity} {{units}}", 1)

    elif re.search(
        r"(?:purchase|sale|subscription|placement|dividend|specie|inheritance)$",
        template,
    ):
        template += f" of {quantity} {{units}}"

    return template.format(**names)


def get_primary_holder(record: dict[str, any]) -> str | None:
    # The holder in the same filing that holds the securities directly, when
    # it carried out the whole transaction. Part IV gives one amount for
    # every holder, and several funds of a group may each have done part of
    # it, so the direct holder is named only when its own total moved by
    # that amount
    amount = record.get("amount_transaction") or None
    change = record.get("_primary_holder_change")

    if amount is not None and change is not None and amount != change:
        return None

    return record.get("_primary_holder")


def is_group_amount(record: dict[str, any]) -> bool:
    # Part IV of a multi-holder Form 3 gives one amount for all the holders
    # giving notice. A holder whose own total moved by a different number
    # did only part of it, so the amount is the group's and not its own
    amount = record.get("amount_transaction") or None
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    return bool(
        record.get("filing_type") == "form_3_part_iii_iv"
        and amount is not None
        and holding_before is not None
        and holding_after is not None
        and abs(holding_after - holding_before) != amount
    )


def is_unmoved_joint_holder(record: dict[str, any]) -> bool:
    # A holder named on a multi-holder Form 3 whose direct, deemed and total
    # holdings are all unchanged. Part IV's one transaction, and its ticked
    # box, belong to the other holders giving notice
    holdings = [
        record.get(f"{prefix}_{section}")
        for prefix in ("direct", "deemed", "holding")
        for section in ("before", "after")
    ]

    return (
        record.get("filing_type") == "form_3_part_iii_iv"
        and None not in holdings
        and holdings[0::2] == holdings[1::2]
    )


# Stands in for the acting party when the filing does not name one
UNNAMED_ACTING_PARTY = "a party linked to the holder"


def needs_acting_party(record: dict[str, any]) -> bool:
    # True when only the holder's deemed interest moved in a transaction a
    # related party carried out, and no other holder in the filing is known
    # to be that party. The body then has nobody to name
    rule = select_title_body_rule(
        transaction_type=record.get("transaction_type"),
        tags=record.get("tags") or [],
        circumstance_key=record.get("circumstance_key"),
        holder_side=get_consistent_holder_side(record),
    )

    return bool(
        rule["verb"]
        and get_interest_change_scope(record) == "deemed"
        and not get_primary_holder(record)
    )


def generate_title_and_body(record: dict[str, any]) -> tuple[str, str]:
    holder_name = get_display_holder_name(record)
    company_name = record.get("company_name")
    amount = record.get("amount_transaction") or None

    units, unit = get_security_nouns(record.get("_security_noun"), company_name)

    rule = select_title_body_rule(
        transaction_type=record.get("transaction_type"),
        tags=record.get("tags") or [],
        circumstance_key=record.get("circumstance_key"),
        holder_side=get_consistent_holder_side(record),
    )

    names = {
        "holder": holder_name,
        "company": company_name,
        "units": units,
        "unit": unit,
    }

    detail = rule["detail"].format(**names)
    reason = rule["reason"].format(**names)

    interest_scope = get_interest_change_scope(record)
    deemed_only = interest_scope == "deemed"

    amount_text, price_text = build_transaction_sentence(
        record, units, unit, rule["received"]
    )
    
    stake_already_stated = False
    quantity, quantity_is_value = get_title_quantity(record, rule)
    group_sentence = ""

    if rule["verb"] and is_group_amount(record):
        # The title and the first sentence state no figure, and the amount
        # is given as the group's
        group_sentence = (
            f" The holders giving notice together {rule['verb']} "
            f"{amount_text}{price_text}."
        )
        quantity, amount_text, price_text = None, units, ""

    # A ticked acquisition or disposal box that no longer gives a type is one
    # that normalize_classification took from a holder who did not take part
    ticked_trade_box = (
        ACQUISITION_RULES.get(record.get("circumstance_key"), {})
        .get("transaction_type") == "buy"
    )

    if (
        ticked_trade_box
        and record.get("transaction_type") == "others"
        and is_unmoved_joint_holder(record)
    ):
        title = f"{holder_name} reports no change in interest in {company_name}"
        body = (
            f"{holder_name} reported no change in interest in {company_name}. "
            "The notice was given jointly with other holders, whose "
            "interests changed."
        )

    elif rule["verb"] and not deemed_only:
        # The holder acted directly
        title = build_action_title(rule, names, quantity, quantity_is_value)
        body = (
            f"{holder_name} {rule['verb']} {amount_text} of {company_name}"
            f"{detail}{price_text}."
        )

    elif rule["verb"]:
        # A related party acted, so only the holder's deemed interest moved
        past_direction = "rose" if rule["direction"] == "rises" else "fell"
        title = (
            f"{holder_name}'s deemed interest in {company_name} "
            f"{rule['direction']}"
            f"{build_deemed_reason(rule, names, quantity, quantity_is_value)}"
        )
        # The party that acted: another holder in the same filing that holds
        # the securities directly, or the party the filing's text names. A
        # filing that names nobody still says the holder did not act
        acting_party = (
            get_display_holder_name(record, "_primary_holder")
            if get_primary_holder(record)
            else record.get("acting_party") or UNNAMED_ACTING_PARTY
        )

        body = (
            f"{holder_name}'s deemed interest in {company_name} "
            f"{past_direction} after {acting_party} "
            f"{rule['verb']} {amount_text}{detail}{price_text}."
        )

    else:
        # No stated direction, so report the stake percentages as filed
        stake = "deemed interest" if deemed_only else "stake"
        percentage_prefix = "deemed" if deemed_only else "share"
        percentage_change = get_percentage_change(record, percentage_prefix)

        # An untagged "others" keeps a plain title; the body gives the reason
        is_untagged = rule is TITLE_BODY_TYPE_RULES["default"]

        if percentage_change and not is_untagged:
            before, after = percentage_change
            increased = (
                record[f"{percentage_prefix}_percentage_after"]
                > record[f"{percentage_prefix}_percentage_before"]
            )
            direction = "rises" if increased else "falls"
            title = (
                f"{holder_name}'s {stake} in {company_name} {direction} "
                f"to {after} from {before}{reason}"
            )
            body = (
                f"{holder_name}'s {stake} in {company_name} moved from "
                f"{before} to {after}{detail}."
            )
            stake_already_stated = not deemed_only

        else:
            interest = "deemed interest" if deemed_only else "interest"
            title = (
                f"{holder_name} reports change in {interest} in "
                f"{company_name}{reason}"
            )
            body = (
                f"{holder_name} reported a change in {interest} in "
                f"{company_name}{detail}."
            )

        # With no tag, nothing in the wording above says what happened, so the
        # reason read from the filing is given
        change_reason = record.get("change_reason")

        if change_reason and not (record.get("tags") or []):
            body += f" The filing attributes the change to {change_reason}."

        if amount is not None:
            # A holder whose direct and deemed interests both moved, by
            # different numbers, may state only its own securities as the
            # amount. Securities moved between the two are one number
            direct_only = (
                interest_scope == "both"
                and amount == abs(record["direct_after"] - record["direct_before"])
                and amount != abs(record["deemed_after"] - record["deemed_before"])
            )
            held = " held directly" if direct_only else ""

            body += f" The transaction involved {amount_text}{held}{price_text}."

    body += group_sentence

    notification_title, notification_sentence = (
        build_notification_title_and_sentence(
            record, holder_name, company_name, unit
        )
    )

    if notification_title:
        title = notification_title

    if deemed_only:
        direct_after = record.get("direct_after")

        if direct_after:
            body += (
                f" The direct holding is unchanged at {direct_after:,} {units}."
            )

        else:
            body += f" No {units} are held directly."

    elif interest_scope == "both":
        body += (
            f" The direct interest moved from {record.get('direct_before'):,} "
            f"to {record.get('direct_after'):,} {units} and the deemed interest "
            f"from {record.get('deemed_before'):,} to "
            f"{record.get('deemed_after'):,} {units}."
        )

    body += build_holdings_sentence(record, units, stake_already_stated)
    body += notification_sentence

    # A name ending in a full stop ("Pte. Ltd.") must not double the sentence end
    body = body.replace("..", ".")

    return title, body
