from sgx_scraper.utils.json_helper import open_json

import re 


SLUG_PATTERN = re.compile(r"[^A-Za-z0-9]+")


def fmt_int(value) -> str:
    return f'{value:,}' if value is not None else '-'


def fmt_sgd(value) -> str:
    return f'SGD {value:,}' if value is not None else '-'


def fmt_value(value) -> str:
    if value is None:
        return "-"

    return str(value)


def to_kebab(value: str | None) -> str:
    if not value:
        return "unknown"
    
    return SLUG_PATTERN.sub("-", value.strip()).strip("-").lower()


def format_filing_input(filing: dict) -> str:
    lines = [
        f"symbol: {filing.get('symbol') or '-'}",
        f"company name: {filing.get('issuer_name') or '-'}",
        f"holder name: {filing.get('holder_name') or '-'}",
        f"holder type: {filing.get('holder_type') or '-'}",
        f"transaction type: {filing.get('transaction_type') or '-'}",
        f"shares transacted: {fmt_int(filing.get('amount_transaction'))}",
        f"transaction value: {fmt_sgd(filing.get('transaction_value'))}",
        f"price_per_share: {fmt_sgd(filing.get('price_per_share'))}",
        f"holding before: {fmt_int(filing.get('holding_before'))}",
        f"holding after: {fmt_int(filing.get('holding_after'))}",
        f"ownership before: {filing.get('share_percentage_before')}",
        f"ownership after: {filing.get('share_percentage_after')}",
        f"timestamp: {filing.get('timestamp') or '-'}",
        f"tags: {', '.join(filing.get('tags') or []) or '-'}",
        f"circumstances: {filing.get('circumstances_desc') or '-'}"
    ]
    return '\n'.join(lines)


def format_upcoming_dividend_input(dividend: dict) -> str:
    companies_path = "data/sgx_companies.json"
    companies = open_json(companies_path)

    company = companies.get(dividend.get("symbol")) or {}
    company_name = company.get("name") or "-"
    event_narrative = dividend.get('event_narrative') or {}

    lines = [
        f"symbol: {dividend.get('symbol') or '-'}",
        f"company_name: {company_name}",
        f"reference: {dividend.get('reference') or '-'}",
        f"record date: {dividend.get('recording_date') or '-'}",
        f"ex date: {dividend.get('ex_date') or '-'}",
        f"dividend amount per share: {fmt_sgd(dividend.get('dividend_amount'))}",
        f"payment date: {dividend.get('payment_date') or '-'}",
        f"payment type: {dividend.get('payment_type') or '-'}",
        f"dividend type: {dividend.get('dividend_type') or '-'}",
        f"narrative: {event_narrative.get('narrative') or '-'}",
        f"information conditions: {event_narrative.get('information_conditions') or '-'}",
    ]

    return '\n'.join(lines)


def format_appointment_input(appointment: dict) -> str:
    lines = [
        "event type: appointment",
        f"symbol: {fmt_value(appointment.get('symbol'))}",
        f"company name: {fmt_value(appointment.get('company_name'))}",
        f"source: {fmt_value(appointment.get('source'))}",
        f"reference: {fmt_value(appointment.get('reference'))}",
        f"timestamp: {fmt_value(appointment.get('timestamp'))}",
        (
            "announcement subtitle: "
            f"{fmt_value(appointment.get('announcement_subtitle'))}"
        ),
        f"name: {fmt_value(appointment.get('name'))}",
        f"age: {fmt_value(appointment.get('age'))}",
        f"effective date: {fmt_value(appointment.get('effective_date'))}",
        f"position: {fmt_value(appointment.get('position'))}",
        f"executive status: {fmt_value(appointment.get('executive_status'))}",
        f"description: {fmt_value(appointment.get('description'))}",
        f"board comments: {fmt_value(appointment.get('board_comments'))}",
        (
            "professional qualifications: "
            f"{fmt_value(appointment.get('professional_qualifications'))}"
        ),
        (
            "recent work experience: "
            f"{fmt_value(appointment.get('recent_work_experience'))}"
        ),
        (
            "shareholding details: "
            f"{fmt_value(appointment.get('shareholding_details'))}"
        ),
    ]

    return "\n".join(lines)


def format_cessation_input(cessation: dict) -> str:
    lines = [
        "event type: cessation",
        f"symbol: {fmt_value(cessation.get('symbol'))}",
        f"company name: {fmt_value(cessation.get('company_name'))}",
        f"source: {fmt_value(cessation.get('source'))}",
        f"reference: {fmt_value(cessation.get('reference'))}",
        f"timestamp: {fmt_value(cessation.get('timestamp'))}",
        (
            "announcement subtitle: "
            f"{fmt_value(cessation.get('announcement_subtitle'))}"
        ),
        f"name: {fmt_value(cessation.get('name'))}",
        f"age: {fmt_value(cessation.get('age'))}",
        f"position: {fmt_value(cessation.get('position'))}",
        (
            "effective date known: "
            f"{fmt_value(cessation.get('effective_date_known'))}"
        ),
        f"effective date: {fmt_value(cessation.get('effective_date'))}",
        (
            "effective date note: "
            f"{fmt_value(cessation.get('effective_date_note'))}"
        ),
        f"description: {fmt_value(cessation.get('description'))}",
        f"cessation reason: {fmt_value(cessation.get('cessation_reason'))}",
        (
            "unresolved board difference: "
            f"{fmt_value(cessation.get('unresolved_board_difference'))}"
        ),
        (
            "shareholder attention required: "
            f"{fmt_value(cessation.get('shareholder_attention_required'))}"
        ),
        (
            "other relevant information: "
            f"{fmt_value(cessation.get('other_relevant_information'))}"
        ),
    ]

    return "\n".join(lines)


def format_takeover_input(takeover: dict) -> str:
    lines = [
        f"symbol: {takeover.get('symbol') or '-'}",
        f"company name: {takeover.get('company_name') or '-'}",
        f"announcement title: {takeover.get('announcement_title') or '-'}",
        f"status: {takeover.get('status') or '-'}",
        f"offer type: {takeover.get('offer_type') or '-'}",
        f"percentage sought: {takeover.get('percentage_sought') or '-'}",
        f"record date: {takeover.get('record_date') or '-'}",
        f"ex date: {takeover.get('ex_date') or '-'}",
        f"withdrawal reason: {takeover.get('withdrawal_reason') or '-'}",
    ]

    event_narratives = takeover.get("event_narrative") or []

    lines.append("")
    lines.append("event narrative:")

    if event_narratives:
        for narrative in event_narratives:
            narrative_type = narrative.get("type") or "-"
            narrative_text = narrative.get("text") or "-"

            lines.append(
                f"- {narrative_type}: {narrative_text}"
            )
    else:
        lines.append("-")

    disbursement_options = takeover.get("disbursement_options") or []

    lines.append("")
    lines.append("disbursement options:")

    if not disbursement_options:
        lines.append("-")

    for option_index, option in enumerate(
        disbursement_options,
        start=1,
    ):
        option_number = option.get("option_number") or option_index

        lines.extend(
            [
                f"",
                f"option {option_number}:",
                (
                    "acceptance period: "
                    f"{option.get('acceptance_period') or '-'}"
                ),
                (
                    "closing time: "
                    f"{option.get('closing_time') or '-'}"
                ),
                (
                    "disbursement type: "
                    f"{option.get('disbursement_type') or '-'}"
                ),
                (
                    "offer price: "
                    f"{option.get('offer_price') or '-'}"
                ),
                (
                    "distribution ratio: "
                    f"{option.get('distribution_ratio') or '-'}"
                ),
                (
                    "fractional disposition method: "
                    f"{option.get('fractional_disposition_method') or '-'}"
                ),
                (
                    "pay date: "
                    f"{option.get('pay_date') or '-'}"
                ),
            ]
        )

        option_narratives = option.get("narrative") or []

        lines.append("option narrative:")

        if option_narratives:
            for narrative in option_narratives:
                narrative_type = narrative.get("type") or "-"
                narrative_text = narrative.get("text") or "-"

                lines.append(
                    f"- {narrative_type}: {narrative_text}"
                )
        else:
            lines.append("-")

    return "\n".join(lines)
