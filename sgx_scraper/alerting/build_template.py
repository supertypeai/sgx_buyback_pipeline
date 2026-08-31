from datetime import datetime 

from sgx_scraper.alerting.utils.send_alert_helper import escape_keyword 

import logging


LOGGER = logging.getLogger(__name__)


def build_email_subject(
    title: str,
    alerts: list[dict],
) -> tuple[str, int, str]:
    total = len(alerts)
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"[{title}] {total} alert(s) — {today}"

    return subject, total, today


def build_plain_text_body(alerts, title, total, today):
    lines = [f"{title} — {total} alert(s) on {today}", "-" * 40]

    for index, alert in enumerate(alerts, 1):
        symbol = alert.get("symbol", "-")
        holder = alert.get("holder_name", "-")
        type = alert.get("transaction_type", "-")
        price = alert.get("price_per_share", "-")
        shares = alert.get("amount_transaction", "-")
        transaction_value = alert.get("transaction_value", "-")
        date = alert.get("timestamp", "-")
        url = alert.get("source", "-")

        before = alert.get("holding_before", "-")
        after = alert.get("holding_after", "-")
        before_pct = alert.get("share_percentage_before", "-")
        after_pct = alert.get("share_percentage_after", "-")

        lines.append(
            f"{index}. {symbol} | {type} | holder={holder} | "
            f"shares={shares} | price={price} | value={transaction_value} | date={date}"
        )
        lines.append(
            f"   before={before} ({before_pct}%), after={after} ({after_pct}%)"
        )
        lines.append(f"   src: {url}")

    return "\n".join(lines)


def build_html_body(alerts, title, total, today, escape_keyword):
    rows = []
    for alert in alerts:
        symbol = alert.get("symbol", "-")
        holder = alert.get("holder_name", "-")
        type = alert.get("transaction_type", "-")
        price = alert.get("price_per_share", "-")
        shares = alert.get("amount_transaction", "-")
        transaction_value = alert.get("transaction_value", "-")
        date = alert.get("timestamp", "-")
        url = alert.get("source", "-")

        before = alert.get("holding_before", "-")
        after = alert.get("holding_after", "-")
        before_pct = alert.get("share_percentage_before", "-")
        after_pct = alert.get("share_percentage_after", "-")

        link = (
            f'<a href="{escape_keyword(url)}" target="_blank" rel="noopener">{escape_keyword(url)}</a>'
            if url and url != "-"
            else "-"
        )

        rows.append(
            "<tr>"
            f"<td>{escape_keyword(date)}</td>"
            f"<td><strong>{escape_keyword(symbol)}</strong></td>"
            f"<td>{escape_keyword(holder)}</td>"
            f"<td>{escape_keyword(type)}</td>"
            f"<td style='text-align:right'>{escape_keyword(shares)}</td>"
            f"<td style='text-align:right'>{escape_keyword(price)}</td>"
            f"<td style='text-align:right'>{escape_keyword(transaction_value)}</td>"
            f"<td style='text-align:right'>{escape_keyword(before)} ({escape_keyword(before_pct)}%) → "
            f"{escape_keyword(after)} ({escape_keyword(after_pct)}%)</td>"
            f"<td style='max-width:320px;overflow-wrap:anywhere'>{link}</td>"
            "</tr>"
        )

    table = (
        "<table style='border-collapse:collapse;width:100%;font-family:system-ui,Arial'>"
        "<thead>"
        "<tr style='background:#f3f4f6'>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Date</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Symbol</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Holder</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Type</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:right'>Shares</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:right'>Price</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:right'>Value</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:right'>Before → After</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Source</th>"
        "</tr>"
        "</thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )

    html = (
        f"<div>"
        f"<h2 style='font-family:system-ui,Arial;margin:0 0 8px'>{escape_keyword(title)}</h2>"
        f"<p style='margin:0 0 12px;color:#6b7280'>{total} alert(s) — {today}</p>"
        f"{table}"
        f"</div>"
    )

    return html


def render_filing_email_content(
    alerts: list[dict[str, any]], 
    title: str = "SGX Transaction Alerts"
) -> tuple[str, str, str]:
    subject, total, today = build_email_subject(title, alerts)
    body_text = build_plain_text_body(alerts, title, total, today)
    body_html = build_html_body(alerts, title, total, today, escape_keyword)
    return subject, body_text, body_html


def build_management_plain_text_body(
    alerts: list[dict],
    title: str,
    total: int,
    today: str,
) -> str:
    lines = [
        f"{title} — {total} alert(s) on {today}",
        "-" * 40,
    ]

    for index, alert in enumerate(alerts, 1):
        symbol = alert.get("symbol", "-")
        company_name = alert.get("company_name", "-")
        management_name = alert.get("name", "-")
        position = alert.get("position", "-")
        issue = alert.get("issue", "-")
        annual_report_url = alert.get("annual_report_url", "-")

        lines.append(
            f"{index}. {symbol} | {company_name}"
        )
        lines.append(
            f"   Name: {management_name}"
        )
        lines.append(
            f"   Position: {position}"
        )
        lines.append(
            f"   Issue: {issue}"
        )
        lines.append(
            f"   Annual report: {annual_report_url}"
        )
        lines.append("")

    return "\n".join(lines)


def build_management_html_body(
    alerts: list[dict],
    title: str,
    total: int,
    today: str,
    escape_keyword,
) -> str:
    rows = []

    for alert in alerts:
        symbol = alert.get("symbol", "-")
        company_name = alert.get("company_name", "-")
        management_name = alert.get("name", "-")
        position = alert.get("position", "-")
        issue = alert.get("issue", "-")
        annual_report_url = alert.get("annual_report_url", "-")

        annual_report_link = (
            (
                f'<a href="{escape_keyword(annual_report_url)}" '
                f'target="_blank" rel="noopener">Annual Report</a>'
            )
            if annual_report_url and annual_report_url != "-"
            else "-"
        )

        rows.append(
            "<tr>"
            f"<td>{escape_keyword(symbol)}</td>"
            f"<td>{escape_keyword(company_name)}</td>"
            f"<td>{escape_keyword(management_name)}</td>"
            f"<td>{escape_keyword(position)}</td>"
            f"<td>{escape_keyword(issue)}</td>"
            f"<td>{annual_report_link}</td>"
            "</tr>"
        )

    table = (
        "<table style='border-collapse:collapse;width:100%;font-family:system-ui,Arial'>"
        "<thead>"
        "<tr style='background:#f3f4f6'>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Symbol</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Company</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Name</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Position</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Issue</th>"
        "<th style='padding:8px;border:1px solid #e5e7eb;text-align:left'>Source</th>"
        "</tr>"
        "</thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )

    return (
        "<div>"
        f"<h2 style='font-family:system-ui,Arial;margin:0 0 8px'>"
        f"{escape_keyword(title)}"
        "</h2>"
        f"<p style='margin:0 0 12px;color:#6b7280'>"
        f"{total} alert(s) — {escape_keyword(today)}"
        "</p>"
        f"{table}"
        "</div>"
    )


def render_management_email_content(
    alerts: list[dict],
    title: str = "SGX Management Alerts",
) -> tuple[str, str, str]:
    subject, total, today = build_email_subject(
        title=title,
        alerts=alerts,
    )

    body_text = build_management_plain_text_body(
        alerts=alerts,
        title=title,
        total=total,
        today=today,
    )

    body_html = build_management_html_body(
        alerts=alerts,
        title=title,
        total=total,
        today=today,
        escape_keyword=escape_keyword,
    )

    return subject, body_text, body_html