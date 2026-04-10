from datetime import datetime 

from sgx_scraper.alerting.utils.send_alert_helper import escape_keyword 

import json 
import logging


LOGGER = logging.getLogger(__name__)


def get_data_to_alert(path: str) -> list[dict[str, any]]:
    try:
        with open(path, 'r') as file:
            data_to_alert = json.load(file)
        return data_to_alert
    except Exception as error:
        LOGGER.error(f"Error loading data to alert from {path}: {error}") 
        return []


def build_email_subject(title, alerts):
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


def render_email_content(alerts: list[dict[str, any]], title: str = "SGX Transaction Alerts") -> tuple[str, str, str]:
    subject, total, today = build_email_subject(title, alerts)
    body_text = build_plain_text_body(alerts, title, total, today)
    body_html = build_html_body(alerts, title, total, today, escape_keyword)
    return subject, body_text, body_html


if __name__ == "__main__":
    data_to_alert = get_data_to_alert('data/scraper_output/sgx_filing/sgx_filings_not_insertable.json')
    data_to_alert = data_to_alert[:5]
    subject, body_text, body_html = render_email_content(data_to_alert)
    print(f"Subject: {subject}\n")
    print(f"Body (text): {body_text}\n")
    print(f"Body (HTML): {body_html}\n")