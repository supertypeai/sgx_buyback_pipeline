from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from botocore.exceptions import BotoCoreError, ClientError

from sgx_scraper.alerting.build_template import render_email_content
from sgx_scraper.alerting.filter_data_alert import get_data_alert 
from sgx_scraper.config.settings import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, SENDER_EMAIL, TO_EMAIL, LOGGER
)
from sgx_scraper.alerting.utils.send_alert_helper import attach_files

import boto3


def send_sgx_filings_alert(
        payload_alert: list[dict[str, any]],
        attachments_path: list[str] | None = None
):
    subject, body_text, body_html = (
        render_email_content(payload_alert, title="SGX Non-Insertable Transaction Alerts")
    )

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = TO_EMAIL

    msg_alt = MIMEMultipart("alternative")
    msg_alt.attach(MIMEText(body_text, "plain"))
    msg_alt.attach(MIMEText(body_html, "html"))
    msg.attach(msg_alt)

    if attachments_path:
        for file_path in attachments_path:
            attach_files(file_path, msg)

    ses = boto3.client(
        "ses",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        response = ses.send_raw_email(
            Source=SENDER_EMAIL,
            Destinations=[TO_EMAIL],
            RawMessage={"Data": msg.as_string()},
        )
        message_id = response.get("MessageId")

        LOGGER.info(f"Email sent! Message ID: {message_id}")

    except ClientError as error:
        error_code = error.response["Error"].get("Code", "Unknown")
        error_message = error.response["Error"].get("Message", "No message provided")
        LOGGER.error(f"[send_sgx_filings_alert] AWS ClientError [{error_code}]: {error_message}")

    except BotoCoreError as error:
        LOGGER.error(f"[send_sgx_filings_alert] BotoCoreError: {error}")

    except Exception as error:
        LOGGER.error(f"[send_sgx_filings_alert] Unexpected error: {error}")


if __name__ == '__main__':
    path = 'data/scraper_output/sgx_filing/sgx_filings_today_sep.json'
    alert_insertable, alert_not_insertable = get_data_alert(path) 
    send_sgx_filings_alert(
        payload_alert=alert_not_insertable,
        attachments_path=['data/scraper_output/sgx_filing/manual_check.json']
    )
























