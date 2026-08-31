from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from botocore.exceptions import BotoCoreError, ClientError

from sgx_scraper.config.settings import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, SENDER_EMAIL, TO_EMAIL
)
from sgx_scraper.alerting.utils.send_alert_helper import attach_files

import boto3
import logging


LOGGER = logging.getLogger(__name__)


def send_sgx_alert_email(
    subject: str,
    body_text: str,
    body_html: str,
    attachments_path: list[str] | None = None,
    to_emails: str | list[str] | None = None,
):
    recipients = to_emails or TO_EMAIL

    if isinstance(recipients, str):
        recipients = [
            address.strip()
            for address in recipients.split(",")
            if address.strip()
        ]

    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = SENDER_EMAIL
    message["To"] = ", ".join(recipients)

    alternative_message = MIMEMultipart("alternative")
    alternative_message.attach(
        MIMEText(body_text, "plain")
    )
    alternative_message.attach(
        MIMEText(body_html, "html")
    )

    message.attach(alternative_message)

    if attachments_path:
        for file_path in attachments_path:
            attach_files(file_path, message)

    ses_client = boto3.client(
        "ses",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        response = ses_client.send_raw_email(
            Source=SENDER_EMAIL,
            Destinations=recipients,
            RawMessage={"Data": message.as_string()},
        )

        LOGGER.info(
            "Email sent | message_id=%s",
            response.get("MessageId"),
        )

    except ClientError as error:
        error_code = error.response["Error"].get(
            "Code",
            "Unknown",
        )
        error_message = error.response["Error"].get(
            "Message",
            "No message provided",
        )

        LOGGER.error(
            "[send_sgx_alert_email] AWS ClientError [%s]: %s",
            error_code,
            error_message,
        )

    except BotoCoreError as error:
        LOGGER.error(
            "[send_sgx_alert_email] BotoCoreError: %s",
            error,
        )

    except Exception:
        LOGGER.exception(
            "[send_sgx_alert_email] Unexpected error"
        )