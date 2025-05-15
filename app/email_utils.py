import os
import logging
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import SENDER_EMAIL, SENDER_PASSWORD, SENDER_NAME, VERSION

# Module-level logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Gmail SMTP server settings
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

async def send_email(to_emails, subject, download_url, job_id, logger=None):
    """Send an email notification with the download URL using Gmail SMTP.

    Version: 3.0.4
    """
    logger = logger or default_logger
    try:
        # Create MIME message
        msg = MIMEMultipart()
        msg['From'] = f'{SENDER_NAME} <{SENDER_EMAIL}>'
        msg['To'] = to_emails
        msg['Subject'] = subject
        
        # Set CC recipient
        cc_recipient = 'nik@iconluxurygroup.com' if to_emails != 'nik@iconluxurygroup.com' else 'nik@luxurymarket.com'
        msg['Cc'] = cc_recipient

        # HTML content
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Your file is ready to <a href="{download_url}" class="download-button">download</a></p>            
            <p>--</p>
            <p><small>This is an automated notification.<br>
            User: {to_emails}<br>
            Job ID: {str(job_id)}<br>
            Version: <a href="https://dashboard.iconluxury.group">{VERSION}</a>
            </small>
            </p> 
        </div>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_content, 'html'))

        # Connect and send email
        smtp_client = aiosmtplib.SMTP(
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            use_tls=False,  # Start in non-TLS mode
            start_tls=True  # Automatically handle STARTTLS
        )
        await smtp_client.connect()
        await smtp_client.login(SENDER_EMAIL, SENDER_PASSWORD)
        recipients = [to_emails, cc_recipient]
        await smtp_client.send_message(msg, sender=SENDER_EMAIL, recipients=recipients)
        await smtp_client.quit()

        logger.info(f"📧 Email sent successfully to {to_emails}")
        return True
    except Exception as e:
        logger.error(f"🔴 Error sending email to {to_emails}: {e}", exc_info=True)
        raise

async def send_message_email(to_emails, subject, message, logger=None):
    """Send a plain message email (e.g., for errors) using Gmail SMTP.

    Version: 3.0.4
    """
    logger = logger or default_logger
    try:
        # Create MIME message
        msg = MIMEMultipart()
        msg['From'] = f'{SENDER_NAME} <{SENDER_EMAIL}>'
        msg['To'] = to_emails
        msg['Subject'] = subject
        
        # Set CC recipient
        cc_recipient = 'nik@iconluxurygroup.com' if to_emails != 'nik@iconluxurygroup.com' else 'nik@luxurymarket.com'
        msg['Cc'] = cc_recipient

        # HTML content
        message_with_breaks = message.replace("\n", "<br>")
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Message details:<br>{message_with_breaks}</p>
            <p>--</p>
            <p><small>This is an automated notification.<br>
            Version: <a href="https://dashboard.iconluxury.group">{VERSION}</a>
            <br>
            User: {to_emails}</small></p>
        </div>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_content, 'html'))

        # Connect and send email
        smtp_client = aiosmtplib.SMTP(
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            use_tls=False,  # Start in non-TLS mode
            start_tls=True  # Automatically handle STARTTLS
        )
        await smtp_client.connect()
        await smtp_client.login(SENDER_EMAIL, SENDER_PASSWORD)
        recipients = [to_emails, cc_recipient]
        await smtp_client.send_message(msg, sender=SENDER_EMAIL, recipients=recipients)
        await smtp_client.quit()

        logger.info(f"📧 Message email sent successfully to {to_emails}")
        return True
    except Exception as e:
        logger.error(f"🔴 Error sending message email to {to_emails}: {e}", exc_info=True)
        raise