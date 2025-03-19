# email_utils.py
import os
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Personalization, Cc, To
from config import SENDGRID_API_KEY

# Module-level logger
default_logger = logging.getLogger(__name__)
if not default_logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def send_email(to_emails, subject, download_url, job_id, logger=None):
    """Send an email notification with the download URL."""
    logger = logger or default_logger
    try:
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Your file is ready to <a href="{download_url}" class="download-button">download</a></p>

            <p>Link to search <a href="https://cms.rtsplusdev.com/webadmin/ImageScraperForm.asp?Action=Edit&ID={str(job_id)}">results</a></p>
            
            <p>--</p>
            <p><small>This is an automated notification.<br>
            Notified Users: {to_emails}</small> From: <a href="https://cms.rtsplusdev.com/webadmin/ImageScraper.asp">ImageDistro: v19.3</a></p> 
        </div>
        </body>
        </html>
        """
        message = Mail(from_email='nik@iconluxurygroup.com', subject=subject, html_content=html_content)
        cc_recipient = 'nik@iconluxurygroup.com' if to_emails != 'nik@iconluxurygroup.com' else 'nik@luxurymarket.com'
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message.add_personalization(personalization)
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info(f"📧 Email sent successfully to {to_emails} with status code: {response.status_code}")
    except Exception as e:
        logger.error(f"🔴 Error sending email to {to_emails}: {e}", exc_info=True)
        raise

def send_message_email(to_emails, subject, message, logger=None):
    """Send a plain message email (e.g., for errors)."""
    logger = logger or default_logger
    try:
        message_with_breaks = message.replace("\n", "<br>")
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Message details:<br>{message_with_breaks}</p>
            <p>--</p>
            <p><small>This is an automated notification.<br>
            From: <a href="https://cms.rtsplusdev.com/webadmin/ImageScraper.asp">ImageDistro: v19.3</a> Notified Users: {to_emails}</small></p>
        </div>
        </body>
        </html>
        """
        message_obj = Mail(from_email='nik@iconluxurygroup.com', subject=subject, html_content=html_content)
        cc_recipient = 'nik@iconluxurygroup.com' if to_emails != 'nik@iconluxurygroup.com' else 'nik@luxurymarket.com'
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message_obj.add_personalization(personalization)
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message_obj)
        logger.info(f"📧 Message email sent successfully to {to_emails} with status code: {response.status_code}")
    except Exception as e:
        logger.error(f"🔴 Error sending message email to {to_emails}: {e}", exc_info=True)
        raise