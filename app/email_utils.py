import os
import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Personalization, Cc, To
from config import SENDGRID_API_KEY

logging.basicConfig(level=logging.INFO)

def send_email(to_emails, subject, download_url, job_id):
    try:
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>Your file is ready for download.</p>
             <a href="{download_url}" class="download-button">Download File</a>
            <p><br>Please use the link below to modify the file<br></p>
            <a href="https://cms.rtsplusdev.com/webadmin/ImageScraperForm.asp?Action=Edit&ID={job_id}" class="download-button">Edit / View</a> 
            <br>  
            <p>--</p>
            <p>CMS:v1.1</p>
        </div>
        </body>
        </html>
        """
        message = Mail(from_email='nik@iconluxurygroup.com', subject=subject, html_content=html_content)
        cc_recipient = 'nik@iconluxurygroup.com' if to_emails != 'nik@iconluxurygroup.com' else 'notifications@popovtech.com'
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message.add_personalization(personalization)
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logging.info(f"Email sent successfully: {response.status_code}")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        raise

def send_message_email(to_emails, subject, message):
    try:
        message_with_breaks = message.replace("\n", "<br>")
        html_content = f"""
        <html>
        <body>
       <div class="container">
            <p>Message details:<br>{message_with_breaks}</p>
            
            <p>--</p>
            <p><small>This is an automated system notification.<br>
            Notified Users: {to_emails} JobId:  From: <a href="https://cms.rtsplusdev.com/webadmin/ImageScraper.asp">ImageDistro: v13.3</a></small></p>   
        
        
        
        
        
        """
        message_obj = Mail(from_email='nik@iconluxurygroup.com', subject=subject, html_content=html_content)
        cc_recipient = 'nik@luxurymarket.com'
        personalization = Personalization()
        personalization.add_cc(Cc(cc_recipient))
        personalization.add_to(To(to_emails))
        message_obj.add_personalization(personalization)
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message_obj)
        logging.info(f"Message email sent successfully: {response.status_code}")
    except Exception as e:
        logging.error(f"Error sending message email: {e}")
        raise