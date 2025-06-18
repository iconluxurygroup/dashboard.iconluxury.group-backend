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

async def send_file_details_email(to_emails, subject, file_details_content, logger=None):
    """Send an email with file details using Gmail SMTP.

    Args:
        to_emails (str or list): The recipient email address(es). Can be a single string or a list of strings.
        subject (str): The subject line of the email.
        file_details_content (str): The main content of the email, containing the file details.
        logger (logging.Logger, optional): A custom logger instance. Defaults to the module's default logger.

    Returns:
        bool: True if the email was sent successfully, False otherwise.

    Raises:
        Exception: If an unexpected error occurs during the email sending process.
    """
    logger = logger or default_logger
    try:
        # Normalize to_emails into a list of valid email addresses
        if isinstance(to_emails, str):
            email_list = [to_emails]
        else:
            email_list = []
            for item in to_emails:
                if isinstance(item, list):
                    email_list.extend(item)
                else:
                    email_list.append(item)
        
        valid_emails = [email for email in email_list if isinstance(email, str) and '@' in email]
        if not valid_emails:
            logger.error("No valid email addresses provided for file details email.")
            return False

        # Create the MIME message
        msg = MIMEMultipart()
        msg['From'] = f'{SENDER_NAME} <{SENDER_EMAIL}>'
        msg['To'] = ', '.join(valid_emails)
        msg['Subject'] = subject # Set the email subject
        
        # Determine CC recipient
        cc_recipient = 'nik@iconluxurygroup.com' if 'nik@luxurymarket.com' not in valid_emails else 'nik@luxurymarket.com'
        msg['Cc'] = cc_recipient

        # Prepare HTML content for the email body
        file_details_with_breaks = file_details_content.replace("\n", "<br>")
        html_content = f"""
        <html>
        <body>
        <div class="container">
            <p>File details:<br>{file_details_with_breaks}</p>
            <p>--</p>
            <p><small>This is an automated notification regarding file details.<br>
            Version: <a href="https://dashboard.iconluxury.group">{VERSION}</a>
            <br>
            User: {', '.join(valid_emails)}</small></p>
        </div>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_content, 'html'))

        # Connect to SMTP server and send the email
        smtp_client = aiosmtplib.SMTP(
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            use_tls=False, # We'll start TLS explicitly
            start_tls=True # Initiate STARTTLS
        )
        await smtp_client.connect()
        await smtp_client.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        # Combine To and Cc recipients for sending
        recipients = valid_emails + [cc_recipient]
        await smtp_client.send_message(msg, sender=SENDER_EMAIL, recipients=recipients)
        await smtp_client.quit()

        logger.info(f"📧 File details email sent successfully to {', '.join(valid_emails)} with subject: '{subject}'.")
        return True
    except Exception as e:
        logger.error(f"🔴 Error sending file details email to {to_emails} with subject '{subject}': {e}", exc_info=True)
        raise