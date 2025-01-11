# -*- coding:utf-8 -*-
# Description: This script is used to send email using SMTP server.
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename=f"logs/{os.path.basename(__file__).split('.')[0]}.log",
)
logging.info("Email send job started.")


def send_email(subject: str, body: str, sender_email: str = "", to: str = ""):
    msg = MIMEMultipart()

    msg["From"] = sender_email
    msg["To"] = to
    msg["Subject"] = subject
    body = body
    msg.attach(MIMEText(body, "plain"))

    email = smtplib.SMTP("smtp.office365.com", 587)
    email.starttls()
    email.login(sender_email, "")

    message = msg.as_string()
    try:
        email.sendmail(sender_email, to, message)
        logging.info("Mail successfully sent.")
    except:
        logging.exception("Something went wrong while sending email…")
        raise
