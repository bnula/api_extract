import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.header import Header
from email import encoders


def send_email(script_name, status, day, folder_path=None):
    subject = f"{script_name} - {status}"
    msg = MIMEMultipart()
    msg_from = "Status_check@company.com"
    msg["From"] = msg_from
    msg["To"] = "user1@company.com"
    msg["CC"] = "user2@company.com"
    msg["Subject"] = Header(subject, "utf-8").encode()
    body = f"Log for {script_name} from {day}"
    msg.attach(MIMEText(body, "html", "utf-8"))
    if not folder_path:
        folder_path = f"D:/Logs/{script_name}/"
    file = f"{day} - {script_name}"
    part = MIMEBase("application", "octet-stream")
    part.set_payload(open(folder_path + file, "rb").read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={file}")
    msg.attach(part)

    email_text = msg.as_string()

    sent_from = msg_from
    sent_to = ["user1@company.com", "user2@company.com"]
    mail_server = "smtp-out.company.com"
    s = smtplib.SMTP(mail_server, 25)
    s.ehlo()
    s.sendmail(sent_from, sent_to, email_text)
    s.close()
