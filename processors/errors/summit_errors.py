import logging, os
from pathlib import Path
from datetime import datetime
import datetime as dt

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\errors')

Base = declarative_base()  # needed to subclass for sqlalchemy objects

global_list = ['brbl4762@colorado.edu']


def configure_logger(rundir):
	"""
	Create the project-specific logger. DEBUG and up is saved to the log, INFO and up appears in the console.

	:param rundir: path to create log sub-path in
	:return: logger object
	"""

	logfile = Path(rundir) / 'processor_logs/summit_errors.log'
	logger = logging.getLogger('summit_errors')
	logger.setLevel(logging.DEBUG)
	fh = logging.FileHandler(logfile)
	fh.setLevel(logging.DEBUG)

	ch = logging.StreamHandler()
	ch.setLevel(logging.INFO)

	formatter = logging.Formatter('%(asctime)s -%(levelname)s- %(message)s')

	[H.setFormatter(formatter) for H in [ch, fh]]
	[logger.addHandler(H) for H in [ch, fh]]

	return logger


logger = configure_logger(rundir)


class Error(Base):
	"""
	Errors are activatable states that trigger logging, and usually emails.
	Once activated, they send emails/log events and if/when resolved, will send a resolution email.

	For instance, not having new data for 6 hours from the station might activate an error, send an email, and stay
	active until new data is found. Once new data comes in, it will send an email indicating the error is resolved.
	"""
	__tablename__ = 'errors'

	id = Column(Integer, primary_key=True)

	def __init__(self, logger, reason, email_list=None, expiration=None):
		pass

	def resolve(self):
		pass


def send_email(send_from, send_to, subject, body, user, passw, attach=None, server='smtp.gmail.com'):
	import smtplib, json
	from os.path import basename
	from email.mime.application import MIMEApplication
	from email.mime.multipart import MIMEMultipart
	from email.mime.text import MIMEText
	from email.utils import COMMASPACE, formatdate

	msg = MIMEMultipart()
	msg['From'] = send_from
	msg['To'] = COMMASPACE.join(send_to)
	msg['Date'] = formatdate(localtime=True)
	msg['Subject'] = subject

	msg.attach(MIMEText(body))

	for f in attach or []:
		with open(f, 'rb') as file:
			part = MIMEApplication(file.read(), Name=basename(f))

		part['Content-Disposition'] = f'attachment; filename={basename(f)}'
		msg.attach(part)

	smtp = smtplib.SMTP_SSL(server, 465)

	try:
		smtp.ehlo_or_helo_if_needed()
		smtp.login(user, passw)
		smtp.sendmail(send_from, send_to, msg.as_string())
		smtp.close()
	except (smtplib.SMTPHeloError, smtplib.SMTPAuthenticationError, smtplib.SMTPException):
		logger.critical('An email failed to send due to failed server connection.')


def main(directory):
	import json
	import os
	filedata = (rundir / 'email_auth.txt').read_text()
	userdata = json.loads(filedata)

	file_to_attach = directory / 'processor_logs/summit_errors.log'

	send_email('arl.lab.cu@gmail.com', global_list, 'Error Test!',
			   'This has been a scheduled test of the Summit Error Handling System.\n The entire log is attached.',
			   userdata.get('gmail_username'), userdata.get('gmail_password'),
			   attach=[file_to_attach])


if __name__ == '__main__':
	main(rundir)