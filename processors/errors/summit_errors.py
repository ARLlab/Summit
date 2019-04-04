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


def send_email(send_from, send_to, subject, body, attachments=None, server='smtp.gmail.com'):
	import smtplib, json
	from os.path import basename
	from email.mime.application import MIMEApplication
	from email.mime.multipart import MIMEMultipart
	from email.mime.text import MIMEText
	from email.utils import COMMASPACE, formatdate

	with (rundir / 'email_auth.txt').read_text() as filedata:
		userdata = json.loads(filedata)

	server = smtplib.SMTP_SSL(server, 465)
	server.ehlo()
	server.login(userdata.get('gmail_username'), userdata.get('gmail_password'))

	pass