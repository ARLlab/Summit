import logging, os, json, time
from pathlib import Path
from datetime import datetime
import datetime as dt

from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from summit_core import configure_logger

rundir = Path(r'C:\Users\arl\Desktop\summit_master\processors\errors')

auth_file = (rundir / 'email_auth.json')
sender = 'arl.lab.cu@gmail.com'

logger = configure_logger(rundir, __name__)

Base = declarative_base()  # needed to subclass for sqlalchemy objects

test_list = ['brbl4762@colorado.edu']
global_list = ['brbl4762@colorado.edu']
processor_email_list = ['brbl4762@colorado.edu']


class Error():

	id = 0

	def __init__(self, reason, resolution_function, resolution_value, email_template=None, expiration=None):

		self.id = Error.id
		Error.id += 1  # create unique IDs from incrementing class variable

		self.email_template = email_template
		self.expiration = expiration
		self.resolution_function = resolution_function
		self.resolution_value = resolution_value
		self.reason = '[' + reason + ']'

		logger.error(f'Error for {self.reason} intiated with error ID: {self.id}')
		self.email_template.send()

	def check_resolution(self):
		if self.resolution_function(self.resolution_value):
			self.resolve()
			return True
		else:
			logger.error(f'Error for {self.reason} with error ID {self.id} was checked and is un-resolved.')
			return False

	def resolve(self):
		self.email_template.send_resolution()


class EmailTemplate():

	def __init__(self, send_from, send_to_list, body, resolution_body='',
				 subject=None, attachments=None, auth_file=auth_file):
		self.send_from = send_from
		self.send_to = send_to_list
		self.subject = subject
		self.body = body
		self.resolution_body = resolution_body
		self.attachments = attachments

	def send(self):
		try:
			auth = json.loads(auth_file.read_text())
			user, passw = (auth.get('gmail_username'), auth.get('gmail_password'))
			send_email(self.send_from, self.send_to, self.subject,
					   self.body, user, passw,
					   attach=self.attachments)
		except Exception as e:
			print(e.args)
			print('Handle the new exception!')

	def send_resolution(self):
		try:
			auth = json.loads(auth_file.read_text())
			user, passw = (auth.get('gmail_username'), auth.get('gmail_password'))
			send_email(self.send_from, self.send_to, f'Resolved - {self.subject}',
					   self.resolution_body, user, passw,
					   attach=self.attachments)
		except Exception as e:
			print(e.args)
			print('Handle the new exception!')


class ProccessorEmail(EmailTemplate):

	def __init__(self, send_from, processor_name, attachments=()):
		subject = f'{processor_name} Error'
		body = f'The {processor_name} failed to run properly at {datetime.now().isoformat(" ")}.'
		send_to_list = processor_email_list

		super().__init__(send_from, send_to_list, body, subject=subject, attachments=attachments)


class NewDataEmail(EmailTemplate):

	def __init__(self, send_from, processor_name, last_data_time, attachments=()):
		subject = f'No New Data for {processor_name}'
		body = f'There has been no new data for the {processor_name} in {datetime.now() - last_data_time}.'
		send_to_list = processor_email_list


		self.last_data_time = last_data_time
		super().__init__(send_from, send_to_list, body, subject=subject, attachments=attachments)

	def send_resolution(self):
		try:
			self.resolution_body = (f'New data for {self.processor_name}' +
									f'was found after {datetime.now() - self.last_data_time}.')
			auth = json.loads(auth_file.read_text())
			user, passw = (auth.get('gmail_username'), auth.get('gmail_password'))
			send_email(self.send_from, self.send_to, f'Resolved - {self.subject}',
					   self.resolution_body, user, passw,
					   attach=self.attachments)
		except Exception as e:
			print(e.args)
			print('Handle the new exception!')


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


def send_basic_w_attachement(directory):
	import json
	userdata = json.loads(auth_file.read_text())

	file_to_attach = directory / 'processor_logs/summit_errors.log'

	send_email(sender, test_list, 'Error Test!',
			   'This has been a scheduled test of the Summit Error Handling System.\n The entire log is attached.',
			   userdata.get('gmail_username'), userdata.get('gmail_password'),
			   attach=[file_to_attach])


def main():


	def basic_template_test():
		err_email = EmailTemplate(sender, test_list, 'This is a class-based test.', subject='ClassTest')
		err_email.send()
		err_email.send_resolution()


	def processor_template_test():
		proc_email = ProccessorEmail(sender, 'Picarro Processor')
		proc_email.send()
		proc_email.send_resolution()


	def error_and_resolution_test():

		def is_it_time_test(the_time):
			if datetime.now() > the_time:
				logger.error("It's finally time!")
				return True
			else:
				return False

		template = ProccessorEmail('arl.lab.cu@gmail.com', 'Time Processor')
		the_time = datetime.now() + dt.timedelta(seconds=30)
		test_error = Error("it's not time yet", is_it_time_test, the_time, email_template=template)

		for i in range(8):
			if test_error.check_resolution():
				del test_error
				break

			time.sleep(5)


	# send_basic_w_attachement(rundir)  # works
	# basic_template_test()  # works
	# processor_template_test()  # works
	# error_and_resolution_test()  # works

	pass


if __name__ == '__main__':
	main()