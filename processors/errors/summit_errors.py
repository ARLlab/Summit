"""
The Error module has several purposes ranging from tracking undesirable states that should trigger emails, to framing
different types of warning and error emails that can be used as one-offs as exceptions are caught or need to be warned.

The highest-complexity purpose of the module is to track undesirable states at runtime (like a lack of new data), and
trigger emails if these states are active or the get resolved. This is done with Error objects.

Errors are not database-persisted, and exist only during the runtime of the error module or submodules like
check_for_new_data(). In short, an Error is triggered when some state isn't acceptable, ie there is not new data
from a particular processor. When this occurs, an Error is given a string-reason, ie 'No New Data', a resolution
function, and an email template.

The email template is used to draft and send the email including the reason for the failure.
    Only one email is sent when the error is activated. The Error then stays alive, until it's resolution function
    is run and returns True for being resolved.

The resolution function is (in most cases) a simple function that returns True if the error should be resolved, and
    false if not. For instance, new_data_found() resolves if data greater than the date the Error was initiated with is
    found in the database.

Because resolution functions exist in the Error class once assigned, they need to be passed any necessary arguments
    from the function, like new_data_found() requires the name of the processor, and the last date data was found.
    If it checks the database and returns True for there being newer data, it triggers the error to resolve, which
    will then send the resolution email.
"""

import json
import time
from datetime import datetime
import datetime as dt

from sqlalchemy.ext.declarative import declarative_base

from summit_core import configure_logger
from summit_core import error_dir as rundir

auth_file = (rundir / 'email_auth.json')
sender = 'arl.lab.cu@gmail.com'

logger = configure_logger(rundir, __name__)

Base = declarative_base()  # needed to subclass for sqlalchemy objects

test_list = ['brbl4762@colorado.edu']
global_list = ['brbl4762@colorado.edu']
processor_email_list = ['brbl4762@colorado.edu', 'jach4134@colorado.edu']
instrument_email_list = ['brbl4762@colorado.edu']


class Error():
    """
    Errors are used to track peristent problems that arise, like a lack of new data. They are initiated, which sends
    the original error email, then their resolution function is checked periodically, and once true will send a
    resolution email, stating the error was resolved.
    """
    id = 0

    def __init__(self, reason, resolution_function, email_template, expiration=None):

        self.id = Error.id
        Error.id += 1  # create unique IDs from incrementing class variable

        self.email_template = email_template
        self.expiration = expiration
        self.resolution_function = resolution_function
        self.reason = reason

        logger.error(f'Error for {self.reason} intiated with error ID: {self.id}')
        self.email_template.send()

    def is_resolved(self, **kwargs):
        if self.resolution_function(**kwargs):
            self.resolve()
            logger.error(f'Error for {self.reason} with error ID {self.id} was checked and resolved.')
            return True
        else:
            logger.error(f'Error for {self.reason} with error ID {self.id} was checked and is un-resolved.')
            return False

    def resolve(self):
        self.email_template.send_resolution()


class EmailTemplate():
    """
    EmailTemplates allow for an email to be drafted but not sent. They can be used as one-offs to draft and send an
    email once, or they can be given an initial and resoltion message, which will be send by an Error depending on the
    Error's state.
    """

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
    """
    A subclass of EmailTempltes, ProcessorEmails require only the sender and processor name to be sent.
    The subject and body are composed with the processor name, and additional information given in an exception or
    traceback.
    """

    def __init__(self, send_from, processor_name, exception=None, trace='', attachments=()):

        subject = f'{processor_name} Error'

        if exception and trace:
            body = (f'The {processor_name} failed to run properly at {datetime.now().isoformat(" ")}.\n'
                    + f'Exception {exception.args} caused the failure. The full traceback is:\n{trace}')
        elif exception:
            body = (f'The {processor_name} failed to run properly at {datetime.now().isoformat(" ")}.\n'
                    + f'Exception {exception.args} caused the failure.')
        elif trace:
            body = (f'The {processor_name} failed to run properly at {datetime.now().isoformat(" ")}.\n'
                    + f'The full traceback is:\n{trace}')
        else:
            body = f'The {processor_name} failed to run properly at {datetime.now().isoformat(" ")}.\n'

        send_to_list = processor_email_list

        super().__init__(send_from, send_to_list, body, subject=subject, attachments=attachments)


class NewDataEmail(EmailTemplate):
    """
    NewDataEmails subclass EmailTemplates and are specifically for being passed to Errors that are triggered when no new
    data is found.
    """

    def __init__(self, send_from, processor_name, last_data_time, attachments=()):
        subject = f'No New Data for Summit {processor_name[0].upper() + processor_name[1:]}'
        body = (f'There has been no new data for the {processor_name} processor in {datetime.now() - last_data_time}.\n'
                + 'This is the ONLY email that will be recieved for this error untill it is resolved.')

        send_to_list = processor_email_list
        self.send_from = send_from
        self.processor = processor_name
        self.last_data_time = last_data_time
        super().__init__(send_from, send_to_list, body, subject=subject, attachments=attachments)

    def send_resolution(self):
        try:
            self.resolution_body = (f'New data for {self.processor} ' +
                                    f'was found after {datetime.now() - self.last_data_time}.')
            auth = json.loads(auth_file.read_text())
            user, passw = (auth.get('gmail_username'), auth.get('gmail_password'))
            send_email(self.send_from, self.send_to, f'Resolved - {self.subject}',
                       self.resolution_body, user, passw,
                       attach=self.attachments)
        except Exception as e:
            print(e.args)
            print('Handle the new exception!')


class LogParameterEmail(EmailTemplate):
    """
    LogParameterEmails are a convenient subclass designed to be sent by the voc processor when log values exceed their
    given limits.
    """

    def __init__(self, log, parameters):
        from summit_voc import log_parameter_bounds
        subject = f'LogParameter Error in {log.filename}'
        body = f'The following parameters were outside their limits:\n'

        for parameter in parameters:
            limits = log_parameter_bounds.get(parameter)
            log_value = getattr(log, parameter)
            body = body + f'{parameter}:{log_value} was outside limits of {limits}\n'

        super().__init__(sender, instrument_email_list, body, subject=subject)


def send_email(send_from, send_to, subject, body, user, passw, attach=None, server='smtp.gmail.com'):
    import smtplib
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


def send_processor_email(name, exception=None):
    """
    Wrapper on processor emails to create a one-off and send it.
    :param exception: Exception, the exception that occurred to trigger the email
    :return:
    """
    import traceback

    template = ProccessorEmail(sender, name, exception=exception, trace=traceback.format_exc())
    template.send()


def send_processor_warning(name, type, body):
    """

    :param name: string, the processor name
    :param type: string, type of error to include in subject
    :param body: string, the body of the email; usually contains instructions for checking/resolving the warning
    :return:
    """
    EmailTemplate(sender, processor_email_list, body,
                  subject=f'{name} {type} Warning').send()


def send_logparam_email(logfile, invalid_parameters):
    """
    Wrapper on logparam emails to take a list of parameters and send a one-off email.
    :param filename: filename of the log 
    :param invalid_parameters: list, of string parameters that failed their checks
    :return:
    """
    template = LogParameterEmail(logfile, invalid_parameters)
    template.send()


def send_basic_w_attachement(directory):
    """
    Testing...
    :param directory:
    :return:
    """
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
        another_time = datetime.now() + dt.timedelta(seconds=30)
        test_error = Error("it's not time yet", is_it_time_test, email_template=template)

        for i in range(8):
            if test_error.is_resolved(another_time):
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
