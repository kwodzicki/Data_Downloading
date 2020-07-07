#!/usr/bin/env python
#+
# Name:
#   send_email
# Purpose:
#   A python function for sending email
# Inputs:
#   email   : Email address to send mail to
#   subject : Subject line of the email
#   body    : Email body; optional.
# Outputs:
#   Sends an email
# Keywords:
#   None.
# Author and History:
#   Kyle R. Wodzicki     Created 15 May 2017
#-

import subprocess;
def send_email( email, subject, body = None ):
	if body is None:	body = '';
	echo = subprocess.Popen(['echo', body], stdout = subprocess.PIPE);						# Echo body of message
	mail = subprocess.Popen(['mail', '-s', subject, email], stdin = echo.stdout);	# Send the email
	echo.wait(); mail.wait();																											# Wait for process to finish
	return mail.returncode;																												# Return the subprocess return code