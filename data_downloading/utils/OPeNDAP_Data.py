import logging
import os, traceback
from threading     import Thread
from pydap.client  import open_url;
from pydap.cas.urs import setup_session;

try:
	import Research.EarthData_login as EDL;
	_user   = EDL.username;
	_passwd = EDL.password;
except:
	_user   = None;
	_passwd = None;
	
class ThreadWithReturn(Thread):
	'''
	Name:
	   ThreadWithReturn
	Purpose:
	   A python class that extends the threading.Thread class
	   and returns the value of the target function when the
	   .join() method is called.
	Inputs:
	   Same as the threading.Thread class.
	Outputs:
	   Returns the result of the target function when
	   .join() is called on instance.
	Keywords:
	   Same as the threading.Thread class.
	Author and History:
	   GuySoft     31 Oct. 2016 (from stackoverflow.com)
	       Updated by Kyle R. Wodzicki     04 Apr. 2018
	          Added *args and **kwargs to calls
	'''
	def __init__(self, *args, **kwargs):
		Thread.__init__(self, *args, **kwargs);                        # Initialize the Thread super class
		self._return = None;                                           # Initialize a _return attribute to None
	def run(self):                                                   # Redefine the .run() method
		if self._target is not None:                                   # If a target is set
			self._return = self._target(*self._args, **self._kwargs);    # Run the target function and place result in _return attribute
	def join(self, *args, **kwargs):                                 # Redefine the join method
		Thread.join(self, *args, **kwargs);                            # Join the Thread 
		return self._return;                                           # Return the returned value

class OPeNDAP_Data(object):
	log = logging.getLogger(__name__);
	def __init__(self, user = _user, passwd = _passwd):
		self.user   = user;
		self.passwd = passwd;
		self.sess   = None;
		self.data   = None;
	################################################
	def download(self, url, vars, maxAttempt=3, connect=True, disconnect=True):
		'''
		Name:
		   download
		Purpose:
		   Function to download data and data attributes from OPeNDAP URL.
		Inputs:
		   url   : URL to the data file.
		   vars  : List of variable names to download.
		Ouputs:
		   Returns a dictionary of dictionaries, where top-level tags
		   are the same as those input to 'vars' and sub-tags are
		   data attributes. Variable data is located under the
		   'values' sub-tag.
		Keywords:
		   maxAttempt : Number of times to try to get attributes and data.
		                 Default is three (3)
		   connect    : Enables/disables connect to the OPeNDAP server.
		                 Default is True. 
		   disconnect : Enables/disables disconnection from OPeNDAP server.
		                 Default is True.
		'''
		if type(vars) is not list: vars = [vars];                                   # Convert vars to list
		if connect:                                                                 # If connect is True
			if not self._openData(url): return False;                                 # Return False if login to OPeNDAP fails
		out = {};                                                                   # Initialize a dictionary for output
		for var in vars:                                                            # Iterate over all variables
			self.log.debug('Attempting to download: {}'.format(var));                 # Debugging information
			attempt = 0;                                                              # Set attempt to zero (0)
			while attempt < maxAttempt:                                               # While the attempt is less than the maximum attempts
				self.log.debug(  
				  'Getting attributes attempt {} of {}'.format(attempt+1,maxAttempt)  
				);                                                                      # Debugging information
				if var in out:                                                          # If the variable name is in output dictionary
					attempt = maxAttempt;                                                 # Set attempt to maximum attempts plus one (1)
				else:                                                                   # Else
					try:                                                                  # Try to...
						out[var] = {};                                                      # Initialize tag in output dictionary as dictionary
						for tag in self.data[var].attributes:                               # Iterate over all attributes
							out[var][tag] = self.data[var].attributes[tag];                   # Download each attribute into the output dictionary
					except:                                                               # On exception
						self.log.debug('Download attempt failed!');                         # Debugging information
						if var in out: del out[var];                                        # Delete the tag from the output directory
						attempt += 1;                                                       # Increment attempt
					else:                                                                 # If try is successful
						self.log.debug('Download attempt SUCCESS!');                        # Debugging information
						break;                                                              # Break out of the while loop
			if var not in out:                                                        # If var tag is NOT in out
				self.log.warning( 'Failed to download: {}'.format(var) );               # Log a warning
				continue;                                                               # Continue to next variable
			attempt = 0;                                                              # Reinitialize attempt to zero (0)
			while attempt < maxAttempt:                                               # While the attempt is less than the maximum attempts
				self.log.debug(  
				  'Getting data attempt {} of {}'.format(attempt+1,maxAttempt)  
				);                                                                      # Debugging information
				try:                                                                    # Try to...
					out[var]['values'] = self.data[var].data[:];                          # Download all the data from the variable
				except:                                                                 # On exception
					self.log.debug('Download attempt failed!');                           # Debugging information
					if 'values' in out[var]: del out[var]['values'];                      # Delete the 'values' from the output directory
					attempt += 1;                                                         # Increment attempt
				else:                                                                   # If try is successful
					self.log.debug('Download attempt SUCCESS!');                          # Debugging information
					break;                                                                # Break out of the while loop
			if 'values' not in out[var]:                                              # If 'values' tag is not in the dictionary for the variable
				self.log.warning( 'Failed to download: {}'.format(var) );               # Log a warning
				if var in out: del out[var];                                            # Delete the dictionary fro the variable
		if disconnect: self._closeData();                                           # If disconnect is True, close remote data
		if all( [tag in vars for tag in out] ):                                     # If all the variables were downloaded
			out['all_vars'] = True;                                             # Add 'all_vars' tag to out data and set value to True
		else:                                                                       # Else, some of the variables were NOT downloaded
			out['all_vars'] = False;                                            # Set the 'all_vars' tag to False
		return out;                                                                 # Return the data dictionary
		self.log.warning('Failed to open the URL!');                                # Log a warning
		return False;                                                               # Return false
	################################################
	def _openData(self, url, maxAttempt = 3):
		'''A function to initialize OPeNDAP sessions and open remote files.'''
		if self.sess is not None: self._closeData();                                 # If the session is open, close it
		self.log.debug( 'Setting up sessions and opening remote data...' );
		attempt = 0
		while attempt < maxAttempt:
			try:
				thread = ThreadWithReturn(
				  target = setup_session, 
				  args   = (self.user, self.passwd, ),
				  kwargs = {'check_url' : url},
				  daemon = True
				);                                                                      # Initialize a thread to run the setup_session function on
				thread.start();
				self.sess = thread.join(30);                                            # Wait 30 seconds for thread to finish
				if thread.is_alive():                                                   # If the thread is still alive
					raise Excpetion('pydap setup_session failed!');
				self.data = open_url(url, session=self.sess);                           # Open the data

# 				self.sess = setup_session(self.user, self.passwd, check_url = url);     # Open a session
# 				self.data = open_url(url, session=self.sess);                           # Open the data
			except Exception as e:
				print( 'Caught excpetion in worker thread. Process: {}'.format(os.getpid()) );
				traceback.print_exc();
				raise e;
				attempt += 1;
				self._closeData();
			else:
				return True;
		self.log.debug( 'Failed to open the URL...' );
		return False;
	################################################
	def _closeData(self):
		'''A function to close OPeNDAP sessions.'''
		self.log.debug( 'Closing remote files' );
		if self.sess is not None:                                                   # If the session for the 1 hourly data is NOT None
			try:
				self.sess.close();                                                      # Close the session
			except:
				self.log.warning('Failed to close session!');
			self.sess = None;                                                         # Set the attribute to None
		self.data = None;                                                           # Set data attribute to None
