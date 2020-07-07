import logging
from logging.handlers import QueueHandler
import os, time, atexit
from threading import Thread
from multiprocessing import Process, Event, Queue
import cdsapi


###############################################################################
def downloader(runEvent, dlEvent, requestQueue, logQueue, retry = 3):
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)
	log.addHandler( QueueHandler( logQueue ) )
	c   = cdsapi.Client(debug = True)
	while runEvent.is_set():
		try:
			name, request, target = requestQueue.get( timeout = 0.1 )
		except:
			continue
		else:
			log.debug( '{}, {}, {}'.format(name, request, target) )

		dlEvent.set()
		attempt = 0
		while attempt < retry:
			log.info( 'Download attempt {:3d} of {:3d}: {}'.format(attempt+1, retry, target) )
			try:
				c.retrieve( name, request, target )
			except:
				log.error( 'Download attempt {:3d} of {:3d} FAILED: {}'.format(attempt+1, retry, target) )
				attempt += 1
			else:
				log.info( 'Download attempt {:3d} of {:3d} SUCESS: {}'.format(attempt+1, retry, target) )
				attempt = retry+1

		if (attempt == retry):
			log.warning('Failed to download file: {}'.format(target))
			if os.path.isfile( target ):
				os.remove( target )
		dlEvent.clear()


###############################################################################
class ERA5_Downloader( object ):
	def __init__(self, nThreads = 2):
		self.log       = logging.getLogger(__name__)
		self.runEvent  = Event()
		self.reqQueue  = Queue()
		self.logQueue  = Queue()
		self.logThread = Thread( target = self._mpLogger )
		self.logThread.start()
		
		self.runEvent.set()
		self.procs    = []
		self.dlEvents = []
		for i in range( nThreads ):
			dlEvent = Event()
			args    = (self.runEvent, dlEvent, self.reqQueue, self.logQueue,)
			proc    = Process(target = downloader, args=args)
			proc.start()
			self.procs.append( proc )
			self.dlEvents.append( dlEvent )

		atexit.register( self._quit )

	def retrieve(self, name, request, target = None):
		self.reqQueue.put( (name, request.copy(), target,) )

	def wait(self):
		while not self.reqQueue.empty(): time.sleep(0.1)									# While the queue is NOT empty, sleep for 100 ms
		for dlEvent in self.dlEvents:																# Iterate over the download events
			dl = dlEvent.wait(timeout=0.1);														# Wait for the download event to be set; time out after 100 ms
			if dl:																						# If did NOT timeout
				while dlEvent.is_set(): time.sleep(0.1)										# While the file is downloading, sleep 100 ms

	def _quit(self):
		self.logQueue.put(None)
		self.runEvent.clear()

	def _mpLogger(self):
		while True:
			try:
				record = self.logQueue.get( timeout = 1.0 )
			except:
				pass
			else:
				if record is None: break
				self.log.callHandlers( record )
