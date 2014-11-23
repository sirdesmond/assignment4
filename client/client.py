#!/usr/bin/env python
import socket
import time
from optparse import OptionParser
import logging
import asyncore



#parser for input arguments 
def parse():
	parser = OptionParser()

	parser.add_option("-s","--scheduler",type="string",dest="scheduler",default="none",help="")

	parser.add_option("-w","--workloadfile",type="string",dest="wlfile",default="none",help="")

	return (parser.parse_args());



'''
	This class is the http client to talk to the scheduler
	The asyncore module is acting as the notification handler 
	It detects  when there is some data to read or write,connects and closes the socket
'''
class Client(asyncore.dispatcher):
	def __init__(self,options,size=1024):
		
		self.size = size
		self.logger = logging.getLogger("Client")

		if(options.wlfile != "none" and options.scheduler != "none"):
			self.host,self.port = options.scheduler.split(':')
			self.task_ids = []

			with open(options.wlfile,"r") as wlfile:
				for line in wlfile:
					self.task,self.taskid = line.rstrip().split(',') 

					self.task_ids.append(self.taskid)

					self.to_send = self.task
			asyncore.dispatcher.__init__(self)
			self.create_socket(socket.AF_INET,socket.SOCK_STREAM)

			try:
				self.logger.debug('connecting to %s ...', (self.host,self.port))
				self.connect((self.host,int(self.port)))
			except Exception, e:
				raise e
			

		else:
			print "Please specify expected inputs"

	def handle_connect(self):
		self.logger.debug('handle connect called')

	def handle_close(self):
		self.logger.debug('handle close called')
		self.close()

	def writable(self):
		self.logger.debug('is writable? - %s', bool(self.to_send))
		return (bool(self.to_send))

	def readable(self):
		self.logger.debug('is readable? -> True')
		return True

	def handle_write(self):
		sent = self.send(self.to_send[:self.size])
		self.logger.debug('handle write called -> (%d) "%s" ', sent, self.to_send[:sent])
		self.to_send = self.to_send[sent:]

	def handle_read(self):
		data = self.recv(self.size)
		self.logger.debug('handle read called -> (%d) "%s" ',len(data), data)
		if data in self.task_ids:
			self.task_ids.remove(data)







if __name__ == '__main__':

	logging.basicConfig(filename='debug.log',level=logging.DEBUG)
	options,args = parse()

	client = Client(options)

	asyncore.loop()
