import time

class Logger:
	def __init__(self, location):
		self.location = location

	def log(self, message):
		logString = f"{time.time()}: {message}\n"
		
		# append to logfile
		logfile = open(self.location, "a")
		logfile.write(logString)
		logfile.close()

	def erase(self):
		close(open(self.location, "w"))