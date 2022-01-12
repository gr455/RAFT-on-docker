import time

class Logger:
	def __init__(self, location):
		self.location = location
		self.logFile = open(location, "a")

	def log(self, message):
		logString = f"{time.time()}: {message}"
		print(logString)
		self.logFile.write(logString)

	def erase(self):
		close(open(self.location, "w"))