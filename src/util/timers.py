import threading

class ElectionTimer:
	def __init__(self, time, callback, args):
		self.time = time
		self.callback = callback
		self.args = args
		self.timer = threading.Timer(self.time / 1000, self.callback, args = self.args)

	def tick(self):
		self.timer = threading.Timer(self.time / 1000, self.callback, args = self.args)
		self.timer.start()

	def stop(self):
		self.timer.cancel()

	def reset(self):
		self.stop()
		self.tick()

class HeartbeatTimer:
	def __init__(self, time, callback, args):
		self.time = time
		self.callback = callback
		self.args = args
		self.timer = threading.Timer(self.time / 1000, self.callback, args = self.args)

	def tick(self):
		self.timer = threading.Timer(self.time / 1000, self.callback, args = self.args)
		self.timer.start()

	def stop(self):
		self.timer.cancel()

	def reset(self):
		self.stop()
		self.tick()