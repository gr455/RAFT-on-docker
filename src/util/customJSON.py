import json

class LogEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, LogEntry):
			return { "command": obj.command, "term": obj.term }
		return json.JSONEncoder.default(self, obj)
