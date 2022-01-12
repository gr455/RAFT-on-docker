import requests
import os
import json
from util.customJSON import LogEncoder
from util.logger import Logger

## CONSTANTS
STATE_LEADER = 0
STATE_FOLLOWER = 1
STATE_CANDIDATE = 2

ELECTION_TIMEOUT = 200 # milliseconds

# Take the total number of servers (including failed servers) in the cluster from the env var
# This env var must be set in the docker container
SERVER_COUNT = os.environ["RAFT_ENV_NSERVER"]
LOGFILE_LOCATION = os.environ["RAFT_ENV_LOGFILE_LOCATION"]

keyValueStorage = {}

peerServerHosts = []


# SUPPORTS THE FOLLOWING OPERATIONS
#
# PUT k v: keyValueStorage[k] = v
# GET k _: return keyValueStorage[k]
# REQUESTEL: Not logged, alive servers must respond with { ok: True, granted: True / False }
# HEARTBEAT: Not logged, alive servers must respond with { ok: True }. Only leader may initiate
# NEWLEADER: Not logged, tells alive servers the id of new leader. Only leader may initiate

class Raft:
	def __init__(self, sid, log = [], state = STATE_FOLLOWER, term = 0, commitIndex = -1, appliedIndex = -1, termVotedFor = None):
		self.sid = sid
		self.term = term
		self.state = state
		self.log = log
		self.commitIndex = commitIndex
		self.appliedIndex = appliedIndex
		self.termVotedFor = termVotedFor
		self.logger = Logger(LOGFILE_LOCATION)

		persistentJSON = self.getPersistentJSON()
		if persistentJSON:
			self.loadPersistent(persistentJSON)
		else:
			self.updatePersistent()

	# Send request to all servers' http servers advertising self as candidate
	def requestElection(self):
		self.state = STATE_CANDIDATE
		status = self.requestVotes()
		if status["nvotes"] >= ceil(SERVER_COUNT / 2): self.winElection()

	# Called if ceil(SERVER_COUNT / 2) votes are achieved by self
	def winElection(self):
		self.state = STATE_LEADER
		self.changeCurrentTerm(self.term + 1)
		self.sendAppendRPC("NEWLEADER")

	# Appends command to own log, if the server is leader, then it also sends out
	# AppendRPCs to rest of the servers in the cluster
	def appendToLog(self, command):
		self.logger.log(f"APPEND ({command}, term: {self.term})")
		logEntry = LogEntry(command, self.term)
		self.log.append(logEntry)
		self.appliedIndex += 1
		self.updatePersistent()

		# If leader, also send out RPCs of this log entry
		if self.state == STATE_LEADER:
			## Check on the follower if the log has 0 elements
			rpc = AppendRPC(self.term, self.sid, self.appliedIndex - 1, self.term, self.commitIndex, command)
			return self.sendAppendRPC(rpc)

		return { "ok": True, "message": "Server is not leader" }

	# Sends AppendRPC to all servers in the cluster with the provided command operation
	def sendAppendRPC(self, command):
		self.logger.log(f"SENDAPPEND ({command}, term: {self.term})")
		nfailed = 0
		for host in peerServerHosts:
			if host.sid == self.sid: continue

			status = self.sendAppendRPCSingle(command, host)
			if not status["ok"]: failed += 1

		return { "ok": failed < floor(SERVER_COUNT / 2), "nfailed": nfailed }

	# Handles a single AppendRPC to a single follower
	# Also does consistency check on the follower logs
	def sendAppendRPCSingle(self, command, host):
		rpc = {}
		rpc["term"] = self.term
		rpc["leaderID"] = self.sid
		rpc["prevLogIndex"] = self.appliedIndex - 1
		# if the log is empty, then previous log term is -1
		if self.appliedIndex == 0:
			rpc["prevLogTerm"] = -1
		else:
			rpc["prevLogTerm"] = self.log[self.appliedIndex - 1].term

		rpc["leaderCommit"] = self.commitIndex

		rpcJSON = json.dumps(rpc)
		response = requests.get(url = f"http://{host}/appendRPC", params = { "args": rpcJSON })

		return { "ok": response.status_code == 200 }

	# Sends rquest for votes to all servers in the cluster
	def requestVotes(self):
		nfailed = 0
		votes = 1
		for host in peerServerHosts:
			if host.sid == self.sid: continue

			status = self.requestSingleVote(host)
			if not status["ok"]: failed += 1
			elif status["gotVote"]: votes += 1

		return { "ok": failed < floor(SERVER_COUNT / 2), "nfailed": nfailed, "nvotes": votes }

	# Sends a single REQUESTEL RPC to a single server
	def requestSingleVote(self, host):
		rpc = {}
		rpc["term"] = self.term
		rpc["candidateId"] = self.sid
		rpc["lastLogIndex"] = self.appliedIndex

		if self.appliedIndex < 0:
			rpc["lastLogTerm"] = -1
		else:
			rpc["lastLogTerm"] = self.log[self.appliedIndex]


		rpcJSON = json.dumps(rpc)
		response = request.get(url = f"http://{host}/requestVotes", params = { "args": rpcJSON })

		return { "ok": response.status_code == 200, "gotVote": response.json()["granted"] }

	# Called by http server controller when it receives an AppendRPC
	# Must return ok: True if consistent with the leader else False
	def recvAppendRPC(self, appendRPC):
		self.logger.log(f"RECVAPPEND ({appendRPC.command}, selfterm: {self.term}, lterm: {appendRPC.term})")
		op = appendRPC.command

		if op == "GET" or op == "PUT":
			if appendRPC.term < self.term: # I am not sure if this can ever happen
				return { ok: False, error: "EINCONSISTENT" } # should be a fatal error

			if appendRPC.prevLogIndex > self.appliedIndex:
				return { ok: False, error: "ENOENT" }

			if self.log[appendRPC.prevLogIndex] != appendRPC.prevLogTerm:
				return { ok: False, error: "ETERMMISMATCH" }

			if self.appliedIndex >= appendRPC.prevLogIndex + 1 and self.log[appendRPC.prevLogIndex + 1] != appendRPC.command:
				self.log = self.log[:appendRPC.prevLogIndex + 1]
				self.appliedIndex = appendRPC.prevLogIndex

			logEntry = LogEntry(op, appendRPC.term)
			self.appendToLog(logEntry)

		else:
			self.executeAsyncOperation(AppendRPC)


	def recvVoteRPC(self, requestVotesRPC):
		return self.executeAsyncOperation(requestVotesRPC)

	# Commits all operations in log till commitIndex
	def commitEntry(self, commitIndex):
		self.logger.log(f"COMMIT ({commitIndex})")
		if self.commitIndex > commitIndex: return
		for entry_i in range(self.commitIndex, commitIndex + 1):
			status = self.executeLogOperation(self.log[entry_i].command)
			if not status["ok"]:
				self.commitIndex = entry_i - 1
				return status

		self.commitIndex = commitIndex

	# Executes log operation on the machine (GET/PUT)
	def executeLogOperation(self, operation):
		self.logger.log(f"EXECUTE ({operation}")
		op, k, v = operation.split()
		if op == "GET":
			result = keyValueStorage[k]
			return { "ok": True, "result": result }

		elif op == "PUT":
			keyValueStorage[k] = v
			return { "ok": True }

		return { "ok": False }

	# Executes operations like HEARTBEAT asynchronously to the log
	# Expected to respond immediately
	def executeAsyncOperation(self, rpc):
		op = rpc.command

		if op == "HEARTBEAT":
			logger.log(f"HEARTBEAT (from {rpc.leaderSID})")
			self.resetElectionTimer()
			return { ok: True }

		elif op == "REQUESTEL":
			logger.log(f"CANDIDATURE (from {rpc.candidateSID}, for {rpc.term})")
			if self.term > rpc.term: return { ok: True, granted: False, term: rpc.term }

			vote = self.decideVote(rpc.candidateLastLogTerm, rpc.candidateLastLogIndex)
			return { ok: True, granted: vote, term: rpc.term }

		elif op == "NEWLEADER":
			logger.log(f"NEWLEADER ({rpc.leaderSID} for term {rpc.term})")
			changeCurrentTerm(rpc.term)
			return { ok: True }

	# As a response to REQUESTEL, a server decides whether to vote the candidate
	def decideVote(self, candidateLastLogTerm, candidateLastLogIndex):
		if self.appliedIndex == -1: return True
		return ( self.term < candidateLastLogTerm ) or ( self.term == candidateLastLogTerm and self.appliedIndex > candidateLastLogIndex )

	# Change the current term of the server. Changes on persistent storage too
	def changeCurrentTerm(self, newTerm):
		self.term = newTerm
		self.updatePersistent()

	# Change self.termVotedFor. Changes on persistent storage too
	def changeLastVoted(self, lastVoted):
		self.termVotedFor = lastVoted
		self.updatePersistent()

	# Rewrites persistent storage with in-memory values of self.term, self.log and self.termVotedFor
	def updatePersistent(self):
		persistentData = {
			"currentTerm": self.term,
			"votedFor": self.termVotedFor,
			"log": self.log
		}
		
		persistentJSON = json.dumps(persistentData, cls = LogEncoder)
		storage = open("../persistent.log", "w")
		storage.write(persistentJSON)
		storage.close()

	# Returns JSON from persistent storage
	def getPersistentJSON(self):
		storage = open("../persistent.log", "r")
		persistentJSON = storage.read()
		storage.close()

		return persistentJSON

	# Loads persistent storage data into memory from JSON
	def loadPersistent(self, persistentJSON):
		try:
			rawData = json.loads(persistentJSON)
			self.term = rawData["currentTerm"]
			self.termVotedFor = rawData["votedFor"]

			rawLog = rawData["log"]
			self.log = []

			for rawEntry in rawLog:
				logEntry = LogEntry(rawEntry["command"], rawEntry["term"])
				self.log.append(logEntry)

		except json.decoder.JSONDecodeError:
			return

class LogEntry:
	def __init__(self, command, term):
		self.command = command
		self.term = term

class AppendRPC:
	def __init__(self, term, leaderSID, prevLogIndex, prevLogTerm, leaderCommitIndex, command):
		self.term = term
		self.leaderSID = leaderSID
		self.prevLogIndex = prevLogIndex
		self.prevLogTerm = prevLogTerm
		self.leaderCommitIndex = leaderCommitIndex
		self.command = command

class RequestVoteRPC:
	def __init__(self, term, candidateSID, candidateLastLogTerm, candidateLastLogIndex):
		self.term = term
		self.candidateSID = candidateSID
		self.candidateLastLogTerm = candidateLastLogTerm
		self.candidateLastLogIndex = candidateLastLogIndex
		self.command = "REQUESTEL"