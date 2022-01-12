import requests
import os
import json
import random
from util.customJSON import LogEncoder
from util.logger import Logger
from util.timers import ElectionTimer, HeartbeatTimer

## CONSTANTS
STATE_LEADER = 0
STATE_FOLLOWER = 1
STATE_CANDIDATE = 2

ELECTION_TIMEOUT = random.randint(200000, 500000) # milliseconds
HEARTBEAT_TIME = 1000 # milliseconds

INSTANT = 0.0001

# Take the total number of servers (including failed servers) in the cluster from the env var
# This env var must be set in the docker container
SERVER_COUNT = int(os.environ["RAFT_ENV_NSERVER"])
LOGFILE_LOCATION = os.environ["RAFT_ENV_LOGFILE_LOCATION"]

keyValueStorage = {}

peerServerHosts = [f"server{i}" for i in range(1, SERVER_COUNT + 1)]


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
		self.currentTermVotes = 0

		self.electionTimer = ElectionTimer(ELECTION_TIMEOUT, self.electionTimeout, [])
		self.electionTimer.tick()
		self.heartbeatTimer = HeartbeatTimer(HEARTBEAT_TIME, self.heartbeatTimeout, [])

		persistentJSON = self.getPersistentJSON()
		if persistentJSON:
			self.loadPersistent(persistentJSON)
		else:
			self.updatePersistent()

		self.logger.log("STARTUP")

	def host(self):
		return f"server{self.sid}"

	# Send request to all servers' http servers advertising self as candidate
	def requestElection(self):
		self.logger.log("ELECTIONREQUEST")
		self.state = STATE_CANDIDATE
		self.electionTimer.reset()
		self.currentTermVotes = 1

		status = self.requestVotes()

		return status
	# Called if ceil(SERVER_COUNT / 2) votes are achieved by self
	def winElection(self):
		self.state = STATE_LEADER
		self.changeCurrentTerm(self.term + 1)
		self.sendAppendRPC("NEWLEADER")
		self.logger.log(f"WONELECTION (term {self.term})")

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
			if host == self.host(): continue

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
		rpc["command"] = self.command

		rpcJSON = json.dumps(rpc)
		response = requests.get(url = f"http://{host}/appendRPC", params = { "args": rpcJSON })

		return { "ok": response.status_code == 200 }

	# Sends rquest for votes to all servers in the cluster
	# Election timer is reset when request for votes is sent
	# Votes for current term are set to 1 (own vote)
	def requestVotes(self):
		for host in peerServerHosts:
			if host == self.host(): continue

			status = self.requestSingleVote(host)

		return { "ok": True }

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
		# This request is a fire and forget, we will not be waiting for a response
		try: requests.get(url = f"http://{host}/requestVotes", params = { "args": rpcJSON }, timeout = INSTANT)
		except: pass

		return { "ok": True }

	# Called when candidate receives a positive vote from a fellow server
	def recvVote(self):
		if self.status != STATE_CANDIDATE: return

		self.currentTermVotes += 1

		if self.currentTermVotes >= ceil(SERVER_COUNT / 2): self.winElection()

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


	# Called when a follower receives VoteRPC, not to be confused with function recvVote(self) above
	def recvVoteRPC(self, requestVotesRPC):
		self.logger.log(f"RECVELECTION (from {requestVotesRPC.candidateSID})")
		status = self.executeAsyncOperation(requestVotesRPC)
		candidateId = requestVotesRPC.candidateSID

		# Hardcoded URL for the server's http server
		url = f"http://server{candidateId}/voteCandidate"

		try: requests.get(url = url, args = { "granted": status["granted"] }, timeout = INSTANT)
		except: pass

		return { "ok": True }

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
			self.logger.log(f"HEARTBEAT (from {rpc.leaderSID})")
			self.state = STATE_FOLLOWER
			self.electionTimer.reset()
			return { ok: True }

		elif op == "REQUESTEL":
			self.logger.log(f"CANDIDATURE (from {rpc.candidateSID}, for {rpc.term})")
			if self.term > rpc.term: return { ok: True, granted: False, term: rpc.term }

			vote = self.decideVote(rpc.candidateLastLogTerm, rpc.candidateLastLogIndex)
			return { ok: True, granted: vote, term: rpc.term }

		elif op == "NEWLEADER":
			self.logger.log(f"NEWLEADER ({rpc.leaderSID} for term {rpc.term})")
			changeCurrentTerm(rpc.term)
			self.state = STATE_FOLLOWER
			return { ok: True }

	def electionTimeout(self):
		self.requestElection()

	def heartbeatTimeout(self):
		if self.state != STATE_LEADER: return # this should never arise, but just in case
		self.sendAppendRPC("HEARTBEAT")

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