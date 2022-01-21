###
# This file contains code for communication between servers through http
# All the communication that happens between the leader and followers (election, log appends etc.)
# happen through http controllers in this file
###

from flask import Flask, jsonify, request
import json
import os
from raft import Raft, AppendRPC, RequestVoteRPC

app = Flask(__name__)

# global raft

# REQUESTEL
@app.route("/requestVotes")
def requestVotes():
	argsJSON = request.args.get("args")
	args = json.loads(argsJSON)

	requestRPC = RequestVoteRPC(args["term"], args["candidateId"], args["lastLogTerm"], args["lastLogIndex"])

	status = raft.recvVoteRPC(requestRPC)

	if status["ok"]: return status, 200
	return status, 500

# Fellow server requests this endpoint to vote (positive or negative) current server
@app.route("/voteCandidate")
def voteCandidate():
	argsJSON = request.args.get("args")
	args = json.loads(argsJSON)

	if args["granted"]: raft.recvVote()

	return { "ok": True }, 200

# GET, PUT, HEARTBEAT, NEWLEADER
@app.route("/appendRPC")
def appendRPC():
	argsJSON = request.args.get("args")
	args = json.loads(argsJSON)

	appendRPC = AppendRPC(args["term"], args["leaderID"], args["prevLogIndex"], args["prevLogTerm"], args["leaderCommit"], args["command"])

	status = raft.recvAppendRPC(appendRPC)

	if status["ok"]: return status, 200
	return status, 500

# debug only
@app.route("/ping")
def ping():
	return { "ping": "pong" }, 200

# Initialize server and raft layer
if __name__ == '__main__':
	# RAFT_ENV_SERVERID must be set on the container
	serverID = int(os.environ["RAFT_ENV_SERVERID"])
	raft = Raft(serverID)
	# Runs on port 80 inside the container
	app.run(host = "0.0.0.0", port = 80, debug = False)