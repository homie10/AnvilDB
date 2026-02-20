---- MODULE kv_raft ----
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************
 Phase 7 model expansion:
 - log replication via delayed AppendEntries-style delivery queue
 - partition/heal toggles and packet drop behavior
 - follower divergence under partition and overwrite-on-match repair
 - crash/restart trimming uncommitted suffixes
 - election vote grant includes log up-to-date check
***************************************************************************)

CONSTANTS Nodes, Keys, Values

NoVote == "no_vote"
Roles == {"leader", "follower", "candidate"}
Quorum == (Cardinality(Nodes) \div 2) + 1

CmdType == [op: {"put", "delete"}, key: Keys, val: Values \cup {""}]
EntryType == [idx: Nat, term: Nat, cmd: CmdType]
MessageType ==
  [to: Nodes, prevIdx: Nat, prevTerm: Nat, entries: Seq(EntryType),
   leaderCommit: Nat, deliverAt: Nat]

VARIABLES leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
          electionTimeout, log, commitIndex, kv, prevCommitIndex,
          leaderElectedAtCommit, connected, clock, deliveryQueue

vars ==
  << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
     electionTimeout, log, commitIndex, kv, prevCommitIndex,
     leaderElectedAtCommit, connected, clock, deliveryQueue >>

Min2(a, b) == IF a <= b THEN a ELSE b
Max2(a, b) == IF a >= b THEN a ELSE b

Prefix(seq, upto) ==
  IF upto <= 0
  THEN << >>
  ELSE SubSeq(seq, 1, Min2(upto, Len(seq)))

SuffixFrom(seq, from) ==
  IF from > Len(seq)
  THEN << >>
  ELSE SubSeq(seq, from, Len(seq))

RemoveAt(seq, idx) ==
  SubSeq(seq, 1, idx - 1) \o SubSeq(seq, idx + 1, Len(seq))

ClampCommit(commit, logSeq) ==
  IF commit <= Len(logSeq) THEN commit ELSE Len(logSeq)

CandidateUpToDate(candidate, voter) ==
  LET cLastIdx == Len(log[candidate])
      cLastTerm == IF cLastIdx = 0 THEN 0 ELSE log[candidate][cLastIdx].term
      vLastIdx == Len(log[voter])
      vLastTerm == IF vLastIdx = 0 THEN 0 ELSE log[voter][vLastIdx].term
  IN
    cLastTerm > vLastTerm
    \/ (cLastTerm = vLastTerm /\ cLastIdx >= vLastIdx)

RECURSIVE ApplyEntries(_, _, _)
ApplyEntries(entries, idx, state) ==
  IF idx > Len(entries)
  THEN state
  ELSE
    LET cmd == entries[idx].cmd
    IN
      IF cmd.op = "put"
      THEN ApplyEntries(entries, idx + 1, [state EXCEPT ![cmd.key] = cmd.val])
      ELSE ApplyEntries(entries, idx + 1, [state EXCEPT ![cmd.key] = ""])

ApplyCommitted(logSeq, upto) ==
  ApplyEntries(Prefix(logSeq, upto), 1, [k \in Keys |-> ""])

TypeInv ==
  /\ leader \in Nodes
  /\ role \in [Nodes -> Roles]
  /\ role[leader] = "leader"
  /\ currentTerm \in [Nodes -> Nat]
  /\ votedFor \in [Nodes -> (Nodes \cup {NoVote})]
  /\ votesGranted \in [Nodes -> SUBSET Nodes]
  /\ electionElapsed \in [Nodes -> Nat]
  /\ electionTimeout \in [Nodes -> Nat]
  /\ log \in [Nodes -> Seq(EntryType)]
  /\ commitIndex \in [Nodes -> Nat]
  /\ kv \in [Nodes -> [Keys -> Values \cup {""}]]
  /\ prevCommitIndex \in [Nodes -> Nat]
  /\ leaderElectedAtCommit \in [Nodes -> Nat]
  /\ connected \in [Nodes -> BOOLEAN]
  /\ connected[leader]
  /\ clock \in Nat
  /\ deliveryQueue \in Seq(MessageType)
  /\ \A n \in Nodes : commitIndex[n] <= Len(log[n])
  /\ \A n \in Nodes : \A i \in 1..Len(log[n]) : log[n][i].idx = i

Init ==
  /\ leader \in Nodes
  /\ role = [n \in Nodes |-> IF n = leader THEN "leader" ELSE "follower"]
  /\ currentTerm = [n \in Nodes |-> 1]
  /\ votedFor = [n \in Nodes |-> NoVote]
  /\ votesGranted = [n \in Nodes |-> {}]
  /\ electionElapsed = [n \in Nodes |-> 0]
  /\ electionTimeout = [n \in Nodes |-> 5]
  /\ log = [n \in Nodes |-> << >>]
  /\ commitIndex = [n \in Nodes |-> 0]
  /\ kv = [n \in Nodes |-> [k \in Keys |-> ""]]
  /\ prevCommitIndex = [n \in Nodes |-> 0]
  /\ leaderElectedAtCommit = [n \in Nodes |-> 0]
  /\ connected = [n \in Nodes |-> TRUE]
  /\ clock = 0
  /\ deliveryQueue = << >>

LeaderAppendPut(k, v) ==
  /\ role[leader] = "leader"
  /\ k \in Keys
  /\ v \in Values
  /\ LET nextIdx == Len(log[leader]) + 1
         entry == [idx |-> nextIdx, term |-> currentTerm[leader], cmd |-> [op |-> "put", key |-> k, val |-> v]]
         newLeaderLog == Append(log[leader], entry)
         newLeaderCommit == nextIdx
     IN
       /\ log' = [log EXCEPT ![leader] = newLeaderLog]
       /\ commitIndex' = [commitIndex EXCEPT ![leader] = newLeaderCommit]
       /\ kv' = [kv EXCEPT ![leader] = ApplyCommitted(newLeaderLog, newLeaderCommit)]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, leaderElectedAtCommit, connected, clock, deliveryQueue >>

LeaderAppendDelete(k) ==
  /\ role[leader] = "leader"
  /\ k \in Keys
  /\ LET nextIdx == Len(log[leader]) + 1
         entry == [idx |-> nextIdx, term |-> currentTerm[leader], cmd |-> [op |-> "delete", key |-> k, val |-> ""]]
         newLeaderLog == Append(log[leader], entry)
         newLeaderCommit == nextIdx
     IN
       /\ log' = [log EXCEPT ![leader] = newLeaderLog]
       /\ commitIndex' = [commitIndex EXCEPT ![leader] = newLeaderCommit]
       /\ kv' = [kv EXCEPT ![leader] = ApplyCommitted(newLeaderLog, newLeaderCommit)]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, leaderElectedAtCommit, connected, clock, deliveryQueue >>

Tick(n) ==
  /\ n \in Nodes
  /\ role[n] # "leader"
  /\ electionElapsed[n] < electionTimeout[n]
  /\ electionElapsed' = [electionElapsed EXCEPT ![n] = @ + 1]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionTimeout,
                 log, commitIndex, kv, leaderElectedAtCommit, connected, clock, deliveryQueue >>

AdvanceTime ==
  /\ clock' = clock + 1
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 connected, deliveryQueue >>

StartElection(n) ==
  /\ n \in Nodes
  /\ role[n] # "leader"
  /\ electionElapsed[n] >= electionTimeout[n]
  /\ currentTerm' = [currentTerm EXCEPT ![n] = @ + 1]
  /\ role' = [role EXCEPT ![n] = "candidate"]
  /\ votedFor' = [votedFor EXCEPT ![n] = n]
  /\ votesGranted' = [votesGranted EXCEPT ![n] = {n}]
  /\ electionElapsed' = [electionElapsed EXCEPT ![n] = 0]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 connected, clock, deliveryQueue >>

GrantVote(voter, candidate) ==
  /\ voter \in Nodes
  /\ candidate \in Nodes
  /\ voter # candidate
  /\ connected[voter]
  /\ connected[candidate]
  /\ role[candidate] = "candidate"
  /\ LET candidateTerm == currentTerm[candidate]
     IN
       /\ currentTerm[voter] <= candidateTerm
       /\ CandidateUpToDate(candidate, voter)
       /\ (votedFor[voter] = NoVote \/ votedFor[voter] = candidate)
       /\ currentTerm' = [currentTerm EXCEPT ![voter] = candidateTerm]
       /\ votedFor' = [votedFor EXCEPT ![voter] = candidate]
       /\ votesGranted' = [votesGranted EXCEPT ![candidate] = @ \cup {voter}]
       /\ electionElapsed' = [electionElapsed EXCEPT ![voter] = 0]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 connected, clock, deliveryQueue >>

BecomeLeader(candidate) ==
  /\ candidate \in Nodes
  /\ connected[candidate]
  /\ role[candidate] = "candidate"
  /\ Cardinality(votesGranted[candidate]) >= Quorum
  /\ leader' = candidate
  /\ role' = [n \in Nodes |-> IF n = candidate THEN "leader" ELSE "follower"]
  /\ currentTerm' = [n \in Nodes |-> IF currentTerm[n] < currentTerm[candidate] THEN currentTerm[candidate] ELSE currentTerm[n]]
  /\ votesGranted' = [n \in Nodes |-> {}]
  /\ electionElapsed' = [n \in Nodes |-> 0]
  /\ prevCommitIndex' = commitIndex
  /\ leaderElectedAtCommit' = [leaderElectedAtCommit EXCEPT ![candidate] = commitIndex[candidate]]
  /\ UNCHANGED << votedFor, electionTimeout, log, commitIndex, kv, connected, clock, deliveryQueue >>

Heartbeat ==
  /\ role[leader] = "leader"
  /\ electionElapsed' = [n \in Nodes |-> 0]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionTimeout,
                 log, commitIndex, kv, leaderElectedAtCommit, connected, clock, deliveryQueue >>

Disconnect(n) ==
  /\ n \in Nodes
  /\ n # leader
  /\ connected[n]
  /\ connected' = [connected EXCEPT ![n] = FALSE]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 clock, deliveryQueue >>

Reconnect(n) ==
  /\ n \in Nodes
  /\ n # leader
  /\ ~connected[n]
  /\ connected' = [connected EXCEPT ![n] = TRUE]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 clock, deliveryQueue >>

FollowerDiverges(n, k, v) ==
  /\ n \in Nodes
  /\ n # leader
  /\ ~connected[n]
  /\ k \in Keys
  /\ v \in Values
  /\ LET nextIdx == Len(log[n]) + 1
         entry == [idx |-> nextIdx, term |-> currentTerm[n] + 100, cmd |-> [op |-> "put", key |-> k, val |-> v]]
     IN
       /\ log' = [log EXCEPT ![n] = Append(@, entry)]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, commitIndex, kv, leaderElectedAtCommit,
                 connected, clock, deliveryQueue >>

SendAppend(follower, prevIdx, delay) ==
  /\ follower \in Nodes
  /\ follower # leader
  /\ role[leader] = "leader"
  /\ connected[follower]
  /\ prevIdx \in 0..Len(log[leader])
  /\ delay \in 0..2
  /\ LET prevTerm == IF prevIdx = 0 THEN 0 ELSE log[leader][prevIdx].term
         msg ==
           [to |-> follower,
            prevIdx |-> prevIdx,
            prevTerm |-> prevTerm,
            entries |-> SuffixFrom(log[leader], prevIdx + 1),
            leaderCommit |-> commitIndex[leader],
            deliverAt |-> clock + delay]
     IN
       /\ deliveryQueue' = Append(deliveryQueue, msg)
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 connected, clock >>

DeliverAppend(msgIdx) ==
  /\ msgIdx \in 1..Len(deliveryQueue)
  /\ LET msg == deliveryQueue[msgIdx]
     IN
       /\ msg.deliverAt <= clock
       /\ deliveryQueue' = RemoveAt(deliveryQueue, msgIdx)
       /\ IF ~connected[msg.to]
          THEN
            /\ prevCommitIndex' = commitIndex
            /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                           electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                           connected, clock >>
          ELSE
            LET prevMatches ==
                  msg.prevIdx = 0
                  \/ (msg.prevIdx <= Len(log[msg.to])
                      /\ log[msg.to][msg.prevIdx].term = msg.prevTerm)
                proposedFollowerLog ==
                  IF prevMatches
                  THEN IF Len(msg.entries) = 0
                       THEN log[msg.to]
                       ELSE Prefix(log[msg.to], msg.prevIdx) \o msg.entries
                  ELSE log[msg.to]
                newFollowerLog ==
                  IF Len(proposedFollowerLog) < commitIndex[msg.to]
                  THEN log[msg.to]
                  ELSE proposedFollowerLog
                msgCommit == ClampCommit(msg.leaderCommit, newFollowerLog)
                newFollowerCommit ==
                  IF prevMatches
                  THEN Max2(commitIndex[msg.to], msgCommit)
                  ELSE commitIndex[msg.to]
                newFollowerKv ==
                  IF prevMatches
                  THEN ApplyCommitted(newFollowerLog, newFollowerCommit)
                  ELSE kv[msg.to]
            IN
              /\ log' = [log EXCEPT ![msg.to] = newFollowerLog]
              /\ commitIndex' = [commitIndex EXCEPT ![msg.to] = newFollowerCommit]
              /\ kv' = [kv EXCEPT ![msg.to] = newFollowerKv]
              /\ prevCommitIndex' = commitIndex
              /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                             electionTimeout, leaderElectedAtCommit, connected, clock >>

DropDelivery(msgIdx) ==
  /\ msgIdx \in 1..Len(deliveryQueue)
  /\ deliveryQueue' = RemoveAt(deliveryQueue, msgIdx)
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, role, currentTerm, votedFor, votesGranted, electionElapsed,
                 electionTimeout, log, commitIndex, kv, leaderElectedAtCommit,
                 connected, clock >>

CrashRestart(n) ==
  /\ n \in Nodes
  /\ LET committedPrefix == Prefix(log[n], commitIndex[n])
     IN
       /\ log' = [log EXCEPT ![n] = committedPrefix]
       /\ kv' = [kv EXCEPT ![n] = ApplyCommitted(committedPrefix, commitIndex[n])]
  /\ role' = [role EXCEPT ![n] = IF n = leader THEN "leader" ELSE "follower"]
  /\ votedFor' = [votedFor EXCEPT ![n] = NoVote]
  /\ electionElapsed' = [electionElapsed EXCEPT ![n] = 0]
  /\ prevCommitIndex' = commitIndex
  /\ UNCHANGED << leader, currentTerm, votesGranted, electionTimeout, commitIndex,
                 leaderElectedAtCommit, connected, clock, deliveryQueue >>

Next ==
  \/ \E k \in Keys, v \in Values : LeaderAppendPut(k, v)
  \/ \E k \in Keys : LeaderAppendDelete(k)
  \/ \E n \in Nodes : Tick(n)
  \/ AdvanceTime
  \/ \E n \in Nodes : StartElection(n)
  \/ \E v, c \in Nodes : GrantVote(v, c)
  \/ \E c \in Nodes : BecomeLeader(c)
  \/ Heartbeat
  \/ \E n \in Nodes : Disconnect(n)
  \/ \E n \in Nodes : Reconnect(n)
  \/ \E n \in Nodes, k \in Keys, v \in Values : FollowerDiverges(n, k, v)
  \/ \E n \in Nodes, idx \in 0..Len(log[leader]), d \in 0..2 : SendAppend(n, idx, d)
  \/ \E i \in 1..Len(deliveryQueue) : DeliverAppend(i)
  \/ \E i \in 1..Len(deliveryQueue) : DropDelivery(i)
  \/ \E n \in Nodes : CrashRestart(n)

Spec == Init /\ [][Next]_vars

CommitMonotonicInv ==
  \A n \in Nodes : commitIndex[n] >= prevCommitIndex[n]

ElectionCommitLinkInv ==
  \A n \in Nodes : role[n] = "leader" => commitIndex[n] >= leaderElectedAtCommit[n]

CommittedPrefixAgreementInv ==
  \A n \in Nodes :
    commitIndex[n] <= commitIndex[leader]
    => Prefix(log[n], commitIndex[n]) = Prefix(log[leader], commitIndex[n])

CommittedLogMatchingInv ==
  \A a, b \in Nodes :
    Prefix(log[a], Min2(commitIndex[a], commitIndex[b]))
    = Prefix(log[b], Min2(commitIndex[a], commitIndex[b]))

CommittedStateAgreementInv ==
  \A a, b \in Nodes : commitIndex[a] = commitIndex[b] => kv[a] = kv[b]

CiConstraint ==
  /\ clock <= 2
  /\ Len(deliveryQueue) <= 1
  /\ \A n \in Nodes : Len(log[n]) <= 1
  /\ \A n \in Nodes : currentTerm[n] <= 3

NightlyConstraint ==
  /\ clock <= 5
  /\ Len(deliveryQueue) <= 3
  /\ \A n \in Nodes : Len(log[n]) <= 3
  /\ \A n \in Nodes : currentTerm[n] <= 6

THEOREM TypeSafety == Spec => []TypeInv
THEOREM CommitMonotonicSafety == Spec => []CommitMonotonicInv
THEOREM ElectionCommitLinkSafety == Spec => []ElectionCommitLinkInv
THEOREM CommittedPrefixAgreementSafety == Spec => []CommittedPrefixAgreementInv
THEOREM CommittedLogMatchingSafety == Spec => []CommittedLogMatchingInv
THEOREM CommittedStateAgreementSafety == Spec => []CommittedStateAgreementInv

====
