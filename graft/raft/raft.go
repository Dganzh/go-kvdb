package raft

import (
	"bytes"
	log "github.com/Dganzh/zlog"
	"github.com/Dganzh/zrpc"
	"graft/codec"
	"graft/config"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CmdTypeCommon = iota 
	CmdTypeSnapshot
	CmdTobeLeader
)

const (
	RoleLeader = iota + 1
	RoleFollower
	RoleCandidate
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsLeader bool
	CommandType int 
}

type LogEntry struct {
	Content interface{}
	Term 	int
	Index 	int
}


type Raft struct {
	mu        sync.Mutex          
	peers     []*zrpc.Client 
	persister *Persister     
	me        int        
	dead      int32               // set by Kill()

	CurrentTermID int
	Leader        int // peer's index into peers[]
	Role          int // 1-Leader,2-follower,3-candidate
	lastPing      time.Time
	HasVote       bool
	Voting        bool // select leading doing?

	Logs        []LogEntry
	CommitIndex int
	NextIndex   []int
	MatchIndex  []int
	applyCh     chan ApplyMsg
	applyIndex  int
	snapshotIndex int		// LogEntry和通信传输出去的要加上这个值
	installSnapshotCh chan int
	installSnapshotArgs *InstallSnapshotArgs

	pingInterval time.Duration
	checkLeaderInterval time.Duration
	selectTimeout int
	leaderTimeout time.Duration
}

func (rf *Raft) SetFollower() {
	rf.Role = RoleFollower
}


func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTermID
	isleader = rf.Role == RoleLeader
	return term, isleader
}

func (rf *Raft) IsLeader() bool {
	return !rf.killed() && rf.Role == RoleLeader
}


type QueryStateArgs struct {}

type QueryStateReply struct {
	Me        int
	Dead      int32
	CurrentTermID int
	Leader        int
	Role          int
	LastPing      time.Time
	HasVote       bool
	Voting        bool
	Logs        []LogEntry
	CommitIndex int
	NextIndex   []int
	MatchIndex  []int
	ApplyIndex  int
	SnapshotIndex int
}


func (rf *Raft) QueryStateHandler(args *QueryStateArgs, reply *QueryStateReply) {
	reply.Me = rf.me
	reply.Dead = rf.dead
	reply.CurrentTermID = rf.CurrentTermID
	reply.Leader = rf.Leader
	reply.Role = rf.Role
	reply.LastPing = rf.lastPing
	reply.HasVote = rf.HasVote
	reply.Voting = rf.Voting

	reply.Logs = rf.Logs
	reply.CommitIndex = rf.CommitIndex
	reply.NextIndex = rf.NextIndex
	reply.MatchIndex = rf.MatchIndex
	reply.ApplyIndex = rf.applyIndex
	reply.SnapshotIndex = rf.snapshotIndex
}


func (rf *Raft) ApplyLog() {
	rf.mu.Lock()
	rf.applyIndex++
	rf.persist()
	rf.mu.Unlock()
}

// 把commit了但是没有apply到状态机的log都发到applyCh,把log应用到状态机
func (rf *Raft) RestoreLog() ([]ApplyMsg, int, int) {
	rf.mu.Lock()
	aIdx := rf.applyIndex
	isLeader := rf.Role == RoleLeader
	spLogs := DecodeLog(rf.persister.ReadSnapshot())
	logs := make([]ApplyMsg, 0, len(spLogs))
	for i := 0; i < len(spLogs); i++ {
		entry := spLogs[i]
		logs = append(logs, ApplyMsg{true, entry.Content, entry.Index, isLeader, CmdTypeSnapshot})
	}

	// 把宕机之前apply了的都发过去
	for i := 0; i < aIdx; i++ {
		entry := rf.Logs[i]
		logs = append(logs, ApplyMsg{true, entry.Content, entry.Index, isLeader, CmdTypeSnapshot})
	}
	if len(logs) > 0 {
		log.Info(rf.me, "RestoreLog", len(logs), aIdx, rf.CommitIndex)
	}
	rf.mu.Unlock()
	log.Info(rf.me, "send to kvserver Ok, num:", len(logs))
	return logs, len(spLogs), aIdx
}


func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeData())
}

// 把要持久化的字段转成[]byte
func (rf *Raft) encodeData() []byte {
	w := new(bytes.Buffer)
	e := codec.NewEncoder(w)
	e.Encode(rf.CurrentTermID)
	e.Encode(rf.Leader)
	e.Encode(rf.Role)
	e.Encode(rf.HasVote)
	e.Encode(rf.Voting)
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.applyIndex)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.NextIndex)
	e.Encode(rf.MatchIndex)
	if rf.installSnapshotArgs == nil {
		rf.installSnapshotArgs = &InstallSnapshotArgs{}
	}
	e.Encode(rf.installSnapshotArgs)
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := codec.NewDecoder(r)
	var CurrentTermID int
	var Leader int
	var Role int
	var HasVote bool
	var Voting bool
	var Logs []LogEntry
	var CommitIndex int
	var applyIndex int
	var snapshotIndex int
	var NextIndex []int
	var MatchIndex []int
	var installSnapshotArgs *InstallSnapshotArgs
	if d.Decode(&CurrentTermID) != nil ||
		d.Decode(&Leader) != nil ||
		d.Decode(&Role) != nil ||
		d.Decode(&HasVote) != nil ||
		d.Decode(&Voting) != nil ||
		d.Decode(&Logs) != nil ||
		d.Decode(&CommitIndex) != nil ||
		d.Decode(&applyIndex) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&NextIndex) != nil ||
		d.Decode(&MatchIndex) != nil {
		panic("readPersist failed exit")
	} else {
		rf.CurrentTermID = CurrentTermID
		rf.Leader = Leader
		rf.Role = Role
		rf.HasVote = HasVote
		rf.Voting = Voting
		rf.Logs = Logs
		rf.CommitIndex = CommitIndex
		rf.snapshotIndex = snapshotIndex
		rf.applyIndex = applyIndex
		if len(NextIndex) > 0 {
			rf.NextIndex = NextIndex
		} else {
			rf.NextIndex = make([]int, len(rf.peers))
		}
		if len(MatchIndex) > 0 {
			rf.MatchIndex = MatchIndex
		} else {
			rf.MatchIndex = make([]int, len(rf.peers))
		}
		if rf.snapshotIndex > 0 {
			d.Decode(&installSnapshotArgs)
			rf.installSnapshotArgs = installSnapshotArgs
		} else {
			rf.installSnapshotArgs = &InstallSnapshotArgs{}
		}
	}
}

func DecodeLog(snapshot []byte) []LogEntry {
	r := bytes.NewBuffer(snapshot)
	d := codec.NewDecoder(r)
	var spLogs []LogEntry
	d.Decode(&spLogs)
	return spLogs
}


// 当前log转成[]byte，再读取之前快照，合并两个[]byte，生成快照
// 保存快照，删除log
// 再检查nextIndex,如果发现index已经被删，就发送快照给他
func (rf *Raft) SaveStateAndSnapshot() {
	log.Info("SaveStateAndSnapshot start")
	rf.mu.Lock()
	spLogSize := rf.applyIndex
	if spLogSize == len(rf.Logs) {
		spLogSize--			// 保留一条log
	}
	if spLogSize < 2 {
		rf.mu.Unlock()
		return
	}
	rmLastLog := rf.Logs[spLogSize - 1]
	persistIndex := rf.snapshotIndex + spLogSize
	oldSnapshot := rf.persister.ReadSnapshot()
	oldLog := DecodeLog(oldSnapshot)
	allLog := append(oldLog, rf.Logs[:spLogSize]...)

	w1 := new(bytes.Buffer)
	e1 := codec.NewEncoder(w1)
	e1.Encode(allLog)
	snapshotData := w1.Bytes()

	rf.installSnapshotArgs = &InstallSnapshotArgs{
		Term: rf.CurrentTermID,
		Leader: rf.me,
		Snapshot: []byte{},		//snapshotData,发送RPC时再读出来
		LastIncludedIndex: rmLastLog.Index,
		LastIncludedTerm: rmLastLog.Term,
	}
	// 调整各个index
	// 调整各个peer的index，同时发送快照过去（如果需要）
	if rf.Role == RoleLeader {
		for peer, _ := range rf.peers {
			if rf.me == peer {
				continue
			}
			// 有还没同步过去的log，但是被删了
			//remainMaxIndex := len(rf.Logs) - spLogSize
			if rf.MatchIndex[peer] < spLogSize {
				// 等心跳检测到log被删再发通知吧
			} else {
				rf.MatchIndex[peer] -= spLogSize
				rf.NextIndex[peer] -= spLogSize
			}
		}
	}
	rf.snapshotIndex = persistIndex
	rf.CommitIndex -= spLogSize
	// 删除log
	rf.Logs = rf.Logs[spLogSize:]
	rf.applyIndex -= spLogSize

	w2 := new(bytes.Buffer)
	e2 := codec.NewEncoder(w2)
	e2.Encode(rf.CurrentTermID)
	e2.Encode(rf.Leader)
	e2.Encode(rf.Role)
	e2.Encode(rf.HasVote)
	e2.Encode(rf.Voting)
	e2.Encode(rf.CommitIndex)
	e2.Encode(rf.applyIndex)
	e2.Encode(rf.NextIndex)
	e2.Encode(rf.MatchIndex)
	e2.Encode(rf.snapshotIndex)
	stateData := w2.Bytes()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term int
	Leader	int
	Snapshot []byte
	LastIncludedIndex int
	LastIncludedTerm int
}

type InstallSnapshotReply struct {
	Term int
	Success bool
}

func (rf *Raft) syncSnapshot() {
	rf.mu.Lock()
	syncState := make([]bool, len(rf.peers))
	for i := 0; i < len(syncState); i++ {
		syncState[i] = i == rf.me
	}
	finishCh := make(chan int, len(rf.peers))
	rf.mu.Unlock()
	for {
		if rf.killed() {
			return
		}
		if rf.Role != RoleLeader {
			return
		}
		select {
		case peer := <- rf.installSnapshotCh:
			rf.mu.Lock()
			if syncState[peer] {
				rf.mu.Unlock()
				continue
			}
			syncState[peer] = true
			rf.mu.Unlock()
			go rf.syncSnapshotForPeer(peer, finishCh)
		case peer := <- finishCh:
			rf.mu.Lock()
			syncState[peer] = false
			rf.mu.Unlock()
		}

	}

}


func (rf *Raft) syncSnapshotForPeer(peerIdx int, finishCh chan int) {
	reply := &InstallSnapshotReply{}
	reqCnt := 1
	for {
		if rf.killed() || rf.Role != RoleLeader {
			return
		}
		rf.mu.Lock()
		args := &InstallSnapshotArgs{
			Term: rf.CurrentTermID,
			Leader: rf.Leader,
			Snapshot: rf.persister.ReadSnapshot(),
			LastIncludedIndex: rf.installSnapshotArgs.LastIncludedIndex,
			LastIncludedTerm: rf.installSnapshotArgs.LastIncludedTerm,
		}
		rf.mu.Unlock()
		log.Info(rf.me, "start rpc Raft.InstallSnapshot", peerIdx, "logbyteSize", len(args.Snapshot))
		if ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", args, reply); ok {
			log.Infof("%d get reply rpc Raft.InstallSnapshot %+v\n", rf.me, reply)
			rf.mu.Lock()
			if args.Term != rf.CurrentTermID {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.CurrentTermID {
				log.Info(rf.me, "Raft.InstallSnapshot reply set role 2", reply.Term, "my currentterm", rf.CurrentTermID)
				rf.Leader = 0
				rf.SetFollower()
				rf.CurrentTermID = reply.Term
				rf.mu.Unlock()
				return
			}
			// peer安装快照成功
			if reply.Success {
				if args.LastIncludedIndex == rf.snapshotIndex {
					rf.MatchIndex[peerIdx] = 0
					rf.NextIndex[peerIdx] = 0
					finishCh <- peerIdx		// 标记完成不需要再循环发送RPC了
					log.Info("success rpc Raft.InstallSnapshot")
				} else {
					rf.MatchIndex[peerIdx] = -1
					rf.NextIndex[peerIdx] = -1
					log.Info("success rpc Raft.InstallSnapshot, but need install again", args.LastIncludedIndex, rf.snapshotIndex)
				}
			}
			rf.mu.Unlock()
			return
		} else {
			log.Info("failed rpc Raft.InstallSnapshot")
		}
		time.Sleep(time.Duration(50 * reqCnt) * time.Millisecond)
	}
}

// 处理leader传过来的安装快照rpc
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	log.Infof("get InstallSnapshot rpc args:%+v", args)
	reply.Success = false
	if args.Term < rf.CurrentTermID {
		reply.Term = rf.CurrentTermID
		log.Warn("should not install snapshot term is old", args.Term, rf.CurrentTermID)
		return
	}
	idx := args.LastIncludedIndex
	term := args.LastIncludedTerm
	// 快照里最后一个log已经提交了，使用本地logs快照即可
	if idx <= rf.CommitIndex + rf.snapshotIndex {
		log.Warn(rf.me, "should not install snapshot log has exists", idx, rf.snapshotIndex, rf.applyIndex)
		reply.Success = true
		return
	}

	// 删掉本地的快照，使用传来的快照
	rf.mu.Lock()
	rmLogIdx := idx - rf.snapshotIndex
	if rmLogIdx < len(rf.Logs) && rf.Logs[rmLogIdx - 1].Term == term {
		rf.Logs = rf.Logs[rmLogIdx:]		// 快照之后的log仍要保留
		if rf.CommitIndex > rmLogIdx {
			rf.CommitIndex -= rmLogIdx
		} else {
			rf.CommitIndex = 0
		}
	} else {
		rf.Logs = rf.Logs[:0]
		rf.CommitIndex = 0
	}
	r := bytes.NewBuffer(args.Snapshot)
	d := codec.NewDecoder(r)
	var spLogs []LogEntry
	if d.Decode(&spLogs) != nil {
		reply.Term = rf.CurrentTermID
		rf.mu.Unlock()
		return
	}
	applyIdx := rf.snapshotIndex + rf.applyIndex
	spLogs = spLogs[applyIdx:]

	// 直接应用这些log到状态机
	for _, entry := range spLogs {
		rf.applyCh <- ApplyMsg{false, entry.Content, entry.Index, false, CmdTypeSnapshot}
	}
	rf.applyIndex = 0
	rf.snapshotIndex = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeData(), args.Snapshot)
	reply.Term = rf.CurrentTermID
	reply.Success = true
	rf.mu.Unlock()
	log.Info("install snapshot success!")
}

type RequestVoteArgs struct {
	CandidateID  int
	TermID       int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Vote	bool 		// 是否投这一票
	Term 	int
}


// 处理其他人发来的投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Infof("args %+v, reply %+v\n", args, reply)
	// 可能在这里之前，收到的心跳包把rf.currentTermID置为args.TermID了，所以这里是要大于等于
	if args.TermID >= rf.CurrentTermID {
		rf.mu.Lock()
		stateChanged := false
		mustVote := false
		// 只要其他人有更新的Term，就得更新自己的
		if args.TermID > rf.CurrentTermID {
			mustVote = true
			rf.HasVote = false // termID有更新的就肯定没在这Term内投过票
			log.Infof("set server%d hasvote=false, because termid upgrade: old=%d, new=%d\n", rf.me, rf.CurrentTermID, args.TermID)
			rf.CurrentTermID = args.TermID
			rf.SetFollower()
			stateChanged = true
		}
		shouldVote := rf.checkForSelect(args.LastLogIndex, args.LastLogTerm)
		log.Infof("server%d should vote=%t for %d, vote state=%t, args:%+v\n", rf.me, shouldVote, args.CandidateID, rf.HasVote, args)
		if !shouldVote || !mustVote && (rf.HasVote || rf.Voting) {
			reply.Vote = false
			if stateChanged {
				rf.persist()
			}
			rf.mu.Unlock()
			return
		}
		// 需要投票时才设置role
		reply.Vote = true
		reply.Term = rf.CurrentTermID
		rf.Role = RoleCandidate
		rf.HasVote = true
		rf.lastPing = time.Now()		// 考虑这里有无必要更新？
		rf.persist()
		rf.mu.Unlock()
		return
	}
	reply.Vote = false
}

type PingArgs struct {
	TermID int
	Leader int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type PingReply struct {
	TermID    int
	Leader    int
	Success   bool
	CTerm     int // 冲突的Term
	CMinIndex int // 冲突的Term的最小Index
}


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
		if rf.Role != RoleCandidate {
			return true
		}
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		return ok
}


// 接收处理ping
func (rf *Raft) Ping(args *PingArgs, reply *PingReply) {
	rf.mu.Lock()
	if args.TermID >= rf.CurrentTermID {
		rf.lastPing = time.Now()
		rf.Leader = args.Leader
		// 两个term相同，不应该重置这个状态！不然会把同一Term的票再投一次
		if args.TermID > rf.CurrentTermID {
			rf.HasVote = false // 重置这个已选举状态，为下次选举做准备
		}
		rf.CurrentTermID = args.TermID
		// 这里切换是否合理？
		rf.SetFollower()        // 收到ping,说明竞选完成了，切换为follower
		success, myIndex, myTerm := rf.SyncLogEx(args.PrevLogIndex, args.PrevLogTerm, args.Entries)
		if myIndex >= 0 {
			// 通信的用真实的index
			myIndex += rf.snapshotIndex
		}
		reply.Success = success
		reply.CTerm = myTerm
		reply.CMinIndex = myIndex
		reply.TermID = rf.CurrentTermID
		reply.Leader = rf.Leader
		var commitLogs []ApplyMsg
		if reply.Success {		// 为false时，可能rf.logs里的log需要删除的就不能commit
			if len(rf.Logs) > 0 {
				idx := rf.Logs[len(rf.Logs) - 1].Index
				var cIndex int
				if idx < args.LeaderCommit {
					cIndex = idx
				} else {
					cIndex = args.LeaderCommit
				}
				cIndex -= rf.snapshotIndex
				for index := rf.applyIndex + 1; index <= cIndex; index++  {
					entry := rf.Logs[index - 1]
					commitLogs = append(commitLogs, ApplyMsg{true, entry.Content,
						entry.Index, false, CmdTypeCommon})
				}
				rf.CommitIndex = cIndex
			} else {
				rf.CommitIndex = 0
			}
		}
		rf.persist()
		rf.mu.Unlock()
		// 释放锁之后再发送至ch
		go func() {
			for _, msg := range commitLogs {
				rf.applyCh <- msg
			}
		}()
	} else {  	// 这个leader太旧了，不合法，可能需要重新选举
		reply.TermID = rf.CurrentTermID
		reply.Leader = rf.Leader
		reply.Success = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendPingForPeer(idx int) int {
	if idx == rf.me {
		return 2000
	}
	rf.mu.Lock()
	// 每次发送前都检查一下自己是不是leader，避免发给部分服务器后自己已经不是leader了
	if rf.Role != RoleLeader { // 只有leader才需要发出心跳
		rf.mu.Unlock()
		return 50			// 这里break然后统计commit数量看看有没有log超过半数？
	}
	var sendLogs []LogEntry
	successNextIndex := 0
	prevLogIndex, prevLogTerm := 0, 0
	if len(rf.Logs) > 0 {
		if rf.snapshotIndex > 0	{
			if rf.NextIndex[idx] < 0 || rf.NextIndex[idx] > len(rf.Logs) {
				// log已删，通知安装快照
				rf.installSnapshotCh <- idx
				rf.mu.Unlock()
				return 1000
			}
		}
		if rf.NextIndex[idx] == rf.MatchIndex[idx] {
			sendLogs = rf.Logs[rf.NextIndex[idx]:]
			successNextIndex = len(rf.Logs)
		}
		if rf.NextIndex[idx] == 0 {
			if rf.installSnapshotArgs != nil && rf.installSnapshotArgs.LastIncludedIndex > 0 {
				prevLogIndex = rf.installSnapshotArgs.LastIncludedIndex
				prevLogTerm = rf.installSnapshotArgs.LastIncludedTerm
			} else {
				prevLogIndex = 0
				prevLogTerm = 0
			}
		} else {
			prevLogIndex = rf.snapshotIndex + rf.NextIndex[idx]
			prevLogTerm = rf.Logs[prevLogIndex-rf.snapshotIndex-1].Term
		}
	}
	args := &PingArgs{
		TermID: rf.CurrentTermID,
		Leader: rf.me,
		LeaderCommit: rf.snapshotIndex + rf.CommitIndex,	// 想起他peer通信时用真是的index
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: sendLogs,
	}
	oldSpIdx := rf.snapshotIndex
	rf.mu.Unlock()
	reply := &PingReply{}
	startRPC := time.Now()
	if ok := rf.peers[idx].Call("Raft.Ping", args, reply); !ok {
		if time.Since(startRPC) > time.Second {
			log.Info(rf.me, "ping to", idx, "failed rpc cost time", time.Since(startRPC))
		}
		return 20
	}
	rf.mu.Lock()
	var commitLogs []ApplyMsg
	defer func() {
		rf.mu.Unlock()
		//这里发送后会立马切换goroutine,换到接收方，接收方接受过程中需要锁，导致死锁
		// 释放锁，最后再发送到ch
		if len(commitLogs) > 0 {
			go func() {
				for _, msg := range commitLogs {
					rf.applyCh <- msg
				}
			}()
		}
	}()
	// 请求回来可能已经不是leader了，就没必要处理响应了
	if rf.Role != RoleLeader {
		log.Warn(rf.me,"I not leader after rpc response", idx, "now role:", rf.Role)
		return 100
	}
	defer rf.persist()
	if reply.TermID > rf.CurrentTermID {
		rf.CurrentTermID = reply.TermID
		rf.Leader = reply.Leader
		rf.SetFollower()
		rf.lastPing = time.Now()	// 避免checkLeaderAlive接下来的短时间内发起选举
		return 200
	}
	if oldSpIdx != rf.snapshotIndex {
		successNextIndex -= rf.snapshotIndex - oldSpIdx
	}
	res := 0		// 默认不休眠，下一回直接运行，使log同步更快
	if reply.Success {
		// 这两种情况需要休眠长一点
		// 1. 刚发送了log
		// 2. 同步完log，发送空的心跳包
		if len(sendLogs) > 0 || (len(sendLogs) == 0 && rf.NextIndex[idx] == rf.MatchIndex[idx]) {
			res = 100
		}
		if len(sendLogs) > 0 {
			//rf.NextIndex[idx] += len(sendLogs)		// rpc太久可能rf.NextIndex[idx]因为选举成功被初始化了
			rf.NextIndex[idx] = successNextIndex
		} else {
			if reply.CMinIndex < rf.snapshotIndex {
				rf.NextIndex[idx] = -1
			} else {
				rf.NextIndex[idx] = reply.CMinIndex - rf.snapshotIndex
			}
		}
		rf.MatchIndex[idx] = rf.NextIndex[idx]
		commitLogs = rf.checkCanCommitLog()
	} else {
		if rf.NextIndex[idx] > 1 {
			if reply.CTerm <= 0 {		// 说明对方一个log都没
				rf.NextIndex[idx] = 0
			} else {
				exists := false
				cMinIndex := reply.CMinIndex - rf.snapshotIndex
				if cMinIndex < 0 {
					// log已删，通知安装快照
					rf.installSnapshotCh <- idx
					return 1000
				} else if cMinIndex == 0 {
					// 刚好安装了快照的log同步完过去了
					if reply.CTerm == rf.installSnapshotArgs.LastIncludedTerm {
						// 余下的rf.Logs都是接下来要同步的
						rf.NextIndex[idx] = 0
					} else {
						rf.installSnapshotCh <- idx
						return 1000
					}
				} else {
					for i := rf.NextIndex[idx]-1; i >= cMinIndex; i-- {
						if reply.CTerm == rf.Logs[i-1].Term {
							rf.NextIndex[idx] = i
							exists = true
							break
						}
					}

					if exists == false {
						rf.NextIndex[idx] = cMinIndex - 1
					}
					if rf.NextIndex[idx] < 0 {
						rf.NextIndex[idx] = 0
					}
					res = 40
				}
			}
		} else {
			rf.NextIndex[idx] = 0
		}
	}
	if len(rf.Logs) == 0 {
		res = 100
	}
	log.Info(rf.me, "after log ping", "server=", idx, "MatchIndex=", rf.MatchIndex, "NextIndex=", rf.NextIndex)
	return res
}

func (rf *Raft) CountCommitted(index int) int {
	if index > len(rf.Logs) {
		return 0
	}
	n := 1		// leader自己
	for idx, _ := range rf.peers {
		if idx != rf.me && rf.MatchIndex[idx] >= index { // 已复制的大于这个index
			n++
		}
	}
	return n
}


func (rf *Raft) sendPing() {
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(pi int) {
			t := time.NewTicker(rf.pingInterval)
			defer t.Stop()
			for range t.C {
				if rf.killed() {
					return
				}
				if rf.Role != RoleLeader {
					return
				}
				rf.sendPingForPeer(pi)
			}
		}(idx)
	}
}

func (rf *Raft) checkCanCommitLog() (res []ApplyMsg) {
	// 检查哪些log是大部分commit了的
	for cIndex := len(rf.Logs); cIndex >= rf.CommitIndex+1; cIndex-- {
		// 从新往旧找，找到最新的 且 提交超过半数的
		if rf.Logs[cIndex-1].Term != rf.CurrentTermID {
			continue // flag@1 这里必需！！！
		}
		cnt := rf.CountCommitted(cIndex)
		if cnt > len(rf.peers) / 2 {
			isLeader := rf.Role == RoleLeader
			for index := rf.applyIndex + 1; index <= cIndex; index++ {
				entry := rf.Logs[index-1]
				res = append(res, ApplyMsg{true, entry.Content,
					entry.Index, isLeader, CmdTypeCommon})
			}
			rf.CommitIndex = cIndex
			log.Debug(rf.me, "in SendPing set CommitIndex", rf.CommitIndex, rf.CommitIndex + rf.snapshotIndex)
			return
		}
	}
	return
}


func (rf *Raft) keepAlive() {
	rf.sendPing()
}

func (rf *Raft) initLogIndex() {
	index := 0
	if len(rf.Logs) > 0 {
		index = rf.Logs[len(rf.Logs) - 1].Index - rf.snapshotIndex
	}
	for idx, _ := range rf.peers {
		rf.NextIndex[idx] = index
		rf.MatchIndex[idx] = 0
	}
}

// follower处理ping时调用
// 拿新leader的log修复自己的log，使得跟leader的log一致
func (rf *Raft) SyncLog(logIndex int, logTerm int, logs []LogEntry) bool {
	if len(logs) > 0 {
		if logIndex > len(rf.Logs) || (logIndex <= len(rf.Logs) && rf.Logs[logIndex-1].Term != logTerm) {
			return false
		}
		rf.Logs = append(rf.Logs[:logIndex], logs...)
		rf.persist()
		return true
	}

	if logIndex > len(rf.Logs) {
		return false
	}
	if logIndex == 0 {
		rf.Logs = rf.Logs[:0] // log都是多余的，清空掉
		rf.persist()
		return true
	}
	ok := rf.Logs[logIndex-1].Term == logTerm
	if len(rf.Logs) > 0 {
		rf.Logs = rf.Logs[:logIndex] // 把多余的干掉
		rf.persist()
	}
	if !ok {
		return false
	}
	return true
}

// 优化版
func (rf *Raft) SyncLogEx(logIndex int, logTerm int, logs []LogEntry) (bool, int, int) {
	if len(logs) > 0 {
		// 都已经commit了，你还同步给我，肯定是你落后太多了
		maxLogIdx := logs[len(logs) - 1].Index
		if maxLogIdx <= rf.snapshotIndex + rf.CommitIndex {
			return true, -1, -1
		}

		// 之前收到log了，但是leader没有收到响应
		// 会出现logs与rf.Logs有交叉的情况，中括号为logs,小括号为rf.Logs， 那么他们的范围是这样的([ )]
		// 此时idx是这样的：[logIndex,rf.Logs[0].Index, rf.snapshotIndex + rf.CommitIndex, rf.Logs[-1].Index, logs[-1].Index]
		if logIndex < rf.snapshotIndex + rf.CommitIndex {
			existsCnt := rf.snapshotIndex + rf.CommitIndex - logIndex
			logIndex = rf.CommitIndex
			logs = logs[existsCnt:]
		} else {
			// 这种是正常的情况
			// [rf.Logs[0].Index, logIndex, rf.snapshotIndex + rf.CommitIndex]
			logIndex -= rf.snapshotIndex
			if logIndex <= len(rf.Logs) && logIndex > 0 && rf.Logs[logIndex-1].Term != logTerm {
				return false, -1, -1
			}
		}

		if logIndex < len(rf.Logs) {
			rf.Logs = append(rf.Logs[:logIndex], logs...)
		} else {
			rf.Logs = append(rf.Logs, logs...)
		}
		rf.persist()
		return true, 0, 0
	}

	if logIndex > len(rf.Logs) {
		if len(rf.Logs) == 0 {
			return false, 0, 0
		}
		myIndex := len(rf.Logs)
		myTerm := rf.Logs[myIndex - 1].Term
		if myTerm == logTerm {		// rf.logs都在leader中了
			return true, myIndex, myTerm
		}

		// 所有log的Term都跟myTerm相同
		if myTerm == rf.Logs[0].Term {
			return false, 1, myTerm
		}
		// 找到跟myTerm相同的log中最小的index（这里最小的可能是2）
		for i := len(rf.Logs) - 1; i > 0; i-- {
			if myTerm != rf.Logs[i-1].Term {
				myIndex = i + 1
				break
			}
		}
		return false, myIndex, myTerm
	}
	if logIndex == 0 {
		rf.Logs = rf.Logs[:0] // log都是多余的，清空掉
		rf.persist()
		return true, 0, 0
	}
	ok := rf.Logs[logIndex-1].Term == logTerm
	if len(rf.Logs) > 0 {
		rf.Logs = rf.Logs[:logIndex] // 把多余的干掉
		rf.persist()
	}
	myIndex := logIndex
	myTerm := rf.Logs[myIndex - 1].Term
	if !ok {
		// 所有log的Term都跟myTerm相同
		if myTerm == rf.Logs[0].Term {
			return false, 1, myTerm
		}
		// 找到跟myTerm相同的log中最小的index（这里最小的可能是2）
		for i := logIndex - 1; i > 0; i-- {
			if myTerm != rf.Logs[i-1].Term {
				myIndex = i + 1
				break
			}
		}
		return false, myIndex, myTerm
	}
	return true, myIndex, myTerm
}


// 提交log入口
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	_, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	term = rf.CurrentTermID
	entry := LogEntry{
		Content: command,
		Index: 0,
		Term: term,
	}
	if len(rf.Logs) > 0 {
		lastLog := rf.Logs[len(rf.Logs) - 1]
		entry.Index = lastLog.Index + 1
	} else {
		entry.Index = 1			// 起始log index为1
	}
	index = entry.Index
	rf.Logs = append(rf.Logs, entry)
	rf.MatchIndex[rf.me] = entry.Index - rf.snapshotIndex
	rf.persist()
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) checkLeaderAlive() {
	if rf.Role == 0 {	// 刚启动，没leader呢
		if rf.me != 0 {		// 让0先跑，其他休眠，使得0较容易获得第一届leader
			time.Sleep(500 * time.Millisecond)
		}
	}
	t := time.NewTicker(rf.checkLeaderInterval)
	defer t.Stop()
	for range t.C {
		if rf.killed() {
			return
		}
		if rf.Role == RoleLeader {
			continue
		}
		for rf.shouldStartSelect()  {
			if !rf.StartSelect() {
				// 竞选失败，短暂休息继续选举
				time.Sleep(5 * time.Millisecond)
			} else {
				// 自己当选了，此函数将来一段时间不需要执行了
				time.Sleep(500 * time.Millisecond)
				log.Debug(rf.me, "after sleep role.........", rf.Role)
				break
			}
		}
	}
}

// 检查要不要投票给这个人
// 5.4.1判断log更新，Term更大者更新，index更大者更新
func (rf *Raft) checkForSelect(lastLogIndex, lastLogTerm int) bool {
	if lastLogIndex == 0 {			// 这个人没有log
		return len(rf.Logs)	== 0	// 我也没log时才能投给他
	}
	if lastLogIndex <= rf.snapshotIndex {
		return false
	}
	if len(rf.Logs) > 0 {
		entry := rf.Logs[len(rf.Logs) - 1]
		// 先比较Term
		if entry.Term > lastLogTerm {
			return false
		} else if entry.Term < lastLogTerm {
			return true
		}
		// Term相等时进一步比较Index
		if entry.Index > lastLogIndex {
			return false
		}
		return true
	}
	return true			// 他有log，我没log，肯定应该投
}

// 检查是否应该发起选举
func (rf *Raft) shouldStartSelect() bool {
	if rf.Role == RoleLeader {
		return false
	}
	if rf.Voting {
		return false
	}
	// todo 
	if time.Since(rf.lastPing) > rf.leaderTimeout && !rf.killed() {
		return true
	}
	return false
}

func (rf *Raft) StartSelect() bool {
	rf.mu.Lock()
	log.Info(rf.me, " start select", rf.CurrentTermID, "Role", rf.Role)
	rf.Voting = true
	rf.Role = RoleCandidate
	rf.CurrentTermID++
	termID := rf.CurrentTermID
	//rf.HasVote = true
	lastLogIndex, lastLogTerm := 0, 0
	// 这里不能用CommitIndex，收到log就算了，不需要commit
	if len(rf.Logs) > 0 {
		entry := rf.Logs[len(rf.Logs) - 1]
		lastLogIndex = entry.Index
		lastLogTerm = entry.Term
	}
	args := RequestVoteArgs{
		CandidateID: rf.me,
		TermID: rf.CurrentTermID,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	vote := 1		// 自己的一票
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server != rf.me {
			go func(sev int) {
				reply := RequestVoteReply{}
				for {
					if rf.killed() {
						return
					}
					if !rf.Voting || termID != rf.CurrentTermID {
						log.Warn(rf.me, "vote", sev, "finish, now exit", termID)
						return
					}
					if rf.Role != RoleCandidate {
						log.Warn("I am not candidate, select finish? Now I will exit, Leader=", rf.Leader)
						return
					}
					ok := rf.sendRequestVote(sev, &args, &reply)
					if ok {
						break
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}
				if rf.Role != RoleCandidate {
					log.Warn("After sendRequestVote", "role", rf.Role, "peer", sev, "term", rf.CurrentTermID, "not flower, now I will exit, Leader=", rf.Leader)
					return
				}
				if reply.Vote && reply.Term == rf.CurrentTermID {
					rf.mu.Lock()
					vote++
					log.Info(rf.me, " get vote from", sev, "term", rf.CurrentTermID, "total: ", vote)
					rf.mu.Unlock()
				}
			}(server)
		}
	}
	timeout := rand.Intn(rf.selectTimeout) + 150		// 随机的超时时间
	startSelectTime := time.Now()
	for time.Since(startSelectTime) <= time.Duration(timeout) * time.Millisecond {
		rf.mu.Lock()
		if rf.Role != RoleCandidate || termID != rf.CurrentTermID {
			rf.mu.Unlock()
			break
		}
		if vote > len(rf.peers) / 2 {		// 获得大多数票数
			rf.Role = RoleLeader
			rf.Leader = rf.me
			rf.Voting = false
			//rf.HasVote = false	// 要是这时候刚好有其他人Term相同要求投票，那自己又会投一票，所以这里不能设为已投
			log.Info("Now I am Leader ", rf.me, "termid", rf.CurrentTermID)
			rf.keepAlive()
			go rf.syncSnapshot()
			go func() {
				rf.applyCh <- ApplyMsg{CommandType: CmdTobeLeader}
			}()
			rf.appendEmptyLogForLeader()
			rf.initLogIndex()
			rf.persist()
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	rf.mu.Lock()
	//rf.HasVote = false
	rf.Voting = false
	rf.persist()
	log.Info("this term failed", rf.CurrentTermID)
	rf.mu.Unlock()
	return false
}

func (rf *Raft) Connect(addrs []string) {
	var peers[]*zrpc.Client
	for _, peerAddr := range addrs {
		client := zrpc.NewClient(peerAddr)
		peers = append(peers, client)
	}
	rf.peers = peers
}


func MakeRaft(cfg *config.Config, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = cfg.Id
	rf.applyCh = applyCh
	rf.pingInterval = time.Duration(cfg.PingInterval) * time.Millisecond
	rf.checkLeaderInterval = time.Duration(cfg.CheckLeaderInterval) * time.Millisecond
	rf.selectTimeout = cfg.SelectTimeout
	rf.leaderTimeout = time.Duration(cfg.LeaderTimeout) * time.Millisecond
	return rf
}

func (rf *Raft) StartRaft(peers []*zrpc.Client) *Raft {
	rf.peers = peers
	persister := MakePersister()
	rf.persister = persister
	rf.installSnapshotCh = make(chan int, len(rf.peers))
	rf.readPersist(persister.ReadRaftState())
	if len(rf.NextIndex) == 0 {
		rf.NextIndex = make([]int, len(rf.peers))
	}
	if len(rf.MatchIndex) == 0 {
		rf.MatchIndex = make([]int, len(rf.peers))
	}
	if rf.CurrentTermID > 0 {
		rf.lastPing = time.Now()
	} else {
		rf.lastPing = time.Now().Add(-100 * time.Second)
	}
	if rf.Role == RoleLeader {
		rf.keepAlive()		// 成为leader时调用
		go rf.syncSnapshot()
	}
	go rf.checkLeaderAlive()

	// 刚刚恢复的leader睡眠一个心跳时间的周期，以确保自己真的是leader！
	//if rf.Role == 1 && rf.CurrentTermID > 0 {
	//	time.Sleep(70 * time.Millisecond)
	//}
	return rf
}

func (rf *Raft) appendEmptyLogForLeader() {
	term := rf.CurrentTermID
	entry := LogEntry{
		Content: "",
		Index: 0,
		Term: term,
	}
	if len(rf.Logs) > 0 {
		lastLog := rf.Logs[len(rf.Logs) - 1]
		entry.Index = lastLog.Index + 1
	} else {
		entry.Index = 1			// 起始log index为1
	}

	rf.Logs = append(rf.Logs, entry)
}

