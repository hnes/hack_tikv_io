package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type PrwTaskRequest struct {
	fileName string
	fileType string
	rwFlag   int
	// start offset
	offset int
	sz     int
}

func (p *PrwTaskRequest) CalcQuotaCost() (io, bytes int) {
	io = p.sz/256/1024 + 1
	bytes = p.sz
	return
}

func (p *PrwTaskRequest) String() string {
	return fmt.Sprintf("[%s %s %v %v %v]", p.fileName, p.fileType, p.rwFlag, p.offset, p.sz)
}

type MixIOSchedWaitQMember struct {
	req           PrwTaskRequest
	prwLoadType   string
	closeNitifyCh chan struct{}
}

type MixIOSchedDebtQMember struct {
	req         PrwTaskRequest
	prwLoadType string
}

type MixIOSchedRefundQMember struct {
	req       PrwTaskRequest
	leftBytes int
}

type fastpathCtType struct {
	reqSendCt          int
	reqFinishedCt      int
	reqSendBytesCt     int
	reqFinishedBytesCt int
}

func (c *fastpathCtType) Add(c0 fastpathCtType) {
	c.reqSendCt += c0.reqSendCt
	c.reqSendBytesCt += c0.reqSendBytesCt
	c.reqFinishedCt += c0.reqFinishedCt
	c.reqFinishedBytesCt += c0.reqFinishedBytesCt
}

func (c *fastpathCtType) IsBalanced() bool {
	return c.reqSendCt == c.reqFinishedCt
}

func (c *fastpathCtType) IsEqual(c0 fastpathCtType) bool {
	return c.reqSendCt == c0.reqSendCt && c.reqSendBytesCt == c0.reqSendBytesCt &&
		c.reqFinishedCt == c0.reqFinishedCt && c.reqFinishedBytesCt == c0.reqFinishedBytesCt
}

type sstReadIOIdentifierMapValueType struct {
	sequencialIOStartFromOffset int
	startT                      time.Time
}

type sstReadIOIdentifierType struct {
	fileNameMapToStatsSyncMap  sync.Map
	sequencialJudgeThresholdSz int
	validEntryMaxDuration      time.Duration
}

func newSSTReadIOIdentifier(sequencialJudgeThresholdSz int, validEntryMaxDuration time.Duration) *sstReadIOIdentifierType {
	var s sstReadIOIdentifierType
	s.sequencialJudgeThresholdSz = sequencialJudgeThresholdSz
	s.validEntryMaxDuration = validEntryMaxDuration
	return &s
}

func (s *sstReadIOIdentifierType) IsRandomOrSequencial(req PrwTaskRequest) (randomFlag bool) {
	assert(req.rwFlag == RWFlagR, "req.rwFlag == RWFlagR")
	if req.sz <= 0 {
		return true
	}
	if req.sz >= s.sequencialJudgeThresholdSz {
		return false
	}
	m, ok := s.fileNameMapToStatsSyncMap.Load(req.fileName)
	if ok && m != nil {
	} else {
		return true
	}
	myM := m.(*sync.Map)
	// all the key stored in the myM is the end offset of last io operation
	myV, ok := myM.Load(req.offset)
	if ok && myV != nil { // hit means this prw req a one sequencial prw op.
		sstReadV := myV.(*sstReadIOIdentifierMapValueType)
		t0 := time.Now()
		assert(t0.After(sstReadV.startT), "t0.After(sstReadV.startT)")
		if t0.Sub(sstReadV.startT) > s.validEntryMaxDuration {
			myM.Delete(req.offset)
			return true
		} else {
			assert(req.offset > sstReadV.sequencialIOStartFromOffset, "req.offset > sstReadV.sequencialIOStartFromOffset")
			if req.offset-sstReadV.sequencialIOStartFromOffset >= s.sequencialJudgeThresholdSz {
				return false
			} else {
				return true
			}
		}
	} else { // not hit
		return true
	}
}

func (s *sstReadIOIdentifierType) AddNewFinishedIO(req PrwTaskRequest) {
	assert(req.rwFlag == RWFlagR, "req.rwFlag == RWFlagR")
	if req.sz <= 0 {
		return
	}
	if req.sz >= s.sequencialJudgeThresholdSz {
		return
	}
	m, ok := s.fileNameMapToStatsSyncMap.Load(req.fileName)
	if ok && m != nil {
	} else {
		var newM sync.Map
		s.fileNameMapToStatsSyncMap.Store(req.fileName, &newM)
		m = &newM
		ok = true
	}
	myM := m.(*sync.Map)
	// all the key stored in the myM is the end offset of last io operation
	myV, ok := myM.Load(req.offset)
	if ok && myV != nil { // hit means this prw req a one sequencial prw op.
		myM.Delete(req.offset)
		sstReadV := myV.(*sstReadIOIdentifierMapValueType)
		myM.Store(req.offset+req.sz, sstReadV)
	} else { // not hit
		v := &sstReadIOIdentifierMapValueType{
			sequencialIOStartFromOffset: req.offset,
			startT:                      time.Now(),
		}
		myM.Store(req.offset+req.sz, v)
	}
}

type MixIOSched struct {
	iodepth int
	iops    int
	byteps  int

	fastpathCtLock sync.Mutex
	fastpathCt     fastpathCtType

	sstReadIOIdentifier *sstReadIOIdentifierType

	fastpathDebtQ   chan MixIOSchedDebtQMember
	fastpathRefundQ chan MixIOSchedRefundQMember

	// currently we don't have q0 because q0 is the fast path
	q1 chan MixIOSchedWaitQMember
	q2 chan MixIOSchedWaitQMember
}

const PrwLoadTypeOtherRW = "OtherRW"
const PrwLoadTypeRaftLogRead = "RaftLogRead"
const PrwLoadTypeRaftLogWrite = "RaftLogWrite"
const PrwLoadTypeRocksdbLogRead = "RocksdbLogRead"
const PrwLoadTypeRocksdbLogWrite = "RocksdbLogWrite"

const PrwLoadTypeRandSmallRead = "RandSmallRead"

const PrwLoadTypeRandSmallWrite = "RandSmallWrite"
const PrwLoadTypeSequentialRead = "SequentialRead"   // big chunk is a special case of sequencial
const PrwLoadTypeSequentialWrite = "SequentialWrite" // same as above comment

func (s *MixIOSched) judgePrwTaskRequest(req PrwTaskRequest) (prwLoadType string, fastPathFlag bool) {
	assert(req.fileType != FileTypeNotExist, "fileType!=FileTypeNotExist")
	if req.fileType == FileTypeRaftLog || req.fileType == FileTypeRocksdbLog ||
		req.fileType == FileTypeOther {

		if req.fileType == FileTypeRaftLog {
			fastPathFlag = true
			if req.rwFlag == RWFlagR {
				prwLoadType = PrwLoadTypeRaftLogRead
			} else {
				prwLoadType = PrwLoadTypeRaftLogWrite
			}
			goto FAST_PATH
		}
		if req.fileType == FileTypeRocksdbLog {
			fastPathFlag = true
			if req.rwFlag == RWFlagR {
				prwLoadType = PrwLoadTypeRocksdbLogRead
			} else {
				prwLoadType = PrwLoadTypeRocksdbLogWrite
			}
			goto FAST_PATH
		}
		if req.fileType == FileTypeOther {
			prwLoadType = PrwLoadTypeOtherRW
			fastPathFlag = true
			goto FAST_PATH
		}
	}
	if req.rwFlag == RWFlagR {
		if s.sstReadIOIdentifier.IsRandomOrSequencial(req) {
			prwLoadType = PrwLoadTypeRandSmallRead
			fastPathFlag = true
			goto FAST_PATH
		} else {
			prwLoadType = PrwLoadTypeSequentialRead
			fastPathFlag = false
			return
		}
	} else {
		// the identification here is not accurate, but since all write which not on logs is always on q1
		// so we actually don't need to do such identification at all
		if req.sz >= s.sstReadIOIdentifier.sequencialJudgeThresholdSz {
			prwLoadType = PrwLoadTypeSequentialWrite
			fastPathFlag = false
		} else {
			prwLoadType = PrwLoadTypeRandSmallWrite
			fastPathFlag = false
		}
		return
	}
	return
FAST_PATH:
	s.sendDebtToSched(req, prwLoadType)
	return
}

func NewMixIOSched(iodepth, iops, byteps int) *MixIOSched {
	m := &MixIOSched{
		iodepth: iodepth,
		iops:    iops,
		byteps:  byteps,

		fastpathDebtQ:   make(chan MixIOSchedDebtQMember, 1024*512),
		fastpathRefundQ: make(chan MixIOSchedRefundQMember, 1024*512),

		sstReadIOIdentifier: newSSTReadIOIdentifier(10*1024*1024, time.Second*2),

		q1: make(chan MixIOSchedWaitQMember, 1024),
		q2: make(chan MixIOSchedWaitQMember, 1024),
	}
	go m.schedRoutine()
	return m
}

func (s *MixIOSched) schedFastpathCtGet() fastpathCtType {
	var ret fastpathCtType
	s.fastpathCtLock.Lock()
	ret = s.fastpathCt
	s.fastpathCtLock.Unlock()
	return ret
}

func (s *MixIOSched) schedFastpathCtAdd(incr fastpathCtType) fastpathCtType {
	var ret fastpathCtType
	s.fastpathCtLock.Lock()
	s.fastpathCt.Add(incr)
	ret = s.fastpathCt
	s.fastpathCtLock.Unlock()
	return ret
}

func (s *MixIOSched) tryToConsumAllDebtAndRefundQ() (newDebtCt, newRefundCt int, fastpathCt fastpathCtType) {
LOOP:
	select {
	case debt := <-s.fastpathDebtQ:
		if debt.req.fileType != FileTypeOther {
			newDebtCt++
			fastpathCt.reqSendCt++
			fastpathCt.reqSendBytesCt += debt.req.sz
		}
		goto LOOP
	case refund := <-s.fastpathRefundQ:
		if refund.req.fileType != FileTypeOther {
			newRefundCt++
			fastpathCt.reqFinishedCt++
			fastpathCt.reqFinishedBytesCt += refund.req.sz
		}
		goto LOOP
	default:
		goto END
	}
END:
	if newDebtCt > 0 || newRefundCt > 0 {
		fastpathCt = s.schedFastpathCtAdd(fastpathCt)
		// NOTE: this assert panic under some cases, there maybe sth. wrong
		//assert(fastpathCt.reqFinishedCt <= fastpathCt.reqSendCt, "fastpathCt.reqFinishedCt <= fastpathCt.reqSendCt")
		//assert(fastpathCt.reqFinishedBytesCt <= fastpathCt.reqSendBytesCt, "fastpathCt.reqFinishedBytesCt <= fastpathCt.reqSendBytesCt")
	}
	return
}

func (s *MixIOSched) consumOneDebtOrRefundQ() (newDebtCt, newRefundCt int, fastpathCt fastpathCtType) {
LOOP:
	select {
	case debt := <-s.fastpathDebtQ:
		if debt.req.fileType != FileTypeOther {
			newDebtCt++
			fastpathCt.reqSendCt++
			fastpathCt.reqSendBytesCt += debt.req.sz
			goto END
		}
		goto LOOP
	case refund := <-s.fastpathRefundQ:
		if refund.req.fileType != FileTypeOther {
			newRefundCt++
			fastpathCt.reqFinishedCt++
			fastpathCt.reqFinishedBytesCt += refund.req.sz
			goto END
		}
		goto LOOP
	}
END:
	assert(newDebtCt > 0 || newRefundCt > 0, "newDebtCt > 0 || newRefundCt > 0")
	if newDebtCt > 0 || newRefundCt > 0 {
		fastpathCt = s.schedFastpathCtAdd(fastpathCt)
		//assert(fastpathCt.reqFinishedCt <= fastpathCt.reqSendCt, "fastpathCt.reqFinishedCt <= fastpathCt.reqSendCt")
		//assert(fastpathCt.reqFinishedBytesCt <= fastpathCt.reqSendBytesCt, "fastpathCt.reqFinishedBytesCt <= fastpathCt.reqSendBytesCt")
	}
	return
}

func (s *MixIOSched) schedTryToPickOneTaskFromWaitQ1OrQ2() *MixIOSchedWaitQMember {
	if len(s.q1) > 0 {
		m := <-s.q1
		return &m
	}
	if len(s.q2) > 0 {
		m := <-s.q2
		return &m
	}
	return nil
}

func GenMemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	return fmt.Sprintf("Alloc %v TotalAlloc %v Sys = %v NumGC %v", bToMb(m.Alloc), bToMb(m.TotalAlloc), bToMb(m.Sys), m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func bToKB(b int) int {
	return b / 1024
}

func (s *MixIOSched) q2SchedRoutine() {
	// var perSSTMaxReadMBPS
	// dump overallQ2MBPS
	// todo: gc
	perSSTMaxReadMBPS := 1024 * 1024 // 1MB/s
	nowTs := time.Now().Unix()
	bytesInThisTs := 0

	// (map[string]map[int64]int
	//     fileName     ts   bytesSubmitted
	fileNameMapToIOHistory := make(map[string]map[int64]int)

	epochTicker := time.NewTicker(time.Second)
	defer epochTicker.Stop()
	var backLog []MixIOSchedWaitQMember

	handleTask := func(task MixIOSchedWaitQMember, nowTs int64) bool {
		unitMap, ok := fileNameMapToIOHistory[task.req.fileName]
		if ok == false {
			m := make(map[int64]int)
			m[nowTs] = task.req.sz
			fileNameMapToIOHistory[task.req.fileName] = m
			close(task.closeNitifyCh)
			return true
		}
		if len(unitMap) > 1 {
			for k := range unitMap {
				if k != nowTs {
					delete(unitMap, k)
				}
			}
		}
		oldBytes := unitMap[nowTs]
		if oldBytes < perSSTMaxReadMBPS {
			unitMap[nowTs] = oldBytes + task.req.sz
			close(task.closeNitifyCh)
			return true
		}
		return false
	}
	for {
		select {
		case <-epochTicker.C:
			{
				bytesInPrevTs := bytesInThisTs
				prevBacklogLen := len(backLog)
				nowTs++
				bytesInThisTs = 0
				var newBackLog []MixIOSchedWaitQMember
				for _, task := range backLog {
					if handleTask(task, nowTs) {
						bytesInThisTs += task.req.sz
					} else {
						newBackLog = append(newBackLog, task)
					}
				}
				backLog = newBackLog
				fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"), "sched q2", "PrevKB", bToKB(bytesInPrevTs),
					"BackLogBurstKB", bToKB(bytesInThisTs), "BurstBackLogLen", prevBacklogLen-len(backLog), "BackLogLen", len(backLog))
			}
		case task := <-s.q2:
			if handleTask(task, nowTs) {
				bytesInThisTs += task.req.sz
			} else {
				backLog = append(backLog, task)
			}
		}
		//m := <-s.q2
		//if dumpFlag {
		//fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"), "nosched q2", m.req, m.prwLoadType)
		//}
		//close(m.closeNitifyCh)
	}
}

func (s *MixIOSched) schedRoutine() {
	// priority (high to low)
	//    q0 (current is fast path) logs and others, rand small read
	//    q1 rand small write, sequential write
	//    q2 sequential read
	go func() {
		time.Sleep(377 * time.Millisecond)
		oldCt := s.schedFastpathCtGet()
		for {
			time.Sleep(time.Second)
			newCt := s.schedFastpathCtGet()
			fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"),
				"fastpath speed-MB-sent/finish.",
				fmt.Sprintf("%.3f", float64(newCt.reqSendBytesCt-oldCt.reqSendBytesCt)/float64(1024*1024)),
				fmt.Sprintf("%.3f", float64(newCt.reqFinishedBytesCt-oldCt.reqFinishedBytesCt)/float64(1024*1024)),
				"tps-sent/finish.",
				fmt.Sprintf("%d", (newCt.reqSendCt-oldCt.reqSendCt)),
				fmt.Sprintf("%d", (newCt.reqFinishedCt-oldCt.reqFinishedCt)),
				"slowpathLeft", len(s.q1)+len(s.q2), GenMemUsage())
			oldCt = newCt
		}
	}()
	//if disableSchedFlag {
	//	goto BYPASS
	//}
	if true { // current only choose the simple q2 scheduling to prove our basic idea
		goto BYPASS
	}
	// abondoned
	/*
		{
			epochDuration := 100 * time.Millisecond
			iops := s.iops
			bytesPerSec := s.byteps
			epochIOTimes := int(float64(epochDuration) / float64(time.Second) * float64(iops))
			epochBytes := int(float64(epochDuration) / float64(time.Second) * float64(bytesPerSec))

			var prevFastpathCt fastpathCtType
			myBytesQuota := epochBytes / 3
			myIOQuota := epochIOTimes / 3
			myBytesAvailable := myBytesQuota
			myIOCtAvailable := myIOQuota

			if dumpFlag {
				fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"), "mysched", iops, bytesPerSec, epochIOTimes, epochBytes, myIOCtAvailable, myBytesAvailable)
			}

			epochTicker := time.NewTicker(epochDuration)
			defer epochTicker.Stop()
			lastTimeQSchedRun := time.Now()
			for {
				<-epochTicker.C
				_, _, fastpathCt := s.tryToConsumAllDebtAndRefundQ()
				if fastpathCt.IsBalanced() && fastpathCt.IsEqual(prevFastpathCt) {
					if dumpFlag {
						fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"),
							"mysched runQ1&Q2", myIOCtAvailable, myBytesAvailable, time.Since(lastTimeQSchedRun))
					}
					// last epoch no fastpath req was sent
					// so we could try to process the req in q1 & q2
					consumeQMCt := 0
					qExhaustedFlag := false
					for {
						if myIOCtAvailable <= 0 || myBytesAvailable <= 0 {
							break
						}
						m := s.schedTryToPickOneTaskFromWaitQ1OrQ2()
						if m == nil {
							qExhaustedFlag = true
							break
						}
						ioCostCt, bytesCostCt := m.req.CalcQuotaCost()
						myIOCtAvailable -= ioCostCt
						if myIOCtAvailable < 0 {
							myIOCtAvailable -= 10 * myIOQuota
						}
						if myBytesAvailable < 0 {
							myBytesAvailable -= 10 * myBytesQuota
						}
						myBytesAvailable -= bytesCostCt
						close(m.closeNitifyCh)
						consumeQMCt++
					}
					lastTimeQSchedRun = time.Now()
					if dumpFlag {
						fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"),
							"mysched runQ1&Q2end", myIOCtAvailable, myBytesAvailable, fmt.Sprintf("qExhausted:%t", qExhaustedFlag), consumeQMCt, len(s.q1)+len(s.q2))
					}
					myBytesAvailable += myBytesQuota
					if myBytesAvailable > myBytesQuota {
						myBytesAvailable = myBytesQuota
					}
					myIOCtAvailable += myIOQuota
					if myIOCtAvailable > myIOQuota {
						myIOCtAvailable = myIOQuota
					}
				} else { // no chance to run q1 & q2, so we do some quota recovery
					myBytesAvailable += myBytesQuota
					if myBytesAvailable > myBytesQuota {
						myBytesAvailable = myBytesQuota
					}
					myIOCtAvailable += myIOQuota
					if myIOCtAvailable > myIOQuota {
						myIOCtAvailable = myIOQuota
					}
				}
				prevFastpathCt = fastpathCt
			}
		}
	*/
BYPASS:
	go func() {
		for {
			s.consumOneDebtOrRefundQ()
			s.tryToConsumAllDebtAndRefundQ()
		}
	}()
	go func() {
		for {
			m := <-s.q1
			close(m.closeNitifyCh)
		}
	}()
	if disableSchedFlag { // display q2 task info if nosched
		bytesCt := 0
		q2PassthroughTicker := time.NewTicker(time.Second)
		defer q2PassthroughTicker.Stop()
		for {
			select {
			case <-q2PassthroughTicker.C:
				if bytesCt > 0 {
					fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"), "nosched q2", "KB/s", bToKB(bytesCt))
				}
				bytesCt = 0
			case m := <-s.q2:
				bytesCt += m.req.sz
				//}
				close(m.closeNitifyCh)
			}
		}
		panic("unexpected")
	}
	// q2 sched
	s.q2SchedRoutine()
	panic("unexpected")
	// passthrough with bytes/s indicator
	for {
		m := <-s.q2
		close(m.closeNitifyCh)
	}
	select {}
	for {
		time.Sleep(time.Second)
	}
}

func (s *MixIOSched) sendDebtToSched(req PrwTaskRequest, prwLoadType string) {
	s.fastpathDebtQ <- MixIOSchedDebtQMember{
		req, prwLoadType,
	}
}

func (s *MixIOSched) sendRefundToSched(req PrwTaskRequest, leftBytes int) {
	s.fastpathRefundQ <- MixIOSchedRefundQMember{
		req, leftBytes,
	}
}

func (s *MixIOSched) submitPrwTaskRequestToWaitQ(req PrwTaskRequest, prwLoadType string) (readyNotifyCh chan struct{}) {

	ch := make(chan struct{})
	member := MixIOSchedWaitQMember{
		req:           req,
		prwLoadType:   prwLoadType,
		closeNitifyCh: ch,
	}
	if prwLoadType == PrwLoadTypeRandSmallWrite || prwLoadType == PrwLoadTypeSequentialWrite {
		s.q1 <- member
	} else if prwLoadType == PrwLoadTypeSequentialRead {
		s.q2 <- member
	} else {
		panic("unexpected")
	}
	return ch
}

// call me in prw pre cb
func (s *MixIOSched) SubmitPrwTaskRequestAndWaitUntilReady(req PrwTaskRequest) (fastpathFlag bool) {
	typ, ok := s.judgePrwTaskRequest(req)
	if ok {
		return true
	}
	assert(req.fileType == FileTypeRocksdbSST, "req.fileType == FileTypeRocksdbSST")
	<-s.submitPrwTaskRequestToWaitQ(req, typ)
	return false
}

// left bytes means the amount of the bytes which in request but not actually read/write after syscall returned
// eg. read(fd, buf, sz=10) return 1, then leftBytes is 9 = 10 - 1
func (s *MixIOSched) PrwTaskPostCallback(req PrwTaskRequest, leftBytes int, fastpathFlag bool) {
	assert(leftBytes >= 0, "leftBytes >= 0")
	if fastpathFlag {
		s.sendRefundToSched(req, leftBytes)
	}
	if req.fileType == FileTypeRocksdbSST && req.rwFlag == RWFlagR {
		s.sstReadIOIdentifier.AddNewFinishedIO(req)
	}
	return
}
