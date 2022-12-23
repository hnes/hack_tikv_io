package main

import "C"

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// go build -o hack_go.so -buildmode=c-shared hack_go.go

var count int
var mtx sync.Mutex

var dumpFlag = false
var disableSchedFlag = true
var disableSyncMapFlag = false
var dumpPrefixStr = "hack>>> "

const FileTypeNotExist = "NotExist"
const FileTypeOther = "Other"
const FileTypeRaftLog = "Raft"
const FileTypeRocksdbLog = "RocksLog"
const FileTypeRocksdbSST = "SST"

const RWFlagR = 0
const RWFlagW = 1

var glWrCtLock sync.Mutex
var glWrCt int

var glRdCtLock sync.Mutex
var glRdCt int

var fdToPathSyncMap sync.Map
var currentPid int

var glMyHackSched *MixIOSched

type fdToPathSyncMapValue struct {
	fileType string
	path     string
}

func (v *fdToPathSyncMapValue) String() string {
	return fmt.Sprintf("[%s,%s]", v.fileType, v.path)
}

func addGlRdCt(ct int) {
	glRdCtLock.Lock()
	defer glRdCtLock.Unlock()
	glRdCt += ct
}

func addGlWrCt(ct int) {
	glWrCtLock.Lock()
	defer glWrCtLock.Unlock()
	glWrCt += ct
}

func getGlWrCt() int {
	var ct int
	glWrCtLock.Lock()
	defer glWrCtLock.Unlock()
	ct = glWrCt
	return ct
}

func getGlRdCt() int {
	var ct int
	glRdCtLock.Lock()
	defer glRdCtLock.Unlock()
	ct = glRdCt
	return ct
}

func init() {
	preWCt := 0
	preRCt := 0
	currentPid = os.Getgid()
	glMyHackSched = NewMixIOSched(64, 3000, 128*1024*1024)
	go func() {
		for {
			time.Sleep(time.Second)
			wct := getGlWrCt()
			rct := getGlRdCt()
			fmt.Println(dumpPrefixStr, time.Now().Format("2006-01-02 15:04:05"),
				"speed-MB-w/r",
				fmt.Sprintf("%.3f", float64(wct-preWCt)/float64(1024*1024)),
				fmt.Sprintf("%.3f", float64(rct-preRCt)/float64(1024*1024)),
				"accum-MB-w/r",
				fmt.Sprintf("%.3f", float64(wct)/float64(1024*1024)),
				fmt.Sprintf("%.3f", float64(rct)/float64(1024*1024)))
			preWCt = wct
			preRCt = rct
		}
	}()
}

func getMySched() *MixIOSched {
	assert(glMyHackSched != nil, "glMyHackSched!=nil")
	return glMyHackSched
}

func assert(b bool, str string) {
	if b {
	} else {
		panic(str)
	}
}

func getFileTypeFromAbsoluteFilePath(name string) (fileType string) {
	r := name
	if len(r) <= len("/mnt/") {
		return FileTypeOther
	} else {
		if r[:5] != "/mnt/" {
			return FileTypeOther
		}
		match := ".sst"
		assert(len(match)+5 < len(r), r)
		if r[(len(r)-len(match)):] == match {
			return FileTypeRocksdbSST
		}
		match = ".raftlog"
		assert(len(match)+5 < len(r), r)
		if r[(len(r)-len(match)):] == match {
			return FileTypeRaftLog
		}
		match = ".rewrite"
		assert(len(match)+5 < len(r), r)
		if r[(len(r)-len(match)):] == match {
			return FileTypeRaftLog
		}
		if strings.Contains(r, "/db/") {
			return FileTypeRocksdbLog
		}
		if strings.Contains(r, "/raft-engine/") {
			return FileTypeRaftLog
		}
		return FileTypeOther
	}
}

func getFdRealPath(fd int) (fileType string, path string) {
	r, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd))
	if err != nil {
		panic(err)
	}
	return getFileTypeFromAbsoluteFilePath(r), r
}

func getfdToPathSyncMapValueAndSetIfNotExist(fd int) fdToPathSyncMapValue {
	v, ok := fdToPathSyncMap.Load(fd)
	if ok && v != nil {
		return v.(fdToPathSyncMapValue)
	}
	var nv fdToPathSyncMapValue
	nv.fileType, nv.path = getFdRealPath(fd)
	fdToPathSyncMap.Store(fd, v)
	return nv
}

func mustGetfdToPathSyncMapValue(fd int) fdToPathSyncMapValue {
	v, ok := fdToPathSyncMap.Load(fd)
	if ok {
		return v.(fdToPathSyncMapValue)
	}
	panic("not found fd")
}

func tryGetfdToPathSyncMapValue(fd int) fdToPathSyncMapValue {
	v, ok := fdToPathSyncMap.Load(fd)
	if ok && v != nil {
		return v.(fdToPathSyncMapValue)
	}
	return fdToPathSyncMapValue{
		FileTypeNotExist, "Null",
	}
}

func tryGetfdToPathSyncMapValueAndGetRealPathIfNotExist(fd int) string {
	v := tryGetfdToPathSyncMapValue(fd)
	if v.fileType == FileTypeNotExist {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
		fdToPathSyncMap.Delete(fd)
		return fmt.Sprintf("NotExist %s", v)
	} else {
		return v.String()
	}
}

//export HackGo_open_cb
func HackGo_open_cb(tid int, path string, fd int) {
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "open", path, fd)
	}
	if disableSyncMapFlag {
		return
	}
	var v fdToPathSyncMapValue
	v.fileType, v.path = getFdRealPath(fd)
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "open", v)
	}
	fdToPathSyncMap.Store(fd, v)
}

//export HackGo_close_pre_cb
func HackGo_close_pre_cb(tid, fd int) {
	//fmt.Println("open", path, fd)
	var v fdToPathSyncMapValue
	if disableSyncMapFlag {
	} else {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
	}
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "pre close", fd, v)
	}
}

//export HackGo_close_post_cb
func HackGo_close_post_cb(tid, fd int) {
	//fmt.Println("open", path, fd)
	if disableSyncMapFlag {
		if dumpFlag {
			fmt.Println(dumpPrefixStr, tid, "post close", fd)
		}
		return
	}
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "post close", fd, tryGetfdToPathSyncMapValue(fd))
	}
	fdToPathSyncMap.Delete(fd)
}

//export HackGo_rw_pre_cb
func HackGo_rw_pre_cb(tid, rw_flag, fd int, sz int) {
	// rw_flag 0 for r 1 for w
	//fmt.Println("open", path, fd)
	assert(false, "call prw instead")
	var v fdToPathSyncMapValue
	if disableSyncMapFlag {
	} else {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
	}
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "pre", genRWDesc(0, rw_flag), fd, sz, v)
	}
}

//export HackGo_rw_post_cb
func HackGo_rw_post_cb(tid, rw_flag, fd int, sz int) {
	assert(false, "call prw instead")
	//fmt.Println("open", path, fd)
	var v fdToPathSyncMapValue
	if disableSyncMapFlag {
	} else {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
	}

	if rw_flag == 1 && sz > 0 {
		addGlWrCt(sz)
	}
	if rw_flag == 0 && sz > 0 {
		addGlRdCt(sz)
	}
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "post", genRWDesc(0, rw_flag), fd, sz, v)
	}
}

//export HackGo_prw_pre_cb
func HackGo_prw_pre_cb(tid, rw_flag, fd int, offset, sz int) (fastpathFlag01 int) {
	//fmt.Println("open", path, fd)
	var v fdToPathSyncMapValue
	if disableSyncMapFlag {
	} else {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
	}
	t0 := time.Now()
	fastpathFlag := getMySched().SubmitPrwTaskRequestAndWaitUntilReady(PrwTaskRequest{
		fileName: v.path,
		fileType: v.fileType,
		rwFlag:   rw_flag,
		offset:   offset,
		sz:       sz,
	})
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "pre", genRWDesc(1, rw_flag), fd, offset, sz, v, fastpathFlag, time.Since(t0))
	}
	if fastpathFlag {
		return 1
	} else {
		return 0
	}
}

//export HackGo_prw_post_cb
func HackGo_prw_post_cb(tid, rw_flag, fd int, offset, sz, leftBytes, fastPathFlag01 int) {
	var v fdToPathSyncMapValue
	if disableSyncMapFlag {
	} else {
		v = getfdToPathSyncMapValueAndSetIfNotExist(fd)
	}

	//fmt.Println("open", path, fd)
	if rw_flag == 1 && sz > 0 {
		addGlWrCt(sz)
	}
	if rw_flag == 0 && sz > 0 {
		addGlRdCt(sz)
	}
	fastPathFlag := false
	if fastPathFlag01 != 0 {
		fastPathFlag = true
	}
	t0 := time.Now()
	getMySched().PrwTaskPostCallback(PrwTaskRequest{
		fileName: v.path,
		fileType: v.fileType,
		rwFlag:   rw_flag,
		offset:   offset,
		sz:       sz,
	},
		leftBytes, fastPathFlag,
	)
	if dumpFlag {
		fmt.Println(dumpPrefixStr, tid, "post", genRWDesc(1, rw_flag), fd, offset, sz, v, time.Since(t0))
	}
}

func genRWDesc(p, rw_flag int) string {
	var pstr = ""
	if p == 0 {
		pstr = ""
	} else {
		pstr = "p"
	}
	if rw_flag == 0 {
		return pstr + "read"
	} else {
		return pstr + "write"
	}
}

/*
//export HackGo_write_pre_cb
func HackGo_write_pre_cb(fd int, sz int) {
	//fmt.Println("open", path, fd)
}

//export HackGo_write_post_cb
func HackGo_write_post_cb(fd int, sz int) {
	//fmt.Println("open", path, fd)
}

//export HackGo_pwrite_pre_cb
func HackGo_pwrite_pre_cb(fd int, offset, sz int) {
	//fmt.Println("open", path, fd)
}

//export HackGo_pwrite_post_cb
func HackGo_pwrite_post_cb(fd int, offset, sz int) {
	//fmt.Println("open", path, fd)
}*/

func main() {}
