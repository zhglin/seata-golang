package uuid

import (
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"

	time2 "github.com/opentrx/seata-golang/v2/pkg/util/time"
)

const (
	// Start time cut (2020-05-03)
	epoch uint64 = 1588435200000

	// The number of bits occupied by workerID
	// workerID占用的位数
	workerIDBits = 10

	// The number of bits occupied by timestamp
	// 时间戳占的位数
	timestampBits = 41

	// The number of bits occupied by sequence
	// 序列所占用的比特数
	sequenceBits = 12

	// Maximum supported machine id, the result is 1023
	// 支持的最大机器id，结果是1023，10位的最大值
	maxWorkerID = -1 ^ (-1 << workerIDBits)

	// mask that help to extract timestamp and sequence from a long
	// 53位的最大值, bit全都为1
	timestampAndSequenceMask uint64 = -1 ^ (-1 << (timestampBits + sequenceBits))
)

// timestamp and sequence mix in one Long
// highest 11 bit: not used
// middle  41 bit: timestamp
// lowest  12 bit: sequence
//									时间戳                            序列号
// 00000000 000 (00000 00000000 00000000 00000000 00000000 0000) (0000 00000000)
var timestampAndSequence uint64 = 0

// business meaning: machine ID (0 ~ 1023)
// actual layout in memory:
// highest 1 bit: 0
// middle 10 bit: workerID
// lowest 53 bit: all 0
//     workerID
// 0 (0000000 000) 00000 00000000 00000000 00000000 00000000 00000000 00000000
var workerID = generateWorkerID() << (timestampBits + sequenceBits)

func init() {
	timestamp := getNewestTimestamp()
	timestampWithSequence := timestamp << sequenceBits
	// 写入当前时间
	atomic.StoreUint64(&timestampAndSequence, timestampWithSequence)
}

// Init 更新workerId
func Init(serverNode int64) error {
	if serverNode > maxWorkerID || serverNode < 0 {
		return fmt.Errorf("worker id can't be greater than %d or less than 0", maxWorkerID)
	}
	workerID = serverNode << (timestampBits + sequenceBits)
	return nil
}

func NextID() int64 {
	waitIfNecessary()
	next := atomic.AddUint64(&timestampAndSequence, 1)
	timestampWithSequence := next & timestampAndSequenceMask // 防止bit数溢出
	return int64(uint64(workerID) | timestampWithSequence) // 加上workerId
}

func waitIfNecessary() {
	currentWithSequence := atomic.LoadUint64(&timestampAndSequence)
	current := currentWithSequence >> sequenceBits // 时间戳
	newest := getNewestTimestamp() // 当前的的时间戳
	for current >= newest {
		newest = getNewestTimestamp()
	}
}

// get newest timestamp relative to twepoch
// 获得最新的时间戳相对于twepoch
func getNewestTimestamp() uint64 {
	return time2.CurrentTimeMillis() - epoch
}

// auto generate workerID, try using mac first, if failed, then randomly generate one
// 自动生成workerID，首先尝试使用mac，如果失败，然后随机生成一个
func generateWorkerID() int64 {
	id, err := generateWorkerIDBaseOnMac()
	if err != nil {
		id = generateRandomWorkerID()
	}
	return id
}

// use lowest 10 bit of available MAC as workerID
// 使用可用MAC(第一个活动状态的非环回接口)的最低10位作为workerID
func generateWorkerIDBaseOnMac() (int64, error) {
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		// 非flagUp的接口 (不是活动状态)
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}

		// 是flagLoopback的接口 (是环回地址)
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}

		// 硬件地址
		mac := iface.HardwareAddr

		// 0b-2进制 0x-16进制
		return int64(int((mac[4]&0b11)<<8) | int(mac[5]&0xFF)), nil
	}
	return 0, fmt.Errorf("no available mac found")
}

// randomly generate one as workerID
// 随机生成workerID
func generateRandomWorkerID() int64 {
	return rand.Int63n(maxWorkerID + 1)
}
