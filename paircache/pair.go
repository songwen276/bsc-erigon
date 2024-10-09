package paircache

import (
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/paircache/gopool"
	"github.com/erigontech/erigon/paircache/mysqldb"
	"github.com/erigontech/erigon/paircache/pairtypes"
	"github.com/jmoiron/sqlx"

	cmap "github.com/orcaman/concurrent-map"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	TriangleChannel     = make(chan []*pairtypes.ITriangularArbitrageTriangular, 100000)
	DoneChannel         = make(chan struct{})
	stateObjectCacheMap = cmap.New()
	storageCacheMap     = cmap.New()
	pairCache           = pairtypes.NewPairCache()
	abiStr              = "[{\"inputs\":[],\"name\":\"arb_wcnwzblucpyf\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"token0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair2\",\"type\":\"address\"}],\"internalType\":\"structITriangularArbitrage.Triangular\",\"name\":\"t\",\"type\":\"tuple\"},{\"internalType\":\"uint256\",\"name\":\"startRatio\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"endRatio\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"pieces\",\"type\":\"uint256\"}],\"name\":\"arbitrageQuery\",\"outputs\":[{\"internalType\":\"int256[]\",\"name\":\"roi\",\"type\":\"int256[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"token0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair2\",\"type\":\"address\"}],\"internalType\":\"structITriangularArbitrage.Triangular\",\"name\":\"t\",\"type\":\"tuple\"},{\"internalType\":\"uint256\",\"name\":\"threshold\",\"type\":\"uint256\"}],\"name\":\"isTriangularValid\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"
	ABI                 *abi.ABI
	From                = common.HexToAddress("0xcdecF7Ab7c6654139F65c6C1C7Ecbad653F0dfB0")
	To                  = common.HexToAddress("0x84F7f6016e5ED7819f717994225D4f60c7Af5359")
)

func init() {
	// 初始化triange到内存
	triangleStart := time.Now()
	fetchTriangleMap()
	fmt.Printf("初次加载triange到内存中耗时：%v，共加载%v条，加载pair共%v条\n", time.Since(triangleStart), pairCache.TriangleMapSize(), pairCache.PairTriangleMapSize())

	// 初始化topic到内存
	topicStart := time.Now()
	fetchTopicMap()
	fmt.Printf("初次加载topic到内存中耗时：%v，共加载%v条\n", time.Since(topicStart), len(pairCache.TopicMap))

	// 开启协程周期更新内存中triange与topic
	err := gopool.Submit(timerGetTriangle)
	if err != nil {
		fmt.Printf("开启定时加载Triangle任务失败，err=%v\n", err)
		return
	}
	err = gopool.Submit(timerGetTopic)
	if err != nil {
		fmt.Printf("开启定时加载Topic任务失败，err=%v\n", err)
		return
	}

	// 加载三角合约abi
	if parsed, err := abi.JSON(strings.NewReader(abiStr)); err != nil {
		fmt.Printf("加载三角合约abi失败，err=%v\n", err)
		return
	} else {
		ABI = &parsed
	}
	fmt.Printf("初次加载三角合约abi到内存中成功：%v\n", *ABI)

}

func GetPairControl() *pairtypes.PairCache {
	return pairCache
}

func GetStateObjectCacheMap() cmap.ConcurrentMap {
	return stateObjectCacheMap
}

func GetStorageCacheMap() cmap.ConcurrentMap {
	return storageCacheMap
}

func timerGetTriangle() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fetchTriangleMap()
		}
	}
}

func timerGetTopic() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fetchTopicMap()
		}
	}
}

func fetchTopicMap() {
	// 读取文件内容
	start := time.Now()
	fileContent, err := os.ReadFile("/root/topic.json")
	if err != nil {
		log.Error("Failed to read file", "err", err)
	}

	// 解析 JSON 文件内容到 map
	newTopicMap := make(map[string]string)
	err = json.Unmarshal(fileContent, &newTopicMap)
	if err != nil {
		log.Error("Failed to unmarshal JSON", "err", err)
	}
	pairCache.TopicMap = newTopicMap
	log.Info("刷新内存中topic耗时", "time", time.Since(start), "topic总数", len(newTopicMap))
}

func fetchTriangleMap() {
	// 初始化数据库连接
	printMemUsed()
	start := time.Now()
	mysqlDB := mysqldb.GetMysqlDB()

	// 使用流式查询，逐行处理数据
	rows, err := mysqlDB.Queryx("select id, token0, router0, pair0, token1, router1, pair1, token2, router2, pair2 from arbitrage_triangle order by id")
	if err != nil {
		log.Error("查询失败", "err", err)
	}
	defer func(rows *sqlx.Rows) {
		err := rows.Close()
		if err != nil {
			log.Error("流式查询关闭rows失败", "err", err)
		}
	}(rows)

	// 遍历查询结果
	for rows.Next() {
		triangle := pairtypes.Triangle{}
		err := rows.StructScan(&triangle)
		if err != nil {
			log.Error("填充结果到结构体失败", "err", err)
		}
		triangle.Pair0 = common.HexToAddress(triangle.Pair0).Hex()
		triangle.Pair1 = common.HexToAddress(triangle.Pair1).Hex()
		triangle.Pair2 = common.HexToAddress(triangle.Pair2).Hex()
		id := strconv.FormatInt(triangle.ID, 10)
		pairCache.AddTriangle(id, triangle)
		pairCache.AddPairTriangle(triangle.Pair0, id)
		pairCache.AddPairTriangle(triangle.Pair1, id)
		pairCache.AddPairTriangle(triangle.Pair2, id)
	}

	// 检查是否有遍历中的错误
	if err := rows.Err(); err != nil {
		log.Error("查询失败", "err", err)
	}
	log.Info("刷新内存中triange耗时", "time", time.Since(start), "triange总数", pairCache.TriangleMapSize(), "pair总数", pairCache.PairTriangleMapSize())
	printMemUsed()
}

func printMemUsed() {
	// 读取 /proc/meminfo 文件
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		fmt.Printf("Error reading /proc/meminfo：%v\n", err)
		return
	}

	// 解析内容
	lines := strings.Split(string(data), "\n")
	memInfo := make(map[string]int64)

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.Trim(fields[0], ":")
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err == nil {
			memInfo[key] = value
		}
	}

	// 计算总缓存内存
	totalCache := memInfo["Buffers"] + memInfo["Cached"]

	// 输出总内存、空闲内存、可用内存和总缓存内存
	fmt.Printf("Total RAM: %d MB\n", memInfo["MemTotal"]/1024)
	fmt.Printf("Free RAM: %d MB\n", memInfo["MemFree"]/1024)
	fmt.Printf("Available RAM: %d MB\n", memInfo["MemAvailable"]/1024)
	fmt.Printf("Total Cached RAM (Buffers + Cached): %d MB\n", totalCache/1024)
}

func Encoder(name string, args ...interface{}) ([]byte, error) {
	return ABI.Pack(name, args...)
}
