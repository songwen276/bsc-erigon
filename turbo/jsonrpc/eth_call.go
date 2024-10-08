// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/erigontech/erigon/paircache"
	"github.com/erigontech/erigon/paircache/gopool"
	"github.com/erigontech/erigon/paircache/pairtypes"
	"github.com/ethereum/go-ethereum/common"
	solsha3 "github.com/miguelmota/go-solidity-sha3"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/gointerfaces"
	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

var latestNumOrHash = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

func workerTest(s *APIImpl, results chan<- interface{}, triangular *pairtypes.ITriangularArbitrageTriangular) {
	// 设置上下文，用于控制每个任务方法执行超时时间
	ctx := context.Background()
	param := getArbitrageQueryParam(big.NewInt(0), 0, 10000)
	rois, err := getRoisTest(s, triangular, param, ctx)
	log.Info("10000step", "start", param.Start, "end", param.End, "step", param.Pieces, "rois", rois)
	if err != nil {
		results <- err
		return
	}

	index := resolveROI(rois)
	param = getArbitrageQueryParam(param.Start, index, 1000)
	rois, err = getRoisTest(s, triangular, param, ctx)
	log.Info("1000step", "start", param.Start, "end", param.End, "step", param.Pieces, "rois", rois)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)

	param = getArbitrageQueryParam(param.Start, index, 100)
	rois, err = getRoisTest(s, triangular, param, ctx)
	log.Info("100step", "start", param.Start, "end", param.End, "step", param.Pieces, "rois", rois)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)

	param = getArbitrageQueryParam(param.Start, index, 10)
	rois, err = getRoisTest(s, triangular, param, ctx)
	log.Info("10step", "start", param.Start, "end", param.End, "step", param.Pieces, "rois", rois)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)
	point := new(big.Int).Add(param.Start, big.NewInt(int64(index)))
	if point.Cmp(big.NewInt(0)) == 0 {
		results <- nil
		return
	}
	param.Start = point
	param.End = point
	param.Pieces = big.NewInt(1)

	rois, err = getRoisTest(s, triangular, param, ctx)
	log.Info("point", "start", param.Start, "end", param.End, "step", param.Pieces, "rois", rois)
	if err != nil {
		results <- err
		return
	}

	if rois == nil || rois[13] == nil || rois[13].Cmp(big.NewInt(5000000)) < 0 {
		results <- nil
		return
	}

	snapshotsHash := solsha3.SoliditySHA3(solsha3.Int256(rois[3]), solsha3.Int256(rois[4]), solsha3.Int256(rois[5]))
	subHex := hex.EncodeToString(snapshotsHash)[0:2]

	parameters := []interface{}{
		hex.EncodeToString(solsha3.Uint32(big.NewInt(0))),
		subHex,
		common.BigToAddress(rois[0]),
		getWei(rois[6], 96),
		common.BigToAddress(rois[1]),
		getWei(rois[7], 96),
		common.BigToAddress(rois[2]),
		getWei(rois[10], 96),
		triangular.Token0,
		getWei(rois[11], 96),
		triangular.Pair0,
		getWei(rois[12], 96),
		triangular.Token1,
		getWei(rois[13], 96),
		triangular.Pair1,
		triangular.Token2,
		triangular.Pair2,
	}

	calldata, err := EncodePackedBsc(parameters)
	if err != nil {
		results <- err
		return
	}
	log.Info("编码calldata成功", "calldata", calldata)
	ROI := &ROI{
		TriangularEntity: *triangular,
		CallData:         calldata,
		Profit:           *rois[13],
	}

	results <- ROI
	return
}

func pairWorker(s *APIImpl, results chan<- interface{}, triangular *pairtypes.ITriangularArbitrageTriangular) {
	// 设置上下文，用于控制每个任务方法执行超时时间
	ctx := context.Background()
	param := getArbitrageQueryParam(big.NewInt(0), 0, 10000)
	rois, err := getRois(s, triangular, param, ctx)
	if err != nil {
		results <- err
		return
	}

	index := resolveROI(rois)
	param = getArbitrageQueryParam(param.Start, index, 1000)
	rois, err = getRois(s, triangular, param, ctx)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)

	param = getArbitrageQueryParam(param.Start, index, 100)
	rois, err = getRois(s, triangular, param, ctx)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)

	param = getArbitrageQueryParam(param.Start, index, 10)
	rois, err = getRois(s, triangular, param, ctx)
	if err != nil {
		results <- err
		return
	}
	index = resolveROI(rois)
	point := new(big.Int).Add(param.Start, big.NewInt(int64(index)))
	if point.Cmp(big.NewInt(0)) == 0 {
		results <- nil
		return
	}
	param.Start = point
	param.End = point
	param.Pieces = big.NewInt(1)

	rois, err = getRois(s, triangular, param, ctx)
	if err != nil {
		results <- err
		return
	}

	if rois == nil || rois[13] == nil || rois[13].Cmp(big.NewInt(5000000)) < 0 {
		results <- nil
		return
	}

	snapshotsHash := solsha3.SoliditySHA3(solsha3.Int256(rois[3]), solsha3.Int256(rois[4]), solsha3.Int256(rois[5]))
	subHex := hex.EncodeToString(snapshotsHash)[0:2]

	parameters := []interface{}{
		hex.EncodeToString(solsha3.Uint32(big.NewInt(0))),
		subHex,
		common.BigToAddress(rois[0]),
		getWei(rois[6], 96),
		common.BigToAddress(rois[1]),
		getWei(rois[7], 96),
		common.BigToAddress(rois[2]),
		getWei(rois[10], 96),
		triangular.Token0,
		getWei(rois[11], 96),
		triangular.Pair0,
		getWei(rois[12], 96),
		triangular.Token1,
		getWei(rois[13], 96),
		triangular.Pair1,
		triangular.Token2,
		triangular.Pair2,
	}

	calldata, err := EncodePackedBsc(parameters)
	if err != nil {
		results <- err
		return
	}
	log.Info("编码calldata成功", "calldata", calldata)

	ROI := &ROI{
		TriangularEntity: *triangular,
		CallData:         calldata,
		Profit:           *rois[13],
	}

	results <- ROI
	return
}

type Wei struct {
	BitSize int
	Data    string
}

func getWei(roi *big.Int, bitSize int) *Wei {
	return &Wei{
		BitSize: bitSize,
		Data:    hex.EncodeToString(solsha3.Int256(roi)),
	}
}

type ROI struct {
	TriangularEntity pairtypes.ITriangularArbitrageTriangular
	CallData         string
	Profit           big.Int
}

func getRois(s *APIImpl, triangular *pairtypes.ITriangularArbitrageTriangular, param *ArbitrageQueryParam, ctx context.Context) ([]*big.Int, error) {
	data, _ := paircache.Encoder("arbitrageQuery", triangular, param.Start, param.End, param.Pieces)
	bytes := hexutility.Bytes(data)
	args := ethapi2.CallArgs{From: &paircache.From, To: &paircache.To, Data: &bytes}
	call, err := s.Call(ctx, args, latestNumOrHash, nil)
	if err != nil {
		return nil, err
	} else {
		roiStr := hex.EncodeToString(call)
		lenth := len(roiStr) / 64

		// 前两条为偏移量和rois数组长度，不计入rois
		rois := make([]*big.Int, lenth-2)
		for j := 0; j < lenth; j++ {
			subStr := roiStr[64*j : 64*(j+1)]
			if j > 1 {
				roi, _ := new(big.Int).SetString(subStr, 16)
				rois[j-2] = roi
			}
		}
		return rois, err
	}
}

func getRoisTest(s *APIImpl, triangular *pairtypes.ITriangularArbitrageTriangular, param *ArbitrageQueryParam, ctx context.Context) ([]*big.Int, error) {
	data, _ := paircache.Encoder("arbitrageQuery", triangular, param.Start, param.End, param.Pieces)
	bytes := hexutility.Bytes(data)
	args := ethapi2.CallArgs{From: &paircache.From, To: &paircache.To, Data: &bytes}
	call, err := s.Call(ctx, args, latestNumOrHash, nil)
	if err != nil {
		return nil, err
	} else {
		roiStr := hex.EncodeToString(call)
		lenth := len(roiStr) / 64

		// 前两条为偏移量和rois数组长度，不计入rois
		rois := make([]*big.Int, lenth-2)
		for j := 0; j < lenth; j++ {
			subStr := roiStr[64*j : 64*(j+1)]
			log.Info("CallReturn EncodeToString", "roiStr", subStr)
			if j > 1 {
				roi, _ := new(big.Int).SetString(subStr, 16)
				rois[j-2] = roi
			}
		}
		return rois, err
	}
}

type ArbitrageQueryParam struct {
	Start  *big.Int
	End    *big.Int
	Pieces *big.Int
}

func getArbitrageQueryParam(start *big.Int, index, step int) *ArbitrageQueryParam {
	if index >= 10 {
		index = 9
	}
	// 计算 startNew = start + step * index
	stepBigInt := big.NewInt(int64(step))
	indexBigInt := big.NewInt(int64(index))
	startNew := new(big.Int).Add(start, new(big.Int).Mul(stepBigInt, indexBigInt))

	// 计算 end = startNew + step
	end := new(big.Int).Add(startNew, stepBigInt)

	// 返回查询参数
	return &ArbitrageQueryParam{
		Start:  startNew,
		End:    end,
		Pieces: big.NewInt(10), // 相当于 BigInteger.TEN
	}
}

func resolveROI(rois []*big.Int) int {
	var i int
	// 排除rois前6个元素，剩下元素每8个一组，循环每组中首元素判断是否为0
	for i = 0; i < (len(rois)-6)/8; i++ {
		if rois[i*8+6].Cmp(big.NewInt(0)) == 0 {
			return i
		}
	}
	return i
}

func EncodePackedBsc(values []interface{}) (string, error) {
	var encoded string
	for _, value := range values {
		switch v := value.(type) {
		case string:
			encoded = encoded + v
		case *Wei:
			wei := *v
			encoded = encoded + wei.Data[len(wei.Data)-wei.BitSize/4:]
		case common.Address:
			addrStr := v.Hex()[2:]
			encoded = encoded + addrStr
		default:
			return "", fmt.Errorf("unsupported type: %T", value)
		}
	}
	return encoded, nil
}

func worker(s *APIImpl, results chan<- interface{}, args ethapi2.CallArgs) {
	// 设置上下文，用于控制每个任务方法执行超时时间
	ctx := context.Background()
	call, err := s.Call(ctx, args, latestNumOrHash, nil)
	if err != nil {
		results <- err
	} else {
		results <- call
	}
}

type Results struct {
	GetDatasSince time.Duration          `json:"getDatasSince"`
	SelectSince   time.Duration          `json:"selectSince"`
	TotalSince    time.Duration          `json:"totalSince"`
	ResultMap     map[string]interface{} `json:"resultMap"`
}

func GetEthCallData() ([]ethapi2.CallArgs, error) {
	// 打开测试数据文件
	file, err := os.Open("/bc/bsc/build/bin/testdata.json")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer file.Close()

	// 创建一个缓冲读取器
	scanner := bufio.NewScanner(file)

	datas := make([]ethapi2.CallArgs, 0, 10000)
	for scanner.Scan() {
		line := scanner.Text()
		args := ethapi2.CallArgs{}
		// 从目标字符串之后开始提取内容
		index1 := strings.Index(line, "\"params\":[")
		if index1 != -1 {
			// 提取目标字符串之后的内容
			param1 := line[index1+len("\"params\":[") : len(line)-12]
			err := json.Unmarshal([]byte(param1), &args)
			if err != nil {
				return nil, err
			}
		}
		datas = append(datas, args)
	}
	return datas, nil
}

// // CallBatch batch executes Call
// func (api *APIImpl) CallBatch() (string, error) {
// 	// 读取任务测试数据
// 	log.Info("开始执行CallBatch")
// 	datas, err := GetEthCallData()
// 	if err != nil {
// 		return "", err
// 	}
// 	// getDatasSince := time.Since(start)
// 	// log.Info("获取所有测试数据花费时长", "runtime", getDatasSince)
//
// 	// 根据任务数创建结果读取通道
// 	results := make(chan interface{}, len(datas))
//
// 	// 提交任务到协程池，所有协程完成后关闭结果读取通道
// 	start := time.Now()
// 	var wg sync.WaitGroup
// 	for _, job := range datas {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			worker(api, results, job)
// 		}()
// 	}
// 	wg.Wait()
// 	close(results)
// 	selectSince := time.Since(start)
// 	log.Info("所有eth_call查询任务执行完成花费时长", "runtime", selectSince)
//
// 	// 读取任务结果通道数据进行处理
// 	resultMap := make(map[string]interface{}, len(datas))
// 	i := 1
// 	// 处理结果
// 	for result := range results {
// 		itoa := strconv.Itoa(i)
// 		switch v := result.(type) {
// 		case hexutility.Bytes:
// 			bytes := result.(hexutility.Bytes)
// 			enc, err := json.Marshal(bytes)
// 			if err != nil {
// 				resultMap[itoa] = err.Error()
// 			} else {
// 				resultMap[itoa] = enc
// 			}
// 		case error:
// 			resultMap[itoa] = v.Error()
// 		default:
// 			resultMap[itoa] = v
// 		}
// 		i += 1
// 	}
// 	totalSince := time.Since(start)
// 	r := Results{GetDatasSince: 0, SelectSince: selectSince, TotalSince: totalSince, ResultMap: resultMap}
//
// 	// 创建文件
// 	file, err := os.Create("/root/results.json")
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()
//
// 	// 将 map 编码为 JSON
// 	encoder := json.NewEncoder(file)
// 	encoder.SetIndent("", "  ") // 设置缩进格式
// 	if err := encoder.Encode(r); err != nil {
// 		return "", err
// 	}
// 	log.Info("结果输出到文件完成，结束")
// 	return "ok", nil
// }

// CallBatch batch executes Call
func (api *APIImpl) CallBatch() (string, error) {
	// 读取任务测试数据
	api.logger.Info("开始执行CallBatch")
	var triangulars []*pairtypes.ITriangularArbitrageTriangular
	triangular := &pairtypes.ITriangularArbitrageTriangular{
		Token0:  libcommon.HexToAddress("0x0bc89aa98Ad94E6798Ec822d0814d934cCD0c0cE"),
		Router0: libcommon.HexToAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"),
		Pair0:   libcommon.HexToAddress("0x51C5251BF281C6d0eF8ced7a1741FdB68D130ac2"),
		Token1:  libcommon.HexToAddress("0x55d398326f99059fF775485246999027B3197955"),
		Router1: libcommon.HexToAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"),
		Pair1:   libcommon.HexToAddress("0x7EFaEf62fDdCCa950418312c6C91Aef321375A00"),
		Token2:  libcommon.HexToAddress("0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56"),
		Router2: libcommon.HexToAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"),
		Pair2:   libcommon.HexToAddress("0xEE90C67C9dD5dE862F4eabFDd53007a2D95Df5c6"),
	}
	triangulars = append(triangulars, triangular)

	// 初始化构造当前区块公共数据
	start := time.Now()
	results := make(chan interface{}, len(triangulars))

	// 提交任务到协程池，所有协程完成后关闭结果读取通道
	var wg sync.WaitGroup
	for _, triangular := range triangulars {
		wg.Add(1)
		gopool.Submit(func() {
			defer wg.Done()
			workerTest(api, results, triangular)
		})
	}
	wg.Wait()
	close(results)
	selectSince := time.Since(start)
	api.logger.Info("所有eth_call查询任务执行完成花费时长", "runtime", selectSince)

	// 读取任务结果通道数据进行处理
	rois := make([]ROI, 5000)
	resultMap := make(map[string]interface{}, len(triangulars))
	i := 1
	// 处理结果
	for result := range results {
		itoa := strconv.Itoa(i)
		switch v := result.(type) {
		case *ROI:
			rois = append(rois, *v)
		case error:
			resultMap[itoa] = v.Error()
		default:
			resultMap[itoa] = v
		}
		i += 1
	}

	if len(rois) > 0 {
		// 按 Profit 字段对rois进行降序排序
		sort.Slice(rois, func(i, j int) bool {
			return rois[i].Profit.Cmp(&rois[j].Profit) > 0
		})
		api.logger.Info("降序排序生成rois成功", "rois", rois)

		// 将排序后的rois去重过滤，保证每个pair只能出现一次，重复时将Profit较小的ROI都删除，只保留Profit最大的ROI
		// 去重，保证 Pair0, Pair1, Pair2 中的值只出现一次
		uniquePairs := make(map[libcommon.Address]bool)
		var filteredROIs []ROI
		for _, roi := range rois {
			if uniquePairs[roi.TriangularEntity.Pair0] || uniquePairs[roi.TriangularEntity.Pair1] || uniquePairs[roi.TriangularEntity.Pair2] {
				// 如果任何一个 pair 已经出现过，跳过该结构体（删除）
				continue
			}

			// 如果不存在，则将该结构体加入结果集，并标记 pairs 为已出现
			filteredROIs = append(filteredROIs, roi)
			uniquePairs[roi.TriangularEntity.Pair0] = true
			uniquePairs[roi.TriangularEntity.Pair1] = true
			uniquePairs[roi.TriangularEntity.Pair2] = true
		}
		api.logger.Info("排序去重获rois成功", "filteredROIs", filteredROIs)
	}

	totalSince := time.Since(start)
	r := Results{GetDatasSince: 0, SelectSince: selectSince, TotalSince: totalSince, ResultMap: resultMap}

	// 创建文件
	file, err := os.Create("/root/results.json")
	if err != nil {
		return "", err
	}
	defer file.Close()

	// 将 map 编码为 JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // 设置缩进格式
	if err := encoder.Encode(r); err != nil {
		return "", err
	}
	api.logger.Info("结果输出到文件完成，结束")
	return "ok", nil
}

// PairCallBatch executes Call
func (api *APIImpl) PairCallBatch(triangulars []*pairtypes.ITriangularArbitrageTriangular) {
	// 初始化构造当前区块公共数据
	start := time.Now()
	api.logger.Info("开始执行PairCallBatch")
	results := make(chan interface{}, len(triangulars))

	// 提交任务到协程池，所有协程完成后关闭结果读取通道
	var wg sync.WaitGroup
	for _, triangular := range triangulars {
		wg.Add(1)
		gopool.Submit(func() {
			defer wg.Done()
			pairWorker(api, results, triangular)
		})
	}
	wg.Wait()
	close(results)
	selectSince := time.Since(start)
	api.logger.Info("所有eth_call查询任务执行完成花费时长", "runtime", selectSince)

	// 读取任务结果通道数据进行处理
	rois := make([]ROI, 5000)
	resultMap := make(map[string]interface{}, len(triangulars))
	i := 1
	// 处理结果
	for result := range results {
		itoa := strconv.Itoa(i)
		switch v := result.(type) {
		case *ROI:
			rois = append(rois, *v)
		case error:
			resultMap[itoa] = v.Error()
		default:
			resultMap[itoa] = v
		}
		i += 1
	}

	if len(rois) > 0 {
		// 按 Profit 字段对rois进行降序排序
		api.logger.Info("降序排序前的rois", "降序排序前rois", rois)
		sort.Slice(rois, func(i, j int) bool {
			return rois[i].Profit.Cmp(&rois[j].Profit) > 0
		})
		api.logger.Info("降序排序生成rois成功", "rois", rois)

		// 将排序后的rois去重过滤，保证每个pair只能出现一次，重复时将Profit较小的ROI都删除，只保留Profit最大的ROI
		// 去重，保证 Pair0, Pair1, Pair2 中的值只出现一次
		uniquePairs := make(map[libcommon.Address]bool)
		var filteredROIs []ROI
		for _, roi := range rois {
			if uniquePairs[roi.TriangularEntity.Pair0] || uniquePairs[roi.TriangularEntity.Pair1] || uniquePairs[roi.TriangularEntity.Pair2] {
				// 如果任何一个 pair 已经出现过，跳过该结构体（删除）
				continue
			}

			// 如果不存在，则将该结构体加入结果集，并标记 pairs 为已出现
			filteredROIs = append(filteredROIs, roi)
			uniquePairs[roi.TriangularEntity.Pair0] = true
			uniquePairs[roi.TriangularEntity.Pair1] = true
			uniquePairs[roi.TriangularEntity.Pair2] = true
		}
		api.logger.Info("排序去重获rois成功", "filteredROIs", filteredROIs)
	}

	totalSince := time.Since(start)
	api.logger.Info("处理结果完成", "共耗时", totalSince)

}

// Call implements eth_call. Executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutility.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.GasCap)
	}

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
	header := block.HeaderNoCopy()
	result, err := transactions.DoCall(ctx, engine, args, tx, blockNrOrHash, header, overrides, api.GasCap, chainConfig, stateReader, api._blockReader, api.evmCallTimeout)
	if err != nil {
		return nil, err
	}

	if len(result.ReturnData) > api.ReturnDataLimit {
		return nil, fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), api.ReturnDataLimit)
	}

	// If the result contains a revert reason, try to unpack and return it.
	if len(result.Revert()) > 0 {
		return nil, ethapi2.NewRevertError(result)
	}

	return result.Return(), result.Err
}

// headerByNumberOrHash - intent to read recent headers only, tries from the lru cache before reading from the db
func headerByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, api *APIImpl) (*types.Header, error) {
	_, bNrOrHashHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	block := api.tryBlockFromLru(bNrOrHashHash)
	if block != nil {
		return block.Header(), nil
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	// header can be nil
	return header, nil
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {
	var args ethapi2.CallArgs
	// if we actually get CallArgs here, we use them
	if argsOrNil != nil {
		args = *argsOrNil
	}

	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer dbtx.Rollback()

	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo     = params.TxGas - 1
		hi     uint64
		gasCap uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(libcommon.Address)
	}

	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		h, err := headerByNumberOrHash(ctx, dbtx, bNrOrHash, api)
		if err != nil {
			return 0, err
		}
		if h == nil {
			// if a block number was supplied and there is no header return 0
			if blockNrOrHash != nil {
				return 0, nil
			}

			// block number not supplied, so we haven't found a pending block, read the latest block instead
			h, err = headerByNumberOrHash(ctx, dbtx, latestNumOrHash, api)
			if err != nil {
				return 0, err
			}
			if h == nil {
				return 0, nil
			}
		}
		hi = h.GasLimit
	}

	var feeCap *big.Int
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return 0, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	} else if args.GasPrice != nil {
		feeCap = args.GasPrice.ToInt()
	} else if args.MaxFeePerGas != nil {
		feeCap = args.MaxFeePerGas.ToInt()
	} else {
		feeCap = libcommon.Big0
	}
	// Recap the highest gas limit with account's available balance.
	if feeCap.Sign() != 0 {
		cacheView, err := api.stateCache.View(ctx, dbtx)
		if err != nil {
			return 0, err
		}
		stateReader := rpchelper.CreateLatestCachedStateReader(cacheView, dbtx)
		state := state.New(stateReader)
		if state == nil {
			return 0, errors.New("can't get the current state")
		}

		balance := state.GetBalance(*args.From) // from can't be nil
		available := balance.ToBig()
		if args.Value != nil {
			if args.Value.ToInt().Cmp(available) >= 0 {
				return 0, errors.New("insufficient funds for transfer")
			}
			available.Sub(available, args.Value.ToInt())
		}
		allowance := new(big.Int).Div(available, feeCap)

		// If the allowance is larger than maximum uint64, skip checking
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := args.Value
			if transfer == nil {
				transfer = new(hexutil.Big)
			}
			log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer.ToInt(), "maxFeePerGas", feeCap, "fundable", allowance)
			hi = allowance.Uint64()
		}
	}

	// Recap the highest gas allowance with specified gascap.
	if hi > api.GasCap {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
		hi = api.GasCap
	}
	gasCap = hi

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return 0, err
	}
	engine := api.engine()

	latestCanBlockNumber, latestCanHash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, latestNumOrHash, dbtx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return 0, err
	}

	// try and get the block from the lru cache first then try DB before failing
	block := api.tryBlockFromLru(latestCanHash)
	if block == nil {
		block, err = api.blockWithSenders(ctx, dbtx, latestCanHash, latestCanBlockNumber)
		if err != nil {
			return 0, err
		}
	}
	if block == nil {
		return 0, errors.New("could not find latest block in cache or db")
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))
	stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, txNumsReader, latestCanBlockNumber, isLatest, 0, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return 0, err
	}
	header := block.HeaderNoCopy()

	caller, err := transactions.NewReusableCaller(engine, stateReader, overrides, header, args, api.GasCap, latestNumOrHash, dbtx, api._blockReader, chainConfig, api.evmCallTimeout)
	if err != nil {
		return 0, err
	}

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *evmtypes.ExecutionResult, error) {
		result, err := caller.DoCallWithNewGas(ctx, gas)
		if err != nil {
			if errors.Is(err, core.ErrIntrinsicGas) {
				// Special case, raise gas limit
				return true, nil, nil
			}

			// Bail out
			return true, nil, err
		}
		return result.Failed(), result, nil
	}

	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		failed, _, err := executable(mid)
		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigened. Return the error directly, don't struggle any more.
		if err != nil {
			return 0, err
		}
		if failed {
			lo = mid
		} else {
			hi = mid
		}
	}

	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == gasCap {
		failed, result, err := executable(hi)
		if err != nil {
			return 0, err
		}
		if failed {
			if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
				if len(result.Revert()) > 0 {
					return 0, ethapi2.NewRevertError(result)
				}
				return 0, result.Err
			}
			// Otherwise, the specified gas cap is too low
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", gasCap)
		}
	}
	return hexutil.Uint64(hi), nil
}

// maxGetProofRewindBlockCount limits the number of blocks into the past that
// GetProof will allow computing proofs.  Because we must rewind the hash state
// and re-compute the state trie, the further back in time the request, the more
// computationally intensive the operation becomes.  The staged sync code
// assumes that if more than 100_000 blocks are skipped, that the entire trie
// should be re-computed. Re-computing the entire trie will currently take ~15
// minutes on mainnet.  The current limit has been chosen arbitrarily as
// 'useful' without likely being overly computationally intense.

// GetProof is partially implemented; no Storage proofs, and proofs must be for
// blocks within maxGetProofRewindBlockCount blocks of the head.
func (api *APIImpl) GetProof(ctx context.Context, address libcommon.Address, storageKeys []libcommon.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*accounts.AccProofResult, error) {
	return nil, errors.New("not supported by Erigon3")
	/*
		tx, err := api.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		blockNr, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
		if err != nil {
			return nil, err
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNr)
		if err != nil {
			return nil, err
		}

		latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		if latestBlock < blockNr {
			// shouldn't happen, but check anyway
			return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
		}

		rl := trie.NewRetainList(0)
		var loader *trie.FlatDBTrieLoader
		if blockNr < latestBlock {
			if latestBlock-blockNr > uint64(api.MaxGetProofRewindBlockCount) {
				return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", uint64(api.MaxGetProofRewindBlockCount), latestBlock)
			}
			batch := membatchwithdb.NewMemoryBatch(tx, api.dirs.Tmp, api.logger)
			defer batch.Rollback()

			unwindState := &stagedsync.UnwindState{UnwindPoint: blockNr}
			stageState := &stagedsync.StageState{BlockNumber: latestBlock}

			hashStageCfg := stagedsync.StageHashStateCfg(nil, api.dirs, api.historyV3(batch))
			if err := stagedsync.UnwindHashStateStage(unwindState, stageState, batch, hashStageCfg, ctx, api.logger); err != nil {
				return nil, err
			}

			interHashStageCfg := stagedsync.StageTrieCfg(nil, false, false, false, api.dirs.Tmp, api._blockReader, nil, api.historyV3(batch), api._agg)
			loader, err = stagedsync.UnwindIntermediateHashesForTrieLoader("eth_getProof", rl, unwindState, stageState, batch, interHashStageCfg, nil, nil, ctx.Done(), api.logger)
			if err != nil {
				return nil, err
			}
			tx = batch
		} else {
			loader = trie.NewFlatDBTrieLoader("eth_getProof", rl, nil, nil, false)
		}

		reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, "")
		if err != nil {
			return nil, err
		}
		a, err := reader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if a == nil {
			a = &accounts.Account{}
		}
		pr, err := trie.NewProofRetainer(address, a, storageKeys, rl)
		if err != nil {
			return nil, err
		}

		loader.SetProofRetainer(pr)
		root, err := loader.CalcTrieRoot(tx, nil)
		if err != nil {
			return nil, err
		}

		if root != header.Root {
			return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in proof implementation", root, header.Root)
		}
		return pr.ProofResult()
	*/
}

func (api *APIImpl) tryBlockFromLru(hash libcommon.Hash) *types.Block {
	var block *types.Block
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			block = it
		}
	}
	return block
}

// accessListResult returns an optional accesslist
// Its the result of the `eth_createAccessList` RPC call.
// It contains an error if the transaction itself failed.
type accessListResult struct {
	Accesslist *types2.AccessList `json:"accessList"`
	Error      string             `json:"error,omitempty"`
	GasUsed    hexutil.Uint64     `json:"gasUsed"`
}

// CreateAccessList implements eth_createAccessList. It creates an access list for the given transaction.
// If the accesslist creation fails an error is returned.
// If the transaction itself fails, an vmErr is returned.
func (api *APIImpl) CreateAccessList(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, optimizeGas *bool) (*accessListResult, error) {
	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	blockNumber, hash, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, bNrOrHash, tx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	var stateReader state.StateReader
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = rpchelper.CreateLatestCachedStateReader(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, txNumsReader, blockNumber+1, 0, chainConfig.ChainName)
		if err != nil {
			return nil, err
		}
	}

	header := block.Header()
	// If the gas amount is not set, extract this as it will depend on access
	// lists and we'll need to reestimate every time
	nogas := args.Gas == nil

	var to libcommon.Address
	if args.To != nil {
		to = *args.To
	} else {
		// Require nonce to calculate address of created contract
		if args.Nonce == nil {
			var nonce uint64
			reply, err := api.txPool.Nonce(ctx, &txpool_proto.NonceRequest{
				Address: gointerfaces.ConvertAddressToH160(*args.From),
			}, &grpc.EmptyCallOption{})
			if err != nil {
				return nil, err
			}
			if reply.Found {
				nonce = reply.Nonce + 1
			} else {
				a, err := stateReader.ReadAccountData(*args.From)
				if err != nil {
					return nil, err
				}
				nonce = a.Nonce + 1
			}

			args.Nonce = (*hexutil.Uint64)(&nonce)
		}
		to = crypto.CreateAddress(*args.From, uint64(*args.Nonce))
	}

	if args.From == nil {
		args.From = &libcommon.Address{}
	}

	// Retrieve the precompiles since they don't need to be added to the access list
	precompiles := vm.ActivePrecompiles(chainConfig.Rules(blockNumber, header.Time))
	excl := make(map[libcommon.Address]struct{})
	for _, pc := range precompiles {
		excl[pc] = struct{}{}
	}

	// Create an initial tracer
	prevTracer := logger.NewAccessListTracer(nil, excl, nil)
	if args.AccessList != nil {
		prevTracer = logger.NewAccessListTracer(*args.AccessList, excl, nil)
	}
	for {
		state := state.New(stateReader)
		// Retrieve the current access list to expand
		accessList := prevTracer.AccessList()
		log.Trace("Creating access list", "input", accessList)

		// If no gas amount was specified, each unique access list needs it's own
		// gas calculation. This is quite expensive, but we need to be accurate
		// and it's convered by the sender only anyway.
		if nogas {
			args.Gas = nil
		}
		// Set the accesslist to the last al
		args.AccessList = &accessList

		var msg types.Message

		var baseFee *uint256.Int = nil
		// check if EIP-1559
		if header.BaseFee != nil {
			baseFee, _ = uint256.FromBig(header.BaseFee)
		}

		msg, err = args.ToMessage(api.GasCap, baseFee)
		if err != nil {
			return nil, err
		}

		// Apply the transaction with the access list tracer
		tracer := logger.NewAccessListTracer(accessList, excl, state)
		config := vm.Config{Tracer: tracer, Debug: true, NoBaseFee: true}
		blockCtx := transactions.NewEVMBlockContext(engine, header, bNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
		txCtx := core.NewEVMTxContext(msg)

		evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, config)
		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		res, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		if tracer.Equal(prevTracer) {
			var errString string
			if res.Err != nil {
				errString = res.Err.Error()
			}
			accessList := &accessListResult{Accesslist: &accessList, Error: errString, GasUsed: hexutil.Uint64(res.UsedGas)}
			if optimizeGas == nil || *optimizeGas { // optimize gas unless explicitly told not to
				optimizeWarmAddrInAccessList(accessList, *args.From)
				optimizeWarmAddrInAccessList(accessList, to)
				optimizeWarmAddrInAccessList(accessList, header.Coinbase)
				for addr := range tracer.CreatedContracts() {
					if !tracer.UsedBeforeCreation(addr) {
						optimizeWarmAddrInAccessList(accessList, addr)
					}
				}
			}
			return accessList, nil
		}
		prevTracer = tracer
	}
}

// some addresses (like sender, recipient, block producer, and created contracts)
// are considered warm already, so we can save by adding these to the access list
// only if we are adding a lot of their respective storage slots as well
func optimizeWarmAddrInAccessList(accessList *accessListResult, addr libcommon.Address) {
	indexToRemove := -1

	for i := 0; i < len(*accessList.Accesslist); i++ {
		entry := (*accessList.Accesslist)[i]
		if entry.Address != addr {
			continue
		}

		// https://eips.ethereum.org/EIPS/eip-2930#charging-less-for-accesses-in-the-access-list
		accessListSavingPerSlot := params.ColdSloadCostEIP2929 - params.WarmStorageReadCostEIP2929 - params.TxAccessListStorageKeyGas

		numSlots := uint64(len(entry.StorageKeys))
		if numSlots*accessListSavingPerSlot <= params.TxAccessListAddressGas {
			indexToRemove = i
		}
	}

	if indexToRemove >= 0 {
		*accessList.Accesslist = removeIndex(*accessList.Accesslist, indexToRemove)
	}
}

func removeIndex(s types2.AccessList, index int) types2.AccessList {
	return append(s[:index], s[index+1:]...)
}
