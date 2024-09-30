package parlia

import (
	"container/heap"
	"errors"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/systemcontracts"
	"github.com/erigontech/erigon/core/types"
)

const SecondsPerDay uint64 = 86400

// the params should be two blocks' time(timestamp)
func sameDayInUTC(first, second uint64) bool {
	return first/SecondsPerDay == second/SecondsPerDay
}

func isBreatheBlock(lastBlockTime, blockTime uint64) bool {
	return lastBlockTime != 0 && !sameDayInUTC(lastBlockTime, blockTime)
}

// initializeFeynmanContract initialize new contracts of Feynman fork
func (p *Parlia) initializeFeynmanContract(state *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions, usedGas *uint64, mining bool,
	systemTxCall consensus.SystemTxCall, curIndex *int, txIndex *int,
) (finish bool, err error) {
	// method
	method := "initialize"

	// initialize contracts
	contracts := []common.Address{
		systemcontracts.StakeHubContract,
		systemcontracts.GovernorContract,
		systemcontracts.GovTokenContract,
		systemcontracts.TimelockContract,
		systemcontracts.TokenRecoverPortalContract,
	}
	// get packed data
	data, err := p.stakeHubABI.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for initialize feynman contracts", "error", err)
		return false, err
	}
	for _, c := range contracts {
		// apply message
		log.Info("initialize feynman contract", "block number", header.Number.Uint64(), "contract", c)
		if *curIndex == *txIndex {
			return p.applyTransaction(header.Coinbase, c, u256.Num0, data, state, header, txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
		}
		*curIndex++
	}
	return false, nil
}

type ValidatorItem struct {
	address     common.Address
	votingPower *big.Int
	voteAddress []byte
}

// An ValidatorHeap is a max-heap of validator's votingPower.
type ValidatorHeap []ValidatorItem

func (h *ValidatorHeap) Len() int { return len(*h) }

func (h *ValidatorHeap) Less(i, j int) bool {
	// We want topK validators with max voting power, so we need a max-heap
	if (*h)[i].votingPower.Cmp((*h)[j].votingPower) == 0 {
		return (*h)[i].address.Hex() < (*h)[j].address.Hex()
	} else {
		return (*h)[i].votingPower.Cmp((*h)[j].votingPower) == 1
	}
}

func (h *ValidatorHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *ValidatorHeap) Push(x interface{}) {
	*h = append(*h, x.(ValidatorItem))
}

func (h *ValidatorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (p *Parlia) updateValidatorSetV2(chain consensus.ChainHeaderReader, ibs *state.IntraBlockState, header *types.Header,
	txs *types.Transactions, receipts *types.Receipts, systemTxs *types.Transactions, usedGas *uint64, mining bool,
	systemTxCall consensus.SystemTxCall, curIndex *int, txIndex *int, tx kv.Tx,
) (bool, error) {
	// 1. get all validators and its voting header.Nu power
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)

	if validatorItemsCache == nil && maxElectedValidatorsCache == big.NewInt(0) {
		stateReader := state.NewHistoryReaderV3()
		stateReader.SetTx(tx)
		maxTxNum, _ := rawdbv3.TxNums.Max(tx, header.Number.Uint64()-1)
		stateReader.SetTxNum(maxTxNum)
		history := state.New(stateReader)
		var err error
		validatorItemsCache, err = p.getValidatorElectionInfo(parent, history)
		if err != nil {
			return true, err
		}
		maxElectedValidatorsCache, err = p.getMaxElectedValidators(parent, history)
		if err != nil {
			return true, err
		}
	}

	// 2. sort by voting power
	eValidators, eVotingPowers, eVoteAddrs := getTopValidatorsByVotingPower(validatorItemsCache, maxElectedValidatorsCache)

	// 3. update validator set to system contract
	method := "updateValidatorSetV2"
	data, err := p.validatorSetABI.Pack(method, eValidators, eVotingPowers, eVoteAddrs)
	if err != nil {
		log.Error("Unable to pack tx for updateValidatorSetV2", "error", err)
		return true, err
	}

	// apply message

	if *curIndex == *txIndex {
		return p.applyTransaction(header.Coinbase, systemcontracts.ValidatorContract, u256.Num0, data, ibs, header, txs, receipts, systemTxs, usedGas, mining, systemTxCall, curIndex)
	}
	*curIndex++
	return false, nil
}

func (p *Parlia) getValidatorElectionInfo(header *types.Header, ibs *state.IntraBlockState) ([]ValidatorItem, error) {

	method := "getValidatorElectionInfo"

	data, err := p.stakeHubABI.Pack(method, big.NewInt(0), big.NewInt(0))
	if err != nil {
		log.Error("Unable to pack tx for getValidatorElectionInfo", "error", err)
		return nil, err
	}
	msgData := (hexutility.Bytes)(data)

	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.StakeHubContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	var validators []common.Address
	var votingPowers []*big.Int
	var voteAddrs [][]byte
	var totalLength *big.Int
	if err := p.stakeHubABI.UnpackIntoInterface(&[]interface{}{&validators, &votingPowers, &voteAddrs, &totalLength}, method, returnData); err != nil {
		return nil, err
	}
	if totalLength.Int64() != int64(len(validators)) || totalLength.Int64() != int64(len(votingPowers)) || totalLength.Int64() != int64(len(voteAddrs)) {
		return nil, errors.New("validator length not match")
	}

	validatorItems := make([]ValidatorItem, len(validators))
	for i := 0; i < len(validators); i++ {
		validatorItems[i] = ValidatorItem{
			address:     validators[i],
			votingPower: votingPowers[i],
			voteAddress: voteAddrs[i],
		}
	}

	return validatorItems, nil
}

func (p *Parlia) getMaxElectedValidators(header *types.Header, ibs *state.IntraBlockState) (maxElectedValidators *big.Int, err error) {

	method := "maxElectedValidators"

	data, err := p.stakeHubABI.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for maxElectedValidators", "error", err)
		return nil, err
	}
	msgData := (hexutility.Bytes)(data)

	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.StakeHubContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	if err := p.stakeHubABI.UnpackIntoInterface(&maxElectedValidators, method, returnData); err != nil {
		return nil, err
	}

	return maxElectedValidators, nil
}

func getTopValidatorsByVotingPower(validatorItems []ValidatorItem, maxElectedValidators *big.Int) ([]common.Address, []uint64, [][]byte) {
	var validatorHeap ValidatorHeap
	for i := 0; i < len(validatorItems); i++ {
		// only keep validators with voting power > 0
		if validatorItems[i].votingPower.Cmp(big.NewInt(0)) == 1 {
			validatorHeap = append(validatorHeap, validatorItems[i])
		}
	}
	hp := &validatorHeap
	heap.Init(hp)

	topN := int(maxElectedValidators.Int64())
	if topN > len(validatorHeap) {
		topN = len(validatorHeap)
	}
	eValidators := make([]common.Address, topN)
	eVotingPowers := make([]uint64, topN)
	eVoteAddrs := make([][]byte, topN)
	for i := 0; i < topN; i++ {
		item := heap.Pop(hp).(ValidatorItem)
		eValidators[i] = item.address
		// as the decimal in BNB Beacon Chain is 1e8 and in BNB Smart Chain is 1e18, we need to divide it by 1e10
		eVotingPowers[i] = new(big.Int).Div(item.votingPower, big.NewInt(1e10)).Uint64()
		eVoteAddrs[i] = item.voteAddress
	}

	return eValidators, eVotingPowers, eVoteAddrs
}
