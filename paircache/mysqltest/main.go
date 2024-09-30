package main

import (
	"fmt"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/paircache/mysqldb"
	"github.com/erigontech/erigon/paircache/pairtypes"
	"github.com/jmoiron/sqlx"
	"math/big"
	"strings"
)

var abiStr = "[{\"inputs\":[],\"name\":\"arb_wcnwzblucpyf\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"token0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair2\",\"type\":\"address\"}],\"internalType\":\"structITriangularArbitrage.Triangular\",\"name\":\"t\",\"type\":\"tuple\"},{\"internalType\":\"uint256\",\"name\":\"startRatio\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"endRatio\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"pieces\",\"type\":\"uint256\"}],\"name\":\"arbitrageQuery\",\"outputs\":[{\"internalType\":\"int256[]\",\"name\":\"roi\",\"type\":\"int256[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"token0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair0\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair1\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"token2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"router2\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"pair2\",\"type\":\"address\"}],\"internalType\":\"structITriangularArbitrage.Triangular\",\"name\":\"t\",\"type\":\"tuple\"},{\"internalType\":\"uint256\",\"name\":\"threshold\",\"type\":\"uint256\"}],\"name\":\"isTriangularValid\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"}]"

var ABI *abi.ABI

func init() {
	// 加载三角合约abi
	if parsed, err := abi.JSON(strings.NewReader(abiStr)); err != nil {
		fmt.Printf("加载三角合约abi失败，err=%v\n", err)
		return
	} else {
		ABI = &parsed
	}
	fmt.Printf("初次加载三角合约abi到内存中成功，ABI=%v\n", *ABI)
}

func Encoder(name string, args ...interface{}) ([]byte, error) {
	return ABI.Pack(name, args...)
}

func main() {
	// 初始化数据库连接
	mysqlDB := mysqldb.GetMysqlDB()

	// 使用流式查询，逐行处理数据
	rows, err := mysqlDB.Queryx("SELECT id, token0, router0, pair0, token1, router1, pair1, token2, router2, pair2 FROM arbitrage_triangle limit 0, 10")
	if err != nil {
		fmt.Printf("查询失败，err=%v\n", err)
	}
	defer func(rows *sqlx.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Printf("关闭rows失败，err=%v\n", err)
		}
	}(rows)

	// 遍历查询结果
	for rows.Next() {
		var triangle pairtypes.Triangle
		err := rows.StructScan(&triangle)
		if err != nil {
			fmt.Printf("填充结果到结构体失败，err=%v\n", err)
		}
		fmt.Printf("triangles=%v\n", triangle)

		triangular := pairtypes.ITriangularArbitrageTriangular{
			Token0:  common.HexToAddress(triangle.Token0),
			Router0: common.HexToAddress(triangle.Router0),
			Pair0:   common.HexToAddress(triangle.Pair0),
			Token1:  common.HexToAddress(triangle.Token1),
			Router1: common.HexToAddress(triangle.Router1),
			Pair1:   common.HexToAddress(triangle.Pair1),
			Token2:  common.HexToAddress(triangle.Token2),
			Router2: common.HexToAddress(triangle.Router2),
			Pair2:   common.HexToAddress(triangle.Pair2),
		}

		data, err := Encoder("arbitrageQuery", triangular, big.NewInt(0), big.NewInt(10000), big.NewInt(10))
		if err != nil {
			fmt.Printf("编码triangles数据失败，err=%v\n", err)
		} else {
			fmt.Printf("编码triangles数据成功，data=0x%x\n", data)
		}
	}

	// 检查是否有遍历中的错误
	if err := rows.Err(); err != nil {
		fmt.Printf("查询失败，err=%v\n", err)
	}

}
