package mysqldb

import (
	"fmt"
	"github.com/erigontech/erigon-lib/log/v3"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// DB 是全局数据库连接池
var (
	mysqldb  *sqlx.DB
	once     sync.Once
	user     = "root"
	password = "FG0mKQ35JRvaXxacGgBtXT1uwerwoVwi"
	// hostname = "135.181.218.173:3306"
	hostname        = "192.168.100.102:3306"
	dbname          = "arbitrage-bsc"
	maxOpenConns    = 10
	maxIdleConns    = 5
	connMaxLifetime = time.Hour
)

// InitDB 初始化数据库连接
func init() {
	once.Do(func() {
		// 构建 DSN (Data Source Name)
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, password, hostname, dbname)

		// 打开数据库连接
		var err error
		mysqldb, err = sqlx.Open("mysql", dsn)
		if err != nil {
			log.Error("Error opening database", err)
		}

		// 配置连接池
		mysqldb.SetMaxOpenConns(maxOpenConns)
		mysqldb.SetMaxIdleConns(maxIdleConns)
		mysqldb.SetConnMaxLifetime(connMaxLifetime)

		// 验证连接
		if err = mysqldb.Ping(); err != nil {
			log.Error("Error pinging database", err)
		}
	})
}

// GetDB 返回数据库连接池对象
func GetMysqlDB() *sqlx.DB {
	if mysqldb == nil {
		log.Error("Database not initialized. Call InitDB first.")
	}
	return mysqldb
}
