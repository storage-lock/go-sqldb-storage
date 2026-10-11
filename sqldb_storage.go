package sqldb_storage

import (
	"context"
	"database/sql"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
	mysql_storage "github.com/storage-lock/go-mysql-storage"
	postgresql_storage "github.com/storage-lock/go-postgresql-storage"
	sqlite3_storage "github.com/storage-lock/go-sqlite3-storage"
	sqlserver_storage "github.com/storage-lock/go-sqlserver-storage"
	"github.com/storage-lock/go-storage"
	"reflect"
)

// TODO 2023-8-6 23:09:42 扩展更多支持的驱动
const (
	DriverNameMysql      = "mysql"
	DriverNamePostgresql = "postgres"
	DriverNameSqlServer  = "sqlserver"
	DriverNameSqlite3    = "sqlite3"
)

// NewStorage 根据sql.DB创建对应的Storage
func NewStorage(db *sql.DB) (storage.Storage, error) {
	if db == nil {
		return nil, fmt.Errorf("db can not nil")
	}
	connectionManager := storage.NewFixedSqlDBConnectionManager(db)
	return NewStorageBySqlDb(db, connectionManager)
}

// NewStorageBySqlDb 根据sql.DB和连接管理器创建对应的Storage
// 历史上这就是该函数一直对外暴露的名字（go-sqldb-storage/coverage.html、
// go-sqldb-locks/sqldb_locks_test.go 以及 beego/gorm/xorm/sqlx/sqldb 五个 locks
// 仓库全部调用此名）；曾有工作区改动把它内联进 NewStorage 导致符号消失，本次恢复为
// 独立函数，NewStorage 改为委托它实现，行为与原来完全一致（含 nil db 显式报错）。
func NewStorageBySqlDb(db *sql.DB, connectionManager storage.ConnectionManager[*sql.DB]) (storage.Storage, error) {
	if db == nil {
		return nil, fmt.Errorf("db can not nil")
	}
	driverName, err := GetDriverNameForSqlDb(db)
	if err != nil {
		return nil, err
	}
	return NewStorageByDriverName(driverName, connectionManager)
}

// NewStorageByConnectionManager 从sql.DB的连接管理器中创建Storage
func NewStorageByConnectionManager(ctx context.Context, connectionManager storage.ConnectionManager[*sql.DB]) (sqlDbStorage storage.Storage, returnError error) {

	// 从连接池中获取sql.DB
	db, err := connectionManager.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer func(connectionManager storage.ConnectionManager[*sql.DB], ctx context.Context, connection *sql.DB) {
		err := connectionManager.Return(ctx, connection)
		if err != nil && returnError == nil {
			returnError = err
		}
	}(connectionManager, ctx, db)

	// 释放掉
	driverName, err := GetDriverNameForSqlDb(db)
	if err != nil {
		return nil, err
	}
	return NewStorageByDriverName(driverName, connectionManager)
}

// NewStorageByDriverName 根据驱动名称创建对应的Storage
func NewStorageByDriverName(driverName string, connectionManager storage.ConnectionManager[*sql.DB]) (storage.Storage, error) {
	switch driverName {
	case DriverNameMysql:
		options := mysql_storage.NewMysqlStorageOptions().SetConnectionManager(connectionManager)
		return mysql_storage.NewMysqlStorage(context.Background(), options)
	case DriverNamePostgresql:
		options := postgresql_storage.NewPostgresqlStorageOptions().SetConnectionManager(connectionManager)
		return postgresql_storage.NewPostgresqlStorage(context.Background(), options)
	case DriverNameSqlServer:
		options := sqlserver_storage.NewSqlServerStorageOptions().SetConnectionManager(connectionManager)
		return sqlserver_storage.NewSqlServerStorage(context.Background(), options)
	case DriverNameSqlite3:
		options := sqlite3_storage.NewSqlite3StorageOptions().SetConnectionManager(connectionManager)
		return sqlite3_storage.NewSqlite3Storage(context.Background(), options)
	default:
		return nil, fmt.Errorf("do not suppoort driver name %s", driverName)
	}
}

// GetDriverNameForSqlDb 根据sql.Driver的不同实现来识别驱动的名称
func GetDriverNameForSqlDb(db *sql.DB) (string, error) {
	switch db.Driver().(type) {
	case *mysql.MySQLDriver:
		return DriverNameMysql, nil
	case *pq.Driver:
		return DriverNamePostgresql, nil
	case *mssql.Driver:
		return DriverNameSqlServer, nil
	case *sqlite3.SQLiteDriver:
		return DriverNameSqlite3, nil
	default:
		return "", fmt.Errorf("do not support driver %s", reflect.TypeOf(db.Driver()).Name())
	}
}
