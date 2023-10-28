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
	connectionManager := storage.NewFixedSqlDBConnectionManager(db)
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
