package sqldb_storage

import (
	"context"
	"database/sql"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	mysql_storage "github.com/storage-lock/go-mysql-storage"
	postgresql_storage "github.com/storage-lock/go-postgresql-storage"
	sqlserver_storage "github.com/storage-lock/go-sqlserver-storage"
	"github.com/storage-lock/go-storage"
	"reflect"
)

// TODO 2023-8-6 23:09:42 扩展更多支持的驱动
const (
	DriverNameMysql      = "mysql"
	DriverNamePostgresql = "postgres"
	DriverNameSqlServer  = "sqlserver"
)

// NewStorageBySqlDb 根据sql.DB创建对应的Storage
func NewStorageBySqlDb(db *sql.DB, connectionManager storage.ConnectionManager[*sql.DB]) (storage.Storage, error) {
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
		options := mysql_storage.NewMySQLStorageOptions().SetConnectionProvider(connectionManager)
		return mysql_storage.NewMySQLStorage(context.Background(), options)
	case DriverNamePostgresql:
		options := postgresql_storage.NewPostgreSQLStorageOptions().SetConnectionManager(connectionManager)
		return postgresql_storage.NewPostgreSQLStorage(context.Background(), options)
	case DriverNameSqlServer:
		options := sqlserver_storage.NewSqlServerStorageOptions().SetConnectionManage(connectionManager)
		return sqlserver_storage.NewSqlServerStorage(context.Background(), options)
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
	default:
		return "", fmt.Errorf("do not support driver %s", reflect.TypeOf(db.Driver()).Name())
	}
}
