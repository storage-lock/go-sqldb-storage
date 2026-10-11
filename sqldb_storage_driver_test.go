package sqldb_storage

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

// newMockDB creates a sqlmock DB and returns it with a connection manager
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.ConnectionManager[*sql.DB]) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	cm := storage.NewFixedSqlDBConnectionManager(db)
	return db, mock, cm
}

func TestNewStorageByDriverName_Mysql(t *testing.T) {
	db, mock, cm := newMockDB(t)
	defer db.Close()

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))

	s, err := NewStorageByDriverName(DriverNameMysql, cm)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "mysql-storage", s.GetName())
}

func TestNewStorageByDriverName_Postgres(t *testing.T) {
	db, mock, cm := newMockDB(t)
	defer db.Close()

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))

	s, err := NewStorageByDriverName(DriverNamePostgresql, cm)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "postgresql-storage", s.GetName())
}

func TestNewStorageByDriverName_SqlServer(t *testing.T) {
	db, mock, cm := newMockDB(t)
	defer db.Close()

	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))

	s, err := NewStorageByDriverName(DriverNameSqlServer, cm)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "sql-server-storage", s.GetName())
}
