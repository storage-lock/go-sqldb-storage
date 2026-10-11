package sqldb_storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

func TestNewStorageBySqlDb(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotEmpty(t, s.GetName())
}

func TestNewStorageBySqlDb_UnsupportedDriver(t *testing.T) {
	db, err := sql.Open("test-dummy-driver", "")
	assert.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db)
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewStorageBySqlDb_NilDB(t *testing.T) {
	s, err := NewStorage(nil)
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewStorageByDriverName_Unsupported(t *testing.T) {
	cm := storage.NewFixedSqlDBConnectionManager(&sql.DB{})
	s, err := NewStorageByDriverName("unknown-driver", cm)
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), "do not suppoort driver name")
}

func TestNewStorageByDriverName_Sqlite3(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	cm := storage.NewFixedSqlDBConnectionManager(db)
	s, err := NewStorageByDriverName("sqlite3", cm)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotEmpty(t, s.GetName())
}

func TestGetDriverNameForSqlDb_Unknown(t *testing.T) {
	db, err := sql.Open("test-dummy-driver", "")
	assert.NoError(t, err)
	defer db.Close()

	name, err := GetDriverNameForSqlDb(db)
	assert.Error(t, err)
	assert.Empty(t, name)
}

func TestGetDriverNameForSqlDb_Mysql(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/dbname")
	assert.NoError(t, err)
	defer db.Close()

	name, err := GetDriverNameForSqlDb(db)
	assert.NoError(t, err)
	assert.Equal(t, DriverNameMysql, name)
}

func TestGetDriverNameForSqlDb_Postgres(t *testing.T) {
	db, err := sql.Open("postgres", "postgresql://user:pass@127.0.0.1:5432/dbname?sslmode=disable")
	assert.NoError(t, err)
	defer db.Close()

	name, err := GetDriverNameForSqlDb(db)
	assert.NoError(t, err)
	assert.Equal(t, DriverNamePostgresql, name)
}

func TestGetDriverNameForSqlDb_SqlServer(t *testing.T) {
	db, err := sql.Open("sqlserver", "sqlserver://user:pass@127.0.0.1:1433?database=dbname")
	assert.NoError(t, err)
	defer db.Close()

	name, err := GetDriverNameForSqlDb(db)
	assert.NoError(t, err)
	assert.Equal(t, DriverNameSqlServer, name)
}

func TestGetDriverNameForSqlDb_Sqlite3(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	name, err := GetDriverNameForSqlDb(db)
	assert.NoError(t, err)
	assert.Equal(t, DriverNameSqlite3, name)
}

func TestNewStorage(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotEmpty(t, s.GetName())
}

func TestNewStorage_GetDriverNameError(t *testing.T) {
	db, err := sql.Open("test-dummy-driver", "")
	assert.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db)
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewStorageByConnectionManager(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	cm := storage.NewFixedSqlDBConnectionManager(db)
	s, err := NewStorageByConnectionManager(ctx, cm)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotEmpty(t, s.GetName())
}

func TestNewStorageByConnectionManager_TakeError(t *testing.T) {
	cm := &mockTakeErrorConnectionManager{}
	ctx := context.Background()

	s, err := NewStorageByConnectionManager(ctx, cm)
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewStorageByConnectionManager_ReturnError(t *testing.T) {
	// 创建一个真实的 sqlite3 连接
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	// 创建一个在 Return 时会返回错误的连接管理器
	cm := &mockErrorReturnConnectionManager{db: db}
	ctx := context.Background()

	s, err := NewStorageByConnectionManager(ctx, cm)
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.Equal(t, mockReturnError, err)
}

func TestNewStorageByConnectionManager_ReturnErrorAfterSuccess(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?mode=memory")
	assert.NoError(t, err)
	defer db.Close()

	cm := &mockReturnErrorAfterSecondConnectionManager{
		db:        db,
		returnErr: assert.AnError,
	}
	ctx := context.Background()

	s, err := NewStorageByConnectionManager(ctx, cm)
	assert.Error(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, assert.AnError, err)
}

func TestNewStorageByConnectionManager_GetDriverNameError(t *testing.T) {
	// 使用不支持的驱动
	db, err := sql.Open("test-dummy-driver", "")
	assert.NoError(t, err)
	defer db.Close()

	cm := storage.NewFixedSqlDBConnectionManager(db)
	ctx := context.Background()

	s, err := NewStorageByConnectionManager(ctx, cm)
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), "do not support driver")
}

type testDummyDriver struct{}

func (d *testDummyDriver) Open(name string) (driver.Conn, error) {
	return nil, driver.ErrSkip
}

func init() {
	sql.Register("test-dummy-driver", &testDummyDriver{})
}

type mockTakeErrorConnectionManager struct{}

func (m *mockTakeErrorConnectionManager) Name() string {
	return "mock-take-error"
}

func (m *mockTakeErrorConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
	return nil, assert.AnError
}

func (m *mockTakeErrorConnectionManager) Return(ctx context.Context, db *sql.DB) error {
	return nil
}

func (m *mockTakeErrorConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

var mockReturnError = assert.AnError

type mockErrorReturnConnectionManager struct {
	db *sql.DB
}

func (m *mockErrorReturnConnectionManager) Name() string {
	return "mock-error-return"
}

func (m *mockErrorReturnConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
	return m.db, nil
}

func (m *mockErrorReturnConnectionManager) Return(ctx context.Context, db *sql.DB) error {
	return mockReturnError
}

func (m *mockErrorReturnConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

// mockReturnErrorAfterSecondConnectionManager 用于测试 Return 错误在成功路径之后
type mockReturnErrorAfterSecondConnectionManager struct {
	db          *sql.DB
	returnErr   error
	returnCount int
}

func (m *mockReturnErrorAfterSecondConnectionManager) Name() string {
	return "mock-return-error-after-second"
}

func (m *mockReturnErrorAfterSecondConnectionManager) Take(ctx context.Context) (*sql.DB, error) {
	return m.db, nil
}

func (m *mockReturnErrorAfterSecondConnectionManager) Return(ctx context.Context, db *sql.DB) error {
	m.returnCount++
	// 第一次 Return（来自 Init）成功，第二次 Return（来自外层 defer）失败
	if m.returnCount >= 2 {
		return m.returnErr
	}
	return nil
}

func (m *mockReturnErrorAfterSecondConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

func TestNewStorageBySqlDb_NilDBDirect(t *testing.T) {
	s, err := NewStorageBySqlDb(nil, storage.NewFixedSqlDBConnectionManager(&sql.DB{}))
	assert.Error(t, err)
	assert.Nil(t, s)
}
