package migrago

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/lib/pq"
)

func CreateTestPostgresContainer(t *testing.T, ctx context.Context) (*sql.DB, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16.3",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForSQL(nat.Port("5432"), "postgres", func(host string, port nat.Port) string {
			return fmt.Sprintf("user=postgres password=postgres dbname=postgres host=%s port=%s sslmode=disable", host, port.Port())
		}),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		container.Terminate(ctx)
	})
	ip, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("user=postgres password=postgres dbname=postgres host=%s port=%s sslmode=disable", ip, port.Port())
	t.Logf("Postgres-dsn: %s", dsn)
	d, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func Test_ExecuteMigration(t *testing.T) {
	t.Run("Test with empty MigrationList", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		err = ExecuteMigration(ctx, d, []Migration{})
		assert.NoError(t, err)
	})
	t.Run("Test with one Migrations", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			},
		})
		assert.NoError(t, err)
		var checksum string
		err = d.QueryRow("SELECT checksum FROM changelog").Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)
		var exists bool
		err = d.QueryRow("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = 'test')").Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists)
	})
	t.Run("Test with multiple Migrations", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			},
		})
		assert.NoError(t, err)
		var checksum string
		err = d.QueryRow("SELECT checksum FROM changelog").Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)
		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
	t.Run("Test with multiple Migrations", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.NoError(t, err)
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.True(t, rows.Next())
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f8", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
	t.Run("Test with multiple Migrations and one already exists", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.NoError(t, err)
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.True(t, rows.Next())
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f8", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
	t.Run("Test with multiple Migrations and all already exists", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f8", "DROP TABLE test2")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.NoError(t, err)
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.True(t, rows.Next())
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f8", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)

		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("Test with multiple Migrations and one checksum is diffrent", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f7", "DROP TABLE test2")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.ErrorContains(t, err, "checksum mismatch")
	})

	t.Run("Test with multiple Migrations and one revert", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f7", "DROP TABLE test2")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			}, {
				Id:           "Test2",
				Script:       "CREATE TABLE test2 (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test2",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.ErrorContains(t, err, "checksum mismatch")
	})
	t.Run("Test with one revert Script", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f8", "DROP TABLE test2")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test2 (id VARCHAR(255))")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test (id VARCHAR(255))")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			},
		})
		assert.NoError(t, err)
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Test with multiple revert Scripts", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f8", "DROP TABLE test2")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test3", "9c23564a026f0826f2a05b8423aa21f7", "DROP TABLE test3")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test (id VARCHAR(255))")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test2 (id VARCHAR(255))")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test3 (id VARCHAR(255))")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f9",
			},
		})
		assert.NoError(t, err)
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2', 'test3')").Scan(&count)

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Test with cannot revert Scripts", func(t *testing.T) {
		ctx := context.Background()
		d, err := CreateTestPostgresContainer(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()

		_, err = d.Exec("CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test", "9c23564a026f0826f2a05b8423aa21f9", "DROP TABLE test")
		assert.NoError(t, err)
		_, err = d.Exec("INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", "Test2", "9c23564a026f0826f2a05b8423aa21f8", "DROP TABLE test2")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test (id VARCHAR(255))")
		assert.NoError(t, err)
		_, err = d.Exec("CREATE TABLE IF NOT EXISTS test2 (id VARCHAR(255))")
		assert.NoError(t, err)
		err = ExecuteMigration(ctx, d, []Migration{
			{
				Id:           "Test2",
				Script:       "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL)",
				RevertScript: "DROP TABLE test",
				Checksum:     "9c23564a026f0826f2a05b8423aa21f8",
			},
		})
		assert.ErrorContains(t, err, "not revertedable migration found")
		var checksum string
		var rows *sql.Rows
		rows, err = d.Query("SELECT checksum FROM changelog ORDER BY id")
		assert.True(t, rows.Next())
		assert.NoError(t, err)
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f9", checksum)

		assert.True(t, rows.Next())
		err = rows.Scan(&checksum)
		assert.NoError(t, err)
		assert.Equal(t, "9c23564a026f0826f2a05b8423aa21f8", checksum)

		assert.False(t, rows.Next())

		var count int
		err = d.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name IN ('test', 'test2')").Scan(&count)

		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}
