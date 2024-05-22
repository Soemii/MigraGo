package migrago

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
)

type Migration struct {
	Id           string
	Script       string
	RevertScript string
	Checksum     string
}

func ExecuteMigration(ctx context.Context, conn *sql.DB, migrations []Migration) (err error) {
	fileMigrations := convertMigrationToMap(migrations)
	_, err = conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS changelog (id VARCHAR(255) PRIMARY KEY, checksum VARCHAR(255) NOT NULL, installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, revertscript TEXT)")
	if err != nil {
		return
	}
	var rows *sql.Rows
	rows, err = conn.QueryContext(ctx, "SELECT id, checksum, revertscript FROM changelog ORDER BY installedAt DESC")
	if err != nil {
		return
	}
	defer rows.Close()
	notReverted := false
	for rows.Next() {
		var dbMigration Migration
		rows.Scan(&dbMigration.Id, &dbMigration.Checksum, &dbMigration.RevertScript)
		_, ok := fileMigrations[dbMigration.Id]
		if !ok {
			if notReverted {
				err = errors.New("not revertedable migration found")
				return
			}
			revertScript(ctx, conn, dbMigration)
		} else {
			notReverted = true
			if dbMigration.Checksum != fileMigrations[dbMigration.Id].Checksum {
				err = errors.New("checksum mismatch")
				return
			}
			delete(fileMigrations, dbMigration.Id)
		}
	}
	for _, migration := range fileMigrations {
		err = executeChangeLog(ctx, conn, migration)
		if err != nil {
			return
		}
	}
	return
}

func ReadChangeLogFile(fileName string) (migrations []Migration, err error) {
	var buffer []byte
	buffer, err = os.ReadFile(fileName)
	if err != nil {
		return
	}
	var migrationIds []string
	err = json.Unmarshal(buffer, &migrationIds)
	if err != nil {
		return
	}
	for _, id := range migrationIds {
		buffer, err = os.ReadFile(id + ".sql")
		if err != nil {
			return
		}
		script := string(buffer)
		buffer, err = os.ReadFile(id + ".revert.sql")
		if err != nil {
			return
		}
		revertScript := string(buffer)
		checksum := md5.Sum([]byte(script))
		migrations = append(migrations, Migration{Id: id, Script: script, RevertScript: revertScript, Checksum: string(checksum[:])})
	}
	return
}

func generateHash(s string) (hash string) {
	sum := md5.Sum([]byte(s))
	hash = string(sum[:])
	return
}

func convertMigrationToMap(migrations []Migration) (m map[string]Migration) {
	m = make(map[string]Migration)
	for _, migration := range migrations {
		m[migration.Id] = migration
	}
	return
}

func executeChangeLog(ctx context.Context, conn *sql.DB, migration Migration) (err error) {
	var transaction *sql.Tx
	transaction, err = conn.BeginTx(ctx, nil)
	_, err = transaction.ExecContext(ctx, "INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)", migration.Id, migration.Checksum, migration.RevertScript)
	if err != nil {
		return
	}
	_, err = transaction.ExecContext(ctx, migration.Script)
	if err != nil {
		return
	}
	err = transaction.Commit()
	return
}

func revertScript(ctx context.Context, conn *sql.DB, migration Migration) (err error) {
	var transaction *sql.Tx
	transaction, err = conn.BeginTx(ctx, nil)
	_, err = transaction.ExecContext(ctx, "DELETE FROM changelog WHERE id = $1", migration.Id)
	if err != nil {
		return
	}
	_, err = transaction.ExecContext(ctx, migration.RevertScript)
	if err != nil {
		return
	}
	err = transaction.Commit()
	return
}
