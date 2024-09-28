package migrago

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path/filepath"
	"slices"
)

// MigrationService constructor
func NewMigrationService(configFile, scriptPath string, fs fs.FS, conn *sql.DB) MigrationService {
	return MigrationService{
		configFile: configFile,
		scriptPath: scriptPath,
		fs:         fs,
		conn:       conn,
	}
}

type Migration struct {
	Id           string
	Script       string
	RevertScript string
	Checksum     string
}

type MigrationService struct {
	configFile string
	scriptPath string
	fs         fs.FS
	conn       *sql.DB
}

// readConfigFile reads the configuration file (JSON) and returns a list of migration IDs
func (m MigrationService) readConfigFile() ([]string, error) {
	f, err := m.fs.Open(m.configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer f.Close()

	var migrationIds []string
	if err := json.NewDecoder(f).Decode(&migrationIds); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}
	return migrationIds, nil
}

// readFileContent reads the content of a file
func readFileContent(fs fs.FS, path string) (string, error) {
	f, err := fs.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer f.Close()

	fileContent, err := io.ReadAll(f)
	if err != nil {
		return "", fmt.Errorf("failed to read file content: %w", err)
	}
	return string(fileContent), nil
}

// extractMigration extracts a migration and calculates the checksum of the script
func (m MigrationService) extractMigration(migrationId string) (Migration, error) {
	script, err := readFileContent(m.fs, filepath.Join(m.scriptPath, migrationId+".sql"))
	if err != nil {
		return Migration{}, err
	}

	revertScript, err := readFileContent(m.fs, filepath.Join(m.scriptPath, migrationId+".revert.sql"))
	if err != nil {
		return Migration{}, err
	}

	checksum := md5.Sum([]byte(script))
	log.Printf("checksum: %v", checksum[:])
	return Migration{
		Id:           migrationId,
		Script:       script,
		RevertScript: revertScript,
		Checksum:     hex.EncodeToString(checksum[:]),
	}, nil
}

// getMigrations retrieves the migrations from the configuration file and reads their contents in parallel
func (m MigrationService) getMigrations() (migrations map[string]Migration, err error) {
	var migrationIds []string
	migrationIds, err = m.readConfigFile()
	if err != nil {
		return
	}

	migrations = make(map[string]Migration)
	for _, v := range migrationIds {
		migrations[v], err = m.extractMigration(v)
		if err != nil {
			return
		}
	}
	return
}

// prepareDatabase creates the changelog table if it does not exist
func (m MigrationService) prepareDatabase(ctx context.Context) error {
	_, err := m.conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS changelog (
		id VARCHAR(255) PRIMARY KEY,
		checksum VARCHAR(255) NOT NULL,
		installedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		revertscript TEXT
	)`)
	return err
}

// executeSingleMigration executes a single migration and updates the local list of existing migrations
func (m MigrationService) executeSingleMigration(ctx context.Context, migration Migration) error {
	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Execute the migration script
	_, err = tx.ExecContext(ctx, migration.Script)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to execute migration script: %w", err)
	}

	// Insert the migration into the changelog
	_, err = tx.ExecContext(ctx, `INSERT INTO changelog (id, checksum, revertscript) VALUES ($1, $2, $3)`, migration.Id, migration.Checksum, migration.RevertScript)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert into changelog: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// revertSingleMigration executes the revert script and removes the migration from the changelog
func (m MigrationService) revertSingleMigration(ctx context.Context, migration Migration) error {
	tx, err := m.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, migration.RevertScript)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to execute revert script: %w", err)
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM changelog WHERE id = $1`, migration.Id)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete from changelog: %w", err)
	}

	return tx.Commit()
}

// checkExistingChangelogs checks the existing migrations in the database
func (m MigrationService) checkExistingChangelogs(ctx context.Context, existingMigrations *[]Migration, migrations map[string]Migration) error {
	var notReverted bool
	copyExistingMigrations := make([]Migration, len(*existingMigrations))
	copy(copyExistingMigrations, *existingMigrations)
	for _, dbMigration := range copyExistingMigrations {
		if migration, ok := migrations[dbMigration.Id]; ok {
			if dbMigration.Checksum != migration.Checksum {
				return fmt.Errorf("checksum mismatch for migration %s: file: %s, database: %s", dbMigration.Id, migration.Checksum, dbMigration.Checksum)
			}
			notReverted = true
		} else if notReverted {
			return errors.New("not revertable migration found")
		} else {
			if err := m.revertSingleMigration(ctx, dbMigration); err != nil {
				return err
			}
		}
	}
	return nil
}

// getExistingMigrations retrieves the already executed migrations from the database
func (m MigrationService) getExistingMigrations(ctx context.Context) ([]Migration, error) {
	rows, err := m.conn.QueryContext(ctx, `SELECT id, checksum, revertscript FROM changelog ORDER BY installedAt DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var existingMigrations []Migration
	for rows.Next() {
		var dbMigration Migration
		if err := rows.Scan(&dbMigration.Id, &dbMigration.Checksum, &dbMigration.RevertScript); err != nil {
			return nil, err
		}
		existingMigrations = append(existingMigrations, dbMigration)
	}
	return existingMigrations, nil
}

// ExecuteMigration orchestrates the migration execution process
func (m MigrationService) ExecuteMigration(ctx context.Context) error {
	// Step 1: Prepare the database by creating the changelog table
	if err := m.prepareDatabase(ctx); err != nil {
		return err
	}

	// Step 2: Get all migrations from the configuration
	migrations, err := m.getMigrations()
	if err != nil {
		return err
	}

	// Step 3: Retrieve the already executed migrations from the database
	existingMigrations, err := m.getExistingMigrations(ctx)
	if err != nil {
		return err
	}

	// Step 4: Check existing changelogs for potential reverts or checksum mismatches
	if err := m.checkExistingChangelogs(ctx, &existingMigrations, migrations); err != nil {
		return err
	}

	// Step 5: Execute pending migrations
	for _, migration := range migrations {
		// Skip migrations that are already applied
		if slices.ContainsFunc(existingMigrations, func(e Migration) bool { return e.Id == migration.Id }) {
			continue
		}
		// Execute new migrations and update the local list
		if err := m.executeSingleMigration(ctx, migration); err != nil {
			return err
		}
	}
	return nil
}
