package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"url-shortener/internal/storage"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3" // init sqlite3 driver
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const operation = "storage.sqlite.New"
	db, err := sql.Open("sqlite3", storagePath)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	statement, err := db.Prepare(`
		CREATE TABLE IF NOT EXISTS url(
			id INTEGER PRIMARY KEY,
			alias TEXT NOT NULL UNIQUE,
			url TEXT NOT NULL);
		CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	_, err = statement.Exec()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave, alias string) (int64, error) {
	const operation = "storage.sqlite.SaveURL"

	statement, err := s.db.Prepare("INSERT INTO url(url, alias) VALUES(?, ?)")

	if err != nil {
		return 0, fmt.Errorf("%s: %w", operation, err)
	}

	res, err := statement.Exec(urlToSave, alias)

	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
			return 0, fmt.Errorf("%s: %w", operation, storage.ErrURLExists)
		}
		return 0, fmt.Errorf("%s: %w", operation, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s: failed to get last insert id: %w", operation, err)
	}

	return id, nil
}

func (s *Storage) GetURL(alias string) (string, error) {
	const operation = "storage.sqlite.GetURL"
	statement, err := s.db.Prepare("SELECT url from url WHERE alias = ?")
	if err != nil {
		return "", fmt.Errorf("%s: prepare statement: %w", operation, err)
	}
	var resUrl string
	err = statement.QueryRow(alias).Scan(&resUrl)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", storage.ErrURLNotFound
		}
		return "", fmt.Errorf("%s: execute statement %w", operation, err)

	}
	return resUrl, nil

}

// TODO: DeleteURL implementation
func (s *Storage) DeleteURL(alias string) error {
	const operation = "storage.sqlite.DeleteURL"
	statement, err := s.db.Prepare("DELETE FROM url WHERE alias = ?")
	if err != nil {
		return fmt.Errorf("%s: prepare statement: %w", operation, err)
	}
	res, err := statement.Exec(alias)
	if err != nil {
		return fmt.Errorf("%s: failed to delete url: %w", operation, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to get rows affected: %w", operation, err)
	}
	if rowsAffected == 0 {
		return storage.ErrURLNotFound
	}
	return nil
}
