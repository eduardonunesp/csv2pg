package cmd

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/andybalholm/crlf"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

func dbConnect() (*sql.DB, error) {
	sslMode := "?sslmode=disable"

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s%s",
		userFlag, passwdFlag, hostFlag, portFlag, dbFlag, sslMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func removeFilenameExt(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func normalizeHeader(header string) string {
	normalizedHeader := strings.ReplaceAll(header, " ", "")

	if strings.ToLower(normalizedHeader) == "order" {
		normalizedHeader = fmt.Sprintf("_%s", normalizedHeader)
	}

	return normalizedHeader
}

func normalizeColumn(column string) string {
	if column == "" {
		return "''"
	} else if !unicode.IsPrint(rune(column[0])) {
		return "''"
	} else {
		column = strings.Replace(column, "'", "''", -1)
		return fmt.Sprintf("'%s'", column)
	}
}

func createTable(tx *sql.Tx, fileName string, headers []string) error {
	var fields []string
	tableName := removeFilenameExt(fileName)

	for _, header := range headers {
		fields = append(fields, fmt.Sprintf("%s TEXT", normalizeHeader(header)))
	}

	sqlString := fmt.Sprintf(`
		CREATE TABLE %s (
			%s
		)
	`, tableName, strings.Join(fields, ",\n"))

	if verboseFlag {
		fmt.Println(fmt.Sprintf("Creating table %s", tableName))
	}

	_, err := tx.Query(sqlString)

	return err
}

func insertTable(tx *sql.Tx, fileName string, headers, lines []string) error {
	var fields []string
	var rows []string
	tableName := removeFilenameExt(fileName)

	for _, header := range headers {
		fields = append(fields, normalizeHeader(header))
	}

	for _, col := range lines {
		rows = append(rows, normalizeColumn(col))
	}

	sqlString := fmt.Sprintf(`INSERT INTO %s(%s) VALUES (%s)`,
		tableName, strings.Join(fields, ","), strings.Join(rows, ","))

	if verboseFlag {
		fmt.Println(sqlString)
	}

	_, err := tx.Query(sqlString)

	return err
}

func dropTable(db *sql.DB, fileName string) error {
	tableName := removeFilenameExt(fileName)
	_, err := db.Query(fmt.Sprintf("DROP TABLE %s", tableName))
	fmt.Printf("Droping table %s\n", tableName)
	return err
}

var verboseFlag bool
var delimiterFlag string
var hostFlag string
var portFlag string
var userFlag string
var dbFlag string
var passwdFlag string

func Execute() {
	rootCmd := &cobra.Command{
		Use:  "csv2pg <csv file>",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			csvFile, err := os.Open(args[0])

			if err != nil {
				return err
			}

			reader := csv.NewReader(crlf.NewReader(bufio.NewReader(csvFile)))
			delimiter, _ := utf8.DecodeRuneInString(delimiterFlag)
			reader.Comma = delimiter

			db, err := dbConnect()

			if err != nil {
				return err
			}

			headers, err := reader.Read()

			if err != nil {
				return err
			}

			tx, err := db.Begin()

			if err != nil {
				return err
			}

			fileName := path.Base(args[0])
			err = createTable(tx, fileName, headers)

			if err != nil {
				tx.Rollback()
				return err
			}

			err = tx.Commit()

			if err != nil {
				tx.Rollback()
				return err
			}

			tx, err = db.Begin()
			for {
				line, err := reader.Read()

				if err == io.EOF {
					break
				} else if err != nil {
					tx.Rollback()
					return err
				}

				err = insertTable(tx, fileName, headers, line)
				if err != nil {
					tx.Rollback()
					dropTable(db, fileName)
					return err
				}
			}

			err = tx.Commit()

			if err != nil {
				tx.Rollback()
				dropTable(db, fileName)
				return err
			}

			fmt.Println("Done")

			return nil
		},
	}

	rootCmd.Flags().BoolVarP(&verboseFlag, "verbose", "v", false, "verbose output")
	rootCmd.Flags().StringVarP(&delimiterFlag, "delimiter", "d", ",", "csv delimiter char")

	rootCmd.Flags().StringVarP(&hostFlag, "host", "H", "localhost", "postgres host")
	rootCmd.Flags().StringVarP(&portFlag, "port", "P", "5432", "postgres port")
	rootCmd.Flags().StringVarP(&userFlag, "user", "U", "postgres", "postgres user")
	rootCmd.Flags().StringVarP(&dbFlag, "db", "B", "", "postgres database")
	rootCmd.Flags().StringVarP(&passwdFlag, "passwd", "W", ",", "postgres user password")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
