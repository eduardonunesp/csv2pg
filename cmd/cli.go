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

var (
	verboseFlag   bool
	forceFlag     bool
	delimiterFlag string
	hostFlag      string
	portFlag      string
	userFlag      string
	dbFlag        string
	passwdFlag    string
	sslModeFlag   string
	schemaFlag    string
)

func dbConnect() (*sql.DB, error) {
	sslMode := fmt.Sprintf("?sslmode=%s", sslModeFlag)
	schema := fmt.Sprintf("&search_path=%s", schemaFlag)

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s%s%s",
		userFlag,
		passwdFlag,
		hostFlag,
		portFlag,
		dbFlag,
		sslMode,
		schema,
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

func normalizeColumn(header, column string) string {
	if column == "" {
		return "''"
	} else if !unicode.IsPrint(rune(column[0])) {
		if verboseFlag {
			fmt.Printf("Invalid value found on column %s we're going to ignore it\n", header)
		}
		return "''"
	} else {
		column = strings.Replace(column, "'", "''", -1)
		return fmt.Sprintf("'%s'", column)
	}
}

func createTable(tx *sql.Tx, fileName string, headers []string) error {
	var fields []string
	tableName := removeFilenameExt(fileName)

	fmt.Printf("Creating table '%s' with headers %s\n", tableName, headers)

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

	for i, col := range lines {
		header := headers[i]
		rows = append(rows, normalizeColumn(header, col))
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

			if forceFlag {
				if err := dropTable(db, fileName); err != nil {
					return err
				}
			}

			if err := createTable(tx, fileName, headers); err != nil {
				if err := tx.Rollback(); err != nil {
					return err
				}

				return err
			}

			if err := tx.Commit(); err != nil {
				tx.Rollback()
				return err
			}

			tx, err = db.Begin()

			for {
				line, err := reader.Read()

				if err == io.EOF {
					break
				} else if err != nil {
					if err := tx.Rollback(); err != nil {
						return err
					}

					return err
				}

				if err := insertTable(tx, fileName, headers, line); err != nil {
					if err := tx.Rollback(); err != nil {
						return err
					}

					if err := dropTable(db, fileName); err != nil {
						return err
					}

					return err
				}
			}

			if err := tx.Commit(); err != nil {
				if err := tx.Rollback(); err != nil {
					return err
				}

				if err := dropTable(db, fileName); err != nil {
					return err
				}

				return err
			}

			fmt.Println("Done")

			return nil
		},
	}

	rootCmd.Flags().BoolVarP(&verboseFlag, "verbose", "v", false, "verbose output")
	rootCmd.Flags().BoolVarP(&forceFlag, "force", "f", false, "force command to run and drop table if needed")
	rootCmd.Flags().StringVarP(&delimiterFlag, "delimiter", "d", ",", "csv delimiter char")

	rootCmd.Flags().StringVarP(&hostFlag, "host", "H", "localhost", "postgres host")
	rootCmd.Flags().StringVarP(&portFlag, "port", "P", "5432", "postgres port")
	rootCmd.Flags().StringVarP(&userFlag, "user", "U", "postgres", "postgres user")
	rootCmd.Flags().StringVarP(&dbFlag, "db", "B", "", "postgres database")
	rootCmd.Flags().StringVarP(&passwdFlag, "passwd", "W", ",", "postgres user password")
	rootCmd.Flags().StringVarP(&schemaFlag, "schema", "S", "public", "postgres schema")
	rootCmd.Flags().StringVarP(&sslModeFlag, "sslmode", "M", "disable", "postgres SSL mode")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
