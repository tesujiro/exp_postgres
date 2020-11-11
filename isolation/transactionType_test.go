package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

type state int

const (
	TRN_NO state = iota
	TRN_BEGIN
	TRN_IN_PROGRESS
	TRN_COMMIT
	TRN_ROLLBACK
)

const test_id int = 1

type preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

func setup(t *testing.T) (*sql.DB, error) {
	connStr := "user=tesujiro dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("sql.Open error:%v", err)
		return nil, err
	}

	sqlStmt := `
		create table account (id integer not null primary key, name text, balance integer);
		`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		t.Fatalf("create table error: %v\n", err)
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin error: %v\n", err)
		return nil, err
	}
	ins_stmt, err := tx.Prepare("insert into account (id, name, balance) values ($1, $2, $3);")
	if err != nil {
		t.Fatalf("tx.Prepare error: %v\n", err)
		return nil, err
	}
	defer ins_stmt.Close()

	for i := 0; i < 10; i++ {
		_, err = ins_stmt.Exec(i, fmt.Sprintf("User%03d", i), i*100)
		if err != nil {
			t.Fatalf("insert error: %v\n", err)
			return nil, err
		}
	}
	tx.Commit()

	return db, nil
}

func check(t *testing.T, pr preparer, want int) {
	sel_stmt, err := pr.Prepare("select balance from account where id = $1;")
	if err != nil {
		t.Fatalf("tx.Prepare error: %v\n", err)
		return
	}
	defer sel_stmt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var balance int
	err = sel_stmt.QueryRowContext(ctx, fmt.Sprintf("%d", test_id)).Scan(&balance)
	if err != nil {
		t.Fatalf("stmt.QueryRow().Scan() error: %v\n", err)
		return
	}

	if balance != want {
		t.Errorf("got:%v want %v\n", balance, want)
	}
}

func TestTransactionTypes(t *testing.T) {

	db, err := setup(t)
	if err != nil {
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		db.ExecContext(ctx, "drop table account")
		db.Close()
	}()

	tests := []struct {
		scene int
		state state
		sql   string
		want  int
	}{
		{scene: 1, state: TRN_BEGIN},
		{scene: 2, state: TRN_BEGIN},
		{scene: 1, state: TRN_IN_PROGRESS, sql: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;", want: test_id * 100},
		{scene: 2, state: TRN_IN_PROGRESS, sql: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;", want: test_id * 100},
		{scene: 0, state: TRN_NO, want: test_id * 100},
		{scene: 1, state: TRN_IN_PROGRESS, sql: fmt.Sprintf("update account set balance = balance+1 where id = %v;", test_id), want: test_id*100 + 1},
		{scene: 2, state: TRN_IN_PROGRESS, want: test_id * 100}, // Repeatable Read
		{scene: 0, state: TRN_NO, want: test_id * 100},
		{scene: 1, state: TRN_COMMIT, sql: fmt.Sprintf("update account set balance = balance+1 where id = %v;", test_id), want: test_id*100 + 2},
		{scene: 0, state: TRN_NO, want: test_id*100 + 2},
		{scene: 2, state: TRN_IN_PROGRESS, want: test_id * 100}, // Repeatable Read
		//{scene: 2, state: TRN_COMMIT, sql: fmt.Sprintf("update account set balance = %v where id = %v;", test_id*100, test_id), want: test_id * 100},
	}

	txs := make(map[int]*sql.Tx)
	var tx *sql.Tx

	for test_no, test := range tests {
		fmt.Println(test_no, test)

		if test.state == TRN_NO {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err = db.ExecContext(ctx, test.sql)
			if err != nil {
				t.Errorf("%q: %s\n", err, test.sql)
				return
			}
			check(t, db, test.want)

		} else {
			tx = txs[test.scene]
			if test.state == TRN_BEGIN {
				tx_tmp, err := db.Begin()
				if err != nil {
					t.Fatalf("db.Begin error: %v\n", err)
					return
				}
				txs[test.scene] = tx_tmp
				tx = tx_tmp
			}

			if test.sql != "" {
				stmt, err := tx.Prepare(test.sql)
				if err != nil {
					t.Fatalf("%q: %s\n", err, test.sql)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				_, err = stmt.ExecContext(ctx)
				if err != nil {
					t.Fatalf("%q: %s\n", err, test.sql)
				}
			}
			if test.state != TRN_BEGIN {
				check(t, tx, test.want)
			}
			if test.state == TRN_COMMIT {
				err = tx.Commit()
				if err != nil {
					t.Fatalf("tx.Commit error: %v\n", err)
					return
				}
			}
		}
	}
	for _, tx = range txs {
		tx.Rollback()
	}
}
