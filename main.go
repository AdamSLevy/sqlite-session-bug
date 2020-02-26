package main

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"github.com/AdamSLevy/sqlitechangeset"
)

func run() error {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		return fmt.Errorf("sqlite open: %w", err)
	}
	defer conn.Close()

	fmt.Println("Creating tables...")
	err = sqlitex.ExecScript(conn, `
CREATE TABLE main.t(id INTEGER PRIMARY KEY, a,b,c);
CREATE TABLE temp.t(id INTEGER PRIMARY KEY, a,b,c) --- remove this line or rename the table to avoid issue;
`)
	if err != nil {
		return err
	}
	fmt.Println("Starting session on main...")
	sess, err := conn.CreateSession("main")
	if err != nil {
		return err
	}
	defer sess.Delete()
	// An empty string will pass NULL to sqlite3session_attach and allow
	// the bug.
	var attach string
	// Any of these prevent the bug.
	//attach = "*"
	//attach = "main.*"
	//attach = "main.t"
	fmt.Printf("Attaching to %s ...\n", attach)
	if err := sess.Attach(attach); err != nil {
		return err
	}

	fmt.Println("Inserting into main.t ...")
	commit := sqlitex.Save(conn)
	err = sqlitex.ExecScript(conn, `
INSERT INTO main.t(a,b,c) VALUES (1,2,3);
INSERT INTO temp.t(a,b,c) VALUES (1,2,3) --- This line avoids the conflict, but the changeset doesn't apply to main.t like expected;
`)
	if err != nil {
		return err
	}
	commit(&err)

	sql, err := sqlitechangeset.SessionToSQL(conn, sess)
	if err != nil {
		return err
	}
	fmt.Println("changeset:", sql)

	chgset := bytes.NewBuffer(nil)
	if err := sess.Changeset(chgset); err != nil {
		return err
	}
	invrt := bytes.NewBuffer(nil)
	invrtCp := bytes.NewBuffer(nil)
	if err := sqlite.ChangesetInvert(
		io.MultiWriter(invrt, invrtCp), chgset); err != nil {
		return err
	}

	sql, err = sqlitechangeset.ToSQL(conn, invrtCp)
	if err != nil {
		return err
	}
	fmt.Println("inverted changeset:", sql)

	/*// Dropping the temp table avoids the issue...
	fmt.Println("Dropping temp table...")
		err = sqlitex.ExecScript(conn, `
	DROP TABLE temp.t;
	`)
		if err != nil {
			return err
		}
	*/

	fmt.Println("applying inverted changeset...")
	conflictFn := func(cType sqlite.ConflictType,
		iter sqlite.ChangesetIter) sqlite.ConflictAction {
		fmt.Println("ConflictType:", cType)
		sql, err := sqlitechangeset.ConflictChangesetIterToSQL(conn, iter)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Conflict:", sql)
		return sqlite.SQLITE_CHANGESET_ABORT
	}

	if err := conn.ChangesetApply(invrt, filterFn, conflictFn); err != nil {
		return err
	}
	fmt.Println("done.")

	sql, err = sqlitechangeset.SessionToSQL(conn, sess)
	if err != nil {
		return err
	}
	if len(sql) > 0 {
		return fmt.Errorf("Error: current session changeset not empty: %v", sql)
	}
	fmt.Println("success")

	return nil
}

func filterFn(string) bool { return true }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
