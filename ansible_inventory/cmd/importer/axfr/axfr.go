package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

type HostEntry struct {
	IPv4   string
	IPv6   string
	CNAMEs []string
}

func main() {
	var err error
	var (
		dbPath = flag.String("db", "inventory.db", "Pfad zur SQLite Datenbank")
		wipe   = flag.Bool("wipe", false, "Bestehende Tabellen löschen und neu anlegen")
	)
	flag.Parse()

	hosts := make(map[string]*HostEntry)
	cnameTargets := make(map[string][]string)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "_acme") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		hostname := strings.TrimSuffix(fields[0], ".")
		recordType := fields[3]
		recordValue := strings.TrimSuffix(fields[4], ".")

		switch recordType {
		case "A":
			h := ensureHost(hosts, hostname)
			h.IPv4 = recordValue
		case "AAAA":
			h := ensureHost(hosts, hostname)
			h.IPv6 = recordValue
		case "CNAME":
			alias := hostname
			canonical := recordValue
			cnameTargets[canonical] = append(cnameTargets[canonical], alias)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Fehler beim Lesen von stdin: %v", err)
	}

	for canonical, aliases := range cnameTargets {
		if h, ok := hosts[canonical]; ok {
			h.CNAMEs = append(h.CNAMEs, aliases...)
		}
	}
	for name, h := range hosts {
		if h.IPv4 == "" && h.IPv6 == "" {
			delete(hosts, name)
		}
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("DB open: %v", err)
	}
	defer db.Close()

	if err := initSchema(db, *wipe); err != nil {
		log.Fatalf("Schema: %v", err)
	}

	// Merke bestehenden Disabled-Status der Hosts, damit er beim Re-Import erhalten bleibt
	disabledMap := make(map[string]bool)
	rows, err := db.Query(`SELECT name, disabled FROM hosts`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name string
			var disabled int
			if err := rows.Scan(&name, &disabled); err == nil {
				disabledMap[name] = disabled != 0
			}
		}
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Begin Tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM cnames`); err != nil {
		log.Fatalf("clear cnames: %v", err)
	}
	if _, err := tx.Exec(`DELETE FROM hosts`); err != nil {
		log.Fatalf("clear hosts: %v", err)
	}

	insHost, err := tx.Prepare(`INSERT INTO hosts(name, ipv4, ipv6) VALUES(?, ?, ?)`)
	if err != nil {
		log.Fatalf("prep ins host: %v", err)
	}
	defer insHost.Close()

	insCname, err := tx.Prepare(`INSERT INTO cnames(alias, canonical) VALUES(?, ?)`)
	if err != nil {
		log.Fatalf("prep ins cname: %v", err)
	}
	defer insCname.Close()

	for name, h := range hosts {
		if _, err := insHost.Exec(name, nullIfEmpty(h.IPv4), nullIfEmpty(h.IPv6)); err != nil {
			log.Fatalf("insert host %s: %v", name, err)
		}
		for _, alias := range h.CNAMEs {
			if _, err := insCname.Exec(alias, name); err != nil {
				log.Fatalf("insert cname %s->%s: %v", alias, name, err)
			}
		}
	}

	// Re-appliziere Disabled-Status für bekannte Hosts
	upd, err := tx.Prepare(`UPDATE hosts SET disabled=1 WHERE name=?`)
	if err == nil {
		defer upd.Close()
		for n, dis := range disabledMap {
			if dis {
				_, _ = upd.Exec(n)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Printf("Import abgeschlossen. Hosts: %d, CNAMEs: %d\n", len(hosts), countCnames(hosts))
}

func ensureHost(m map[string]*HostEntry, name string) *HostEntry {
	if h, ok := m[name]; ok {
		return h
	}
	h := &HostEntry{}
	m[name] = h
	return h
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func countCnames(m map[string]*HostEntry) int {
	n := 0
	for _, h := range m {
		n += len(h.CNAMEs)
	}
	return n
}

func initSchema(db *sql.DB, wipe bool) error {
	if wipe {
		_, _ = db.Exec(`DROP VIEW IF EXISTS class_c;
DROP TABLE IF EXISTS host_vars;
DROP TABLE IF EXISTS group_vars;
DROP TABLE IF EXISTS host_groups;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS cnames;
DROP TABLE IF EXISTS hosts;`)
	}
	_, err := db.Exec(`
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS hosts(
  name TEXT PRIMARY KEY,
  ipv4 TEXT,
  ipv6 TEXT,
  disabled INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cnames(
  alias TEXT PRIMARY KEY,
  canonical TEXT NOT NULL,
  FOREIGN KEY(canonical) REFERENCES hosts(name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cnames_canonical ON cnames(canonical);

CREATE TABLE IF NOT EXISTS groups(
  name TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS host_groups(
  host  TEXT NOT NULL,
  grp   TEXT NOT NULL,
  PRIMARY KEY(host, grp),
  FOREIGN KEY(host) REFERENCES hosts(name) ON DELETE CASCADE,
  FOREIGN KEY(grp)  REFERENCES groups(name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_host_groups_grp  ON host_groups(grp);
CREATE INDEX IF NOT EXISTS idx_host_groups_host ON host_groups(host);

CREATE TABLE IF NOT EXISTS group_vars(
  grp   TEXT NOT NULL,
  key   TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY(grp, key),
  FOREIGN KEY(grp) REFERENCES groups(name) ON DELETE CASCADE
);

-- NEU: host_vars
CREATE TABLE IF NOT EXISTS host_vars(
  host TEXT NOT NULL,
  key  TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY(host, key),
  FOREIGN KEY(host) REFERENCES hosts(name) ON DELETE CASCADE
);

-- View: class_c
CREATE VIEW IF NOT EXISTS class_c AS
select
    rtrim(rtrim(ipv4, '0123456789'), '.') as class_c_network,
    count(*) as cnt
from hosts
where ipv4 is not null
group by class_c_network
order by cnt desc;
`)
	if err != nil {
		return err
	}
	// Migration: add disabled column if it does not exist yet
	_, _ = db.Exec(`ALTER TABLE hosts ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0;`)
	return nil
}
