package main

import (
	"bufio"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"unicode"

	_ "modernc.org/sqlite"
)

type rule struct {
	net  *net.IPNet
	name string
}

type host struct {
	name string
	ipv4 net.IP
}

func main() {
	var (
		dbPath  = flag.String("db", "inventory.db", "Pfad zur SQLite Datenbank")
		file    = flag.String("file", "", "Pfad zur Datei mit 'IPv4-CIDR<space>Group Name' Zeilen")
		verbose = flag.Bool("v", true, "Verbose Log")
	)
	flag.Parse()

	if *file == "" {
		log.Fatal("-file ist erforderlich und muss auf eine Mapping-Datei verweisen")
	}

	rules, err := readRules(*file)
	if err != nil {
		log.Fatalf("Regeln laden: %v", err)
	}
	if len(rules) == 0 {
		log.Fatal("Keine gültigen Regeln gefunden")
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA foreign_keys=ON;`); err != nil {
		log.Fatalf("PRAGMA: %v", err)
	}

	hosts, err := loadHosts(db)
	if err != nil {
		log.Fatalf("Hosts laden: %v", err)
	}
	if *verbose {
		log.Printf("Hosts geladen: %d, Regeln: %d", len(hosts), len(rules))
	}

	// Compute memberships
	memberships := make(map[string]map[string]struct{}) // host -> set(group)
	for _, h := range hosts {
		if h.ipv4 == nil {
			continue
		}
		for _, r := range rules {
			if r.net.Contains(h.ipv4) {
				if memberships[h.name] == nil {
					memberships[h.name] = make(map[string]struct{})
				}
				memberships[h.name][r.name] = struct{}{}
			}
		}
	}

	// Persist
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Tx begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	ensureGroupStmt, err := tx.Prepare(`INSERT OR IGNORE INTO groups(name) VALUES(?)`)
	if err != nil {
		log.Fatalf("prep ensureGroup: %v", err)
	}
	defer ensureGroupStmt.Close()

	insertHGStmt, err := tx.Prepare(`INSERT OR IGNORE INTO host_groups(host, grp) VALUES(?, ?)`)
	if err != nil {
		log.Fatalf("prep host_groups: %v", err)
	}
	defer insertHGStmt.Close()

	var groupsEnsured = make(map[string]struct{})
	inserts := 0
	for host, gs := range memberships {
		for g := range gs {
			if _, ok := groupsEnsured[g]; !ok {
				if _, err := ensureGroupStmt.Exec(g); err != nil {
					log.Fatalf("ensure group %q: %v", g, err)
				}
				groupsEnsured[g] = struct{}{}
			}
			if _, err := insertHGStmt.Exec(host, g); err != nil {
				log.Fatalf("insert host_groups %s -> %s: %v", host, g, err)
			}
			inserts++
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Printf("Zuweisung abgeschlossen. Hosts: %d, neue/ignorierte Mitgliedschaften verarbeitet: %d, Gruppen: %d\n", len(hosts), inserts, len(groupsEnsured))
}

func readRules(path string) ([]rule, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []rule
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		cidr, grp, ok := splitFirstWS(raw)
		if !ok {
			return nil, fmt.Errorf("zeile %d: erwartetes Format 'CIDR<space>Group Name'", lineNo)
		}
		ip, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("zeile %d: ungültiges CIDR %q: %w", lineNo, cidr, err)
		}
		if ip.To4() == nil {
			return nil, fmt.Errorf("zeile %d: nur IPv4-CIDR erlaubt (%q)", lineNo, cidr)
		}
		grp = strings.TrimSpace(grp)
		if grp == "" {
			return nil, fmt.Errorf("zeile %d: Gruppenname fehlt", lineNo)
		}
		out = append(out, rule{net: ipnet, name: grp})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("keine Regeln gefunden")
	}
	return out, nil
}

func splitFirstWS(s string) (first, rest string, ok bool) {
	i := indexFirstWS(s)
	if i < 0 {
		return "", "", false
	}
	return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:]), true
}

func indexFirstWS(s string) int {
	for i, r := range s {
		if unicode.IsSpace(r) {
			return i
		}
	}
	return -1
}

func loadHosts(db *sql.DB) ([]host, error) {
	rows, err := db.Query(`SELECT name, ipv4 FROM hosts WHERE ipv4 IS NOT NULL AND ipv4 <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []host
	for rows.Next() {
		var n, v4 string
		if err := rows.Scan(&n, &v4); err != nil {
			return nil, err
		}
		ip := net.ParseIP(v4)
		if ip != nil {
			ip = ip.To4()
		}
		res = append(res, host{name: n, ipv4: ip})
	}
	return res, rows.Err()
}
