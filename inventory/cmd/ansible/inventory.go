package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	_ "modernc.org/sqlite"
)

type HostVars struct {
	AnsibleHost string                 `json:"ansible_host,omitempty"`
	IPv4        string                 `json:"ipv4,omitempty"`
	IPv6        string                 `json:"ipv6,omitempty"`
	CNAMEs      []string               `json:"cnames,omitempty"`
	Extra       map[string]interface{} `json:"-"`
}

type Group struct {
	Hosts []string               `json:"hosts"`
	Vars  map[string]interface{} `json:"vars,omitempty"`
}

func main() {
	var (
		dbPath       = flag.String("db", "ansible.db", "Pfad zur SQLite Datenbank")
		list         = flag.Bool("list", false, "Vollständiges Inventory ausgeben")
		host         = flag.String("host", "", "Host-Variablen für einen Host ausgeben")
		includeEmpty = flag.Bool("include-empty-groups", false, "Leere Gruppen mit ausgeben")
	)
	flag.Parse()

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	_, _ = db.Exec(`PRAGMA foreign_keys = ON;`)

	switch {
	case *list:
		if err := outputList(db, *includeEmpty); err != nil {
			log.Fatalf("ansible: %v", err)
		}
	case *host != "":
		hv, err := getHostVars(db, *host)
		if err != nil {
			log.Fatalf("hostvars: %v", err)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(hv); err != nil {
			log.Fatalf("encode json: %v", err)
		}
	default:
		fmt.Fprintln(os.Stderr, "Verwendung: --list oder --host <name>")
		os.Exit(1)
	}
}

func outputList(db *sql.DB, includeEmpty bool) error {
	hostvars, allHosts, err := collectHostvars(db)
	if err != nil {
		return err
	}
	groups, err := collectGroups(db)
	if err != nil {
		return err
	}

	out := make(map[string]interface{})
	meta := map[string]interface{}{"hostvars": flattenHostvars(hostvars)}
	out["_meta"] = meta

	sort.Strings(allHosts)
	out["all"] = map[string]interface{}{
		"hosts": allHosts,
		"vars":  map[string]interface{}{},
	}

	ungroupedHosts, err := computeUngrouped(allHosts, groups)
	if err != nil {
		return err
	}
	out["ungrouped"] = map[string]interface{}{
		"hosts": ungroupedHosts,
	}

	for gname, g := range groups {
		if len(g.Hosts) == 0 && !includeEmpty {
			continue
		}
		sort.Strings(g.Hosts)
		item := map[string]interface{}{"hosts": g.Hosts}
		if len(g.Vars) > 0 {
			item["vars"] = g.Vars
		}
		out[gname] = item
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func collectHostvars(db *sql.DB) (map[string]HostVars, []string, error) {
	rows, err := db.Query(`SELECT name, COALESCE(ipv4,''), COALESCE(ipv6,'') FROM hosts WHERE disabled = 0`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	hostvars := make(map[string]HostVars)
	var allHosts []string

	for rows.Next() {
		var name, ipv4, ipv6 string
		if err := rows.Scan(&name, &ipv4, &ipv6); err != nil {
			return nil, nil, err
		}
		cnames, err := cnamesFor(db, name)
		if err != nil {
			return nil, nil, err
		}
		ansibleHost := ipv4
		if ansibleHost == "" {
			ansibleHost = ipv6
		}
		hv := HostVars{
			AnsibleHost: ansibleHost,
			IPv4:        ipv4,
			IPv6:        ipv6,
			CNAMEs:      cnames,
			Extra:       map[string]interface{}{},
		}
		extra, err := extraHostVars(db, name)
		if err != nil {
			return nil, nil, err
		}
		for k, v := range extra {
			hv.Extra[k] = v
		}
		hostvars[name] = hv
		allHosts = append(allHosts, name)
	}
	return hostvars, allHosts, rows.Err()
}

func collectGroups(db *sql.DB) (map[string]Group, error) {
	out := make(map[string]Group)

	rows, err := db.Query(`
SELECT g.name, hg.host, COALESCE(h.disabled,0)
FROM groups g
LEFT JOIN host_groups hg ON hg.grp = g.name
LEFT JOIN hosts h ON h.name = hg.host
ORDER BY g.name, hg.host`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var gname, host sql.NullString
		var disabled int
		if err := rows.Scan(&gname, &host, &disabled); err != nil {
			return nil, err
		}
		key := gname.String
		g := out[key]
		if host.Valid && host.String != "" && disabled == 0 {
			g.Hosts = append(g.Hosts, host.String)
		}
		out[key] = g
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	vr, err := db.Query(`SELECT grp, key, value FROM group_vars`)
	if err != nil {
		return nil, err
	}
	defer vr.Close()

	for vr.Next() {
		var grp, key, val string
		if err := vr.Scan(&grp, &key, &val); err != nil {
			return nil, err
		}
		g := out[grp]
		if g.Vars == nil {
			g.Vars = make(map[string]interface{})
		}
		g.Vars[key] = val
		out[grp] = g
	}
	return out, vr.Err()
}

func computeUngrouped(allHosts []string, groups map[string]Group) ([]string, error) {
	inGroup := make(map[string]bool)
	for _, g := range groups {
		for _, h := range g.Hosts {
			inGroup[h] = true
		}
	}
	var res []string
	for _, h := range allHosts {
		if !inGroup[h] {
			res = append(res, h)
		}
	}
	sort.Strings(res)
	return res, nil
}

func getHostVars(db *sql.DB, name string) (map[string]interface{}, error) {
	row := db.QueryRow(`SELECT COALESCE(ipv4,''), COALESCE(ipv6,'') FROM hosts WHERE name = ? AND disabled = 0`, name)
	var ipv4, ipv6 string
	if err := row.Scan(&ipv4, &ipv6); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]interface{}{}, nil
		}
		return nil, err
	}
	cnames, err := cnamesFor(db, name)
	if err != nil {
		return nil, err
	}
	ansibleHost := ipv4
	if ansibleHost == "" {
		ansibleHost = ipv6
	}
	out := map[string]interface{}{
		"ansible_host": ansibleHost,
	}
	if ipv4 != "" {
		out["ipv4"] = ipv4
	}
	if ipv6 != "" {
		out["ipv6"] = ipv6
	}
	if len(cnames) > 0 {
		out["cnames"] = cnames
	}
	extra, err := extraHostVars(db, name)
	if err != nil {
		return nil, err
	}
	for k, v := range extra {
		out[k] = v
	}
	return out, nil
}

func cnamesFor(db *sql.DB, canonical string) ([]string, error) {
	rows, err := db.Query(`SELECT alias FROM cnames WHERE canonical = ? ORDER BY alias`, canonical)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var alias string
		if err := rows.Scan(&alias); err != nil {
			return nil, err
		}
		res = append(res, alias)
	}
	return res, rows.Err()
}

func extraHostVars(db *sql.DB, host string) (map[string]interface{}, error) {
	rows, err := db.Query(`SELECT key, value FROM host_vars WHERE host = ?`, host)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]interface{})
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func flattenHostvars(h map[string]HostVars) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{})
	for host, hv := range h {
		m := map[string]interface{}{
			"ansible_host": hv.AnsibleHost,
		}
		if hv.IPv4 != "" {
			m["ipv4"] = hv.IPv4
		}
		if hv.IPv6 != "" {
			m["ipv6"] = hv.IPv6
		}
		if len(hv.CNAMEs) > 0 {
			m["cnames"] = hv.CNAMEs
		}
		for k, v := range hv.Extra {
			m[k] = v
		}
		out[host] = m
	}
	return out
}

/*
var groupNameRe = regexp.MustCompile(`[^A-Za-z0-9_]+`)

func sanitizeGroup(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	s = groupNameRe.ReplaceAllString(s, "_")
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return s
}*/
