# Go Inventory Tools (SQLite-backed)

Tools:
- axfr2sqlite: Importiert AXFR/dig Output in SQLite (hosts, cnames)
- inventory: Dynamic Inventory mit Gruppen und host_vars

## Inventory erstellen aus Daten von einem AXFR
Beispiel:
```bash
dig @ns.example.tld example.tld AXFR | ./axfr2sqlite -db ansible.db -wipe
```

## Inventory ausgeben im Ansible-Format
```bash
./ansible --db ansible.db --list
```

