MODULE=cert.at/go_inventory

all: 	bin/axfr \
		bin/inventory \
		bin/net2grp

deps:
	go get modernc.org/sqlite

clean:
	@rm -f bin/axfr
	@rm -f bin/inventory
	@rm -f bin/net2grp

bin:
	@mkdir -p bin


bin/inventory: cmd/ansible bin
	go build -o bin/inventory cmd/inventory/inventory.go

bin/axfr: cmd/importer/axfr/axfr.go bin
	go build -o bin/axfr cmd/importer/axfr/axfr.go

bin/net2grp: cmd/importer/net2grp/net2grp.go bin
	go build -o bin/net2grp cmd/importer/net2grp/net2grp.go


