mkdir -p ./coverage
go clean -testcache
go test -coverprofile=./coverage/coverage.out ./...
go tool cover -html=./coverage/coverage.out -o ./coverage/coverage.html