cd registry
go run main.go handler.go api.go config.go &
cd ../graft
go run main.go 0 &
go run main.go 1 &
go run main.go 2 &
cd ..
