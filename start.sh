# 先把注册中心跑起来
cd registry
go run main.go handler.go api.go config.go &
# 把各个节点跑起来
cd ../graft
go run main.go 0 &
go run main.go 1 &
go run main.go 2 &
cd ..
