# 先把注册中心跑起来
cd registry
go run . &
# 把各个节点跑起来
cd ../graft
go run main.go 0 &
go run main.go 1 &
go run main.go 2 &
cd ..
