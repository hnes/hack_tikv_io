
go build -o hack_go.so -buildmode=c-shared hack_go_sched.go hack_go.go
gcc -g -shared -fPIC hack.c -o hack.so -ldl -lpthread
