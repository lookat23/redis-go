package main
import (
	"fmt"
	"net"
	"errors"
	"strings"
	"strconv"
)

// 一个hash table来存放数据
// 启动一个服务来解析客户端传过来的命令数据
// 根据客户端的命令操作hash table，然后返回数据

const (
	HOST = "127.0.0.1"
	PORT = "26379"
)


// 临时hash table
var dict = make(map[string]string)

func getRequestCommand(conn net.Conn) (string, error) {
	// 先不管效率，一个一个字节来
	cmd := ""
	buf := make([]byte, 1)
	for {
		reqLen, err := conn.Read(buf)
		if err != nil {
			return "",err
		}
		if reqLen != 1 {
			return "", errors.New("read request length err")
		}

		if buf[0] == '\n' {	// TODO 只判断\n是不对的
			break
		} else {
			cmd += string(buf[0])
		}
	}
	cmd = strings.Trim(cmd, "\r\n")
	return cmd, nil
}

func handleCommandSet(arrCmd []string) error {
	dict[arrCmd[1]] = arrCmd[2]
	fmt.Printf("set %s is %s", arrCmd[1], arrCmd[2])

	return nil
}

func handleCommandGet(conn net.Conn, arrCmd []string) error {
	fmt.Printf("get %s is %s", arrCmd[1], dict[arrCmd[1]])
	value := dict[arrCmd[1]]
	// fmt.Println(dict)
	// 返回给客户端
	// ${内容长度}\r\n{内容}\r\n
	cmd := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
	conn.Write([]byte(cmd))

	return nil
}

func handleRequest(conn net.Conn) {
	// 先通过\r\n 获取到一条完整的命令
	defer conn.Close()
	cmd, err := getRequestCommand(conn)
	if err != nil {
		panic(err)
		return
	}

	fmt.Println("cmd=", cmd)

	arrCmd := strings.Split(cmd, " ")
	if len(arrCmd) == 0 {
		return
	}

	// 先只测试set 和 get
	switch(arrCmd[0]) {
	case "get":
		handleCommandGet(conn, arrCmd)
	case "set":
		handleCommandSet(arrCmd)
	}

}
	 

func net_handler() error {
	l, err := net.Listen("tcp", HOST + ":" + PORT)
	if err != nil {
		return err
	}

	defer l.Close()

	fmt.Println("Listening on " + HOST + ":" + PORT)
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go handleRequest(conn)
	}

}


func main() {
	err := net_handler()
	panic(err)
}