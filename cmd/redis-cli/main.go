package main
import (
	"fmt"
	"errors"
	"os"
	"github.com/jessevdk/go-flags"
	"net"
	"strconv"
	"strings"
)

type Options struct {
	Hostport int `long:"port" short:"p" description:"host port" default:"16379"`
	Repeat uint64 `long:"repeat" short:"r" description:"repeat times" default:"1"`
	Dbnum int `long:"dbnum" short:"n" description:"db num" default:"0"`
	Auth string `long:"auth" short:"a" description:"auth password" default:""`
	Interactive bool `long:"interactive" short:"i" description:""`
	Command string `long:"command" short:"m" description:""`
}

var options Options

var parser = flags.NewParser(&options, flags.Default)


func parseOption(config *Config) {
	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {				
					os.Exit(0)
				} else {
					os.Exit(1)
				}
	}

	config.Hostip = "127.0.0.1"
	config.Hostport = options.Hostport
	config.Repeat = options.Repeat
	config.Dbnum = options.Dbnum
	config.Interactive = options.Interactive
	config.Auth = options.Auth
	config.Command = options.Command

	// fmt.Printf("%+v\n", config)
}

type Config struct {
	Hostip string
	Hostport int
	Repeat uint64
	Dbnum int
	Interactive bool
	Auth string
	Command string
}

var config Config

func repl() {
	fmt.Println("repl")
}

// func testSocket() {
// 	conn, err := net.Dial("tcp", "127.0.0.1:6379")
// 	if err != nil {
// 		fmt.Println("net dial err!")
// 		os.Exit(1);
// 	}
// 	defer conn.Close()
	
// 	cmd := fmt.Sprintf("SELECT %d\r\n", 0)
// 	buf := make([]byte, 1)
// 	conn.Write([]byte(cmd))
// 	conn.Read(buf)
// 	fmt.Println(buf)

// }

func  cliConnect() (net.Conn, error){
	hostandport := fmt.Sprintf("%s:%d", config.Hostip, config.Hostport)
	conn, err := net.Dial("tcp", hostandport)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func selectDb(conn net.Conn) error {
	if config.Dbnum == 0 {
		return nil
	}

	cmd := fmt.Sprintf("SELECT %d\r\n", config.Dbnum)
	// todo write 返回来一个n，文档没写需不需要发送长度不全时，再继续调用Write继续发送
	// 之后再看看
	_, err := conn.Write([]byte(cmd)) 
	if err != nil {
		return err
	}

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	read_type := buf[0]
	if read_type != '+' {
		return errors.New("read data err when select db")
	}

	return nil
}

func generateSendCommand() (string, error) {
	// 从代码上看到的结构
	//{参数1} {参数2} {参数3}...\r\n
	cmd := ""
	cmd = config.Command
	cmd += "\r\n"
	
	return cmd, nil
}

func cliReadLine(conn net.Conn) (string,error) {
	// 读一个字节，如果是-1返回空
	// 如果是 0 或者 \n 表示读完
	line := ""
	for {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if (err != nil) {
			return "",err
		}

		c := buf[0]
		if c == 0xff {
			return "", errors.New("response -1")
		} else if (c == 0x0 ) {
			break;
		} else if c == '\n'{
			line += string(c)
			break;
		} else {
			line += string(c)
		}
	}
	line = strings.Trim(line, "\r\n")
	return line, nil
}

func cliReadBulkReply(conn net.Conn) (error) {
	// {内容长度}\r\n{内容}\n
	str_len, err := cliReadLine(conn)
	if err != nil {
		return err
	}

	len, err := strconv.Atoi(str_len)
	if err != nil {
		return err
	}

	if len == -1 {
		fmt.Println("(nil)")
		return nil
	}

	if len == 0 {
		return errors.New("content length is 0")
	}

	buf := make([]byte, len)
	_, err = conn.Read(buf)
	if err != nil {
		return err
	}
	crlf := make([]byte, 2)
	_, err = conn.Read(crlf)
	if err != nil {
		return err
	}

	fmt.Println(string(buf))

	return nil
}

func cliReadReply(conn net.Conn) (error) {
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err != nil {
		return err
	}
	read_type := buf[0]
	switch read_type {
	case '-':
		fmt.Printf("(error) ")
		fmt.Printf("\n will call cliReadSingleLineReply\n")
	case '+':
		fmt.Printf("\n will call cliReadSingleLineReply\n")
	case ':':
		fmt.Printf("\n will call cliReadSingleLineReply\n")
	case '$':
		//fmt.Printf("\n will call cliReadBulkReply\n")
		err = cliReadBulkReply(conn)
		if err != nil {
			panic(err)
		}
	case '*':
		fmt.Printf("\n will call cliReadMultiBulkReply\n")
	default:
		fmt.Printf("\n protocal error, got %c as reply type byte\n", read_type)
	}
	return nil
}

func cliSendCommand() (error) {
	// 连接服务器
	conn, err := cliConnect()
	if err != nil {
		return err
	}

	// 选择db
	err = selectDb(conn)
	if err != nil {
		return err
	}

	// 拼接发送命令
	cmd, err := generateSendCommand();
	if err != nil {
		return err
	}


	// 发送命令
	_, err = conn.Write([]byte(cmd)) 
	if err != nil {
		return err
	}

	// 接受和解析命令
	err = cliReadReply(conn)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	parseOption(&config)

	if len(os.Args) == 1 || config.Interactive {
		repl()
		os.Exit(0)
	}

	if err := cliSendCommand(); err != nil {
		fmt.Println("send command err: ", err)
	}

	// fmt.Println(config)
	// fmt.Printf("%+v\n", config)
}