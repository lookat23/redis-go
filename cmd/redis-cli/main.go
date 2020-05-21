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

const (
	REDIS_CMD_INLINE = 1
	REDIS_CMD_BULK = 2
	REDIS_CMD_MULTIBULK = 4
)

type redisCommand struct {name string; arity int; flags int;}

var arrCmdTable = []redisCommand {
	{"auth",2,REDIS_CMD_INLINE},
	{"get",2,REDIS_CMD_INLINE},
	{"set",3,REDIS_CMD_BULK},
	{"setnx",3,REDIS_CMD_BULK},
	{"append",3,REDIS_CMD_BULK},
	{"substr",4,REDIS_CMD_INLINE},
	{"del",-2,REDIS_CMD_INLINE},
	{"exists",2,REDIS_CMD_INLINE},
	{"incr",2,REDIS_CMD_INLINE},
	{"decr",2,REDIS_CMD_INLINE},
	{"rpush",3,REDIS_CMD_BULK},
	{"lpush",3,REDIS_CMD_BULK},
	{"rpop",2,REDIS_CMD_INLINE},
	{"lpop",2,REDIS_CMD_INLINE},
	{"brpop",-3,REDIS_CMD_INLINE},
	{"blpop",-3,REDIS_CMD_INLINE},
	{"llen",2,REDIS_CMD_INLINE},
	{"lindex",3,REDIS_CMD_INLINE},
	{"lset",4,REDIS_CMD_BULK},
	{"lrange",4,REDIS_CMD_INLINE},
	{"ltrim",4,REDIS_CMD_INLINE},
	{"lrem",4,REDIS_CMD_BULK},
	{"rpoplpush",3,REDIS_CMD_BULK},
	{"sadd",3,REDIS_CMD_BULK},
	{"srem",3,REDIS_CMD_BULK},
	{"smove",4,REDIS_CMD_BULK},
	{"sismember",3,REDIS_CMD_BULK},
	{"scard",2,REDIS_CMD_INLINE},
	{"spop",2,REDIS_CMD_INLINE},
	{"srandmember",2,REDIS_CMD_INLINE},
	{"sinter",-2,REDIS_CMD_INLINE},
	{"sinterstore",-3,REDIS_CMD_INLINE},
	{"sunion",-2,REDIS_CMD_INLINE},
	{"sunionstore",-3,REDIS_CMD_INLINE},
	{"sdiff",-2,REDIS_CMD_INLINE},
	{"sdiffstore",-3,REDIS_CMD_INLINE},
	{"smembers",2,REDIS_CMD_INLINE},
	{"zadd",4,REDIS_CMD_BULK},
	{"zincrby",4,REDIS_CMD_BULK},
	{"zrem",3,REDIS_CMD_BULK},
	{"zremrangebyscore",4,REDIS_CMD_INLINE},
	{"zmerge",-3,REDIS_CMD_INLINE},
	{"zmergeweighed",-4,REDIS_CMD_INLINE},
	{"zrange",-4,REDIS_CMD_INLINE},
	{"zrank",3,REDIS_CMD_BULK},
	{"zrevrank",3,REDIS_CMD_BULK},
	{"zrangebyscore",-4,REDIS_CMD_INLINE},
	{"zcount",4,REDIS_CMD_INLINE},
	{"zrevrange",-4,REDIS_CMD_INLINE},
	{"zcard",2,REDIS_CMD_INLINE},
	{"zscore",3,REDIS_CMD_BULK},
	{"incrby",3,REDIS_CMD_INLINE},
	{"decrby",3,REDIS_CMD_INLINE},
	{"getset",3,REDIS_CMD_BULK},
	{"randomkey",1,REDIS_CMD_INLINE},
	{"select",2,REDIS_CMD_INLINE},
	{"move",3,REDIS_CMD_INLINE},
	{"rename",3,REDIS_CMD_INLINE},
	{"renamenx",3,REDIS_CMD_INLINE},
	{"keys",2,REDIS_CMD_INLINE},
	{"dbsize",1,REDIS_CMD_INLINE},
	{"ping",1,REDIS_CMD_INLINE},
	{"echo",2,REDIS_CMD_BULK},
	{"save",1,REDIS_CMD_INLINE},
	{"bgsave",1,REDIS_CMD_INLINE},
	{"rewriteaof",1,REDIS_CMD_INLINE},
	{"bgrewriteaof",1,REDIS_CMD_INLINE},
	{"shutdown",1,REDIS_CMD_INLINE},
	{"lastsave",1,REDIS_CMD_INLINE},
	{"type",2,REDIS_CMD_INLINE},
	{"flushdb",1,REDIS_CMD_INLINE},
	{"flushall",1,REDIS_CMD_INLINE},
	{"sort",-2,REDIS_CMD_INLINE},
	{"info",1,REDIS_CMD_INLINE},
	{"mget",-2,REDIS_CMD_INLINE},
	{"expire",3,REDIS_CMD_INLINE},
	{"expireat",3,REDIS_CMD_INLINE},
	{"ttl",2,REDIS_CMD_INLINE},
	{"slaveof",3,REDIS_CMD_INLINE},
	{"debug",-2,REDIS_CMD_INLINE},
	{"mset",-3,REDIS_CMD_MULTIBULK},
	{"msetnx",-3,REDIS_CMD_MULTIBULK},
	{"monitor",1,REDIS_CMD_INLINE},
	{"multi",1,REDIS_CMD_INLINE},
	{"exec",1,REDIS_CMD_INLINE},
	{"discard",1,REDIS_CMD_INLINE},
	{"hset",4,REDIS_CMD_MULTIBULK},
	{"hget",3,REDIS_CMD_BULK},
	{"hdel",3,REDIS_CMD_BULK},
	{"hlen",2,REDIS_CMD_INLINE},
	{"hkeys",2,REDIS_CMD_INLINE},
	{"hvals",2,REDIS_CMD_INLINE},
	{"hgetall",2,REDIS_CMD_INLINE},
	{"hexists",3,REDIS_CMD_BULK},
}

var CMDTABLE map[string]redisCommand

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

func generateSendCommandInline(arrCmd []string) (string, error) {
	cmd := strings.Join(arrCmd, " ")
	cmd += "\r\n"
	return cmd, nil
}

func generateSendCommandBulk(arrCmd []string) (string, error) {
	cmd := ""
	paramCount := len(arrCmd) 
	for i, v := range arrCmd {
		if i != 0 {
			cmd += " "
		}

		if i == paramCount-1 {// 最后一个参数
			cmd += strconv.Itoa(len(v)) + "\r\n"
		} 

		cmd += v
	}
	cmd += "\r\n"

	return cmd, nil
}

func generateSendCommand() (string, error) {
	// 从代码上看到的结构
	// 如果是INLINE 发送参数是 {参数1} {参数2} {参数3}...\r\n
	// 如果是BULK 最后一个参数将会需要先传参数长度  发送参数是 {参数1} {参数2} {参数3长度}\r\n{参数3}\r\n
	var err error
	arrCmd := strings.Fields(config.Command)
	
	if len(arrCmd) == 0 {
		return "", errors.New("命令不能为空")
	}

	cmdName := arrCmd[0]
	cmdAttribute, ok := CMDTABLE[cmdName];
	if !ok {
		return "", errors.New(fmt.Sprintf("Unknown command '%s'", cmdName))
	}

	if len(arrCmd) != cmdAttribute.arity {
		return "", errors.New(fmt.Sprintf("Wrong number of arguments for '%s'", cmdName))
	}

	cmd := ""
	switch cmdAttribute.flags {
	case REDIS_CMD_INLINE:
		cmd, err = generateSendCommandInline(arrCmd)
	case REDIS_CMD_BULK:
		cmd, err = generateSendCommandBulk(arrCmd)
	case REDIS_CMD_MULTIBULK:
		return "", errors.New("MULTIBULK 类型还没实现")
	default:
		return "", errors.New("不存在的类型")
	}

	return cmd, err
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
	fmt.Println(string(read_type))
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

	CMDTABLE = make(map[string]redisCommand)
	for _, v := range arrCmdTable {
		CMDTABLE[v.name] = v
	}

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