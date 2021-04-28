package redis

import (
	"net"
	"bufio"
	"strings"
	"strconv"
	"HFish/utils/try"
	"HFish/core/report"
	"HFish/utils/log"
	"HFish/utils/is"
	"HFish/core/rpc/client"
	"fmt"
	"HFish/core/pool"
	"time"
)

var kvData map[string]string

func Start(addr string) {
	kvData = make(map[string]string)

	//建立socket，监听端口
	netListen, _ := net.Listen("tcp", addr)

	defer netListen.Close()

	wg, poolX := pool.New(10)
	defer poolX.Release()

	for {
		wg.Add(1)
		poolX.Submit(func() {
			time.Sleep(time.Second * 2)

			conn, err := netListen.Accept()

			if err != nil {
				log.Pr("Redis", "127.0.0.1", "Redis 连接失败", err)
			}

			arr := strings.Split(conn.RemoteAddr().String(), ":")

			// 判断是否为 RPC 客户端
			var id string

			if is.Rpc() {
				id = client.ReportResult("REDIS", "", arr[0], conn.RemoteAddr().String()+" 已经连接", "0")
			} else {
				id = strconv.FormatInt(report.ReportRedis(arr[0], "本机", conn.RemoteAddr().String()+" 已经连接"), 10)
			}

			log.Pr("Redis", arr[0], "已经连接")

			go handleConnection(conn, id)

			wg.Done()
		})
	}
}

//处理 Redis 连接
func handleConnection(conn net.Conn, id string) {

	fmt.Println("redis ", id)

	for {
		str := parseRESP(conn)

		switch value := str.(type) {
		case string:
			if is.Rpc() {
				go client.ReportResult("REDIS", "", "", "&&"+str.(string), id)
			} else {
				go report.ReportUpdateRedis(id, "&&"+str.(string))
			}

			if len(value) == 0 {
				goto end
			}
			conn.Write([]byte(value))
		case []string:
			if value[0] == "SET" || value[0] == "set" {
				// 模拟 redis set

				try.Try(func() {
					key := string(value[1])
					val := string(value[2])
					kvData[key] = val

					if is.Rpc() {
						go client.ReportResult("REDIS", "", "", "&&"+value[0]+" "+value[1]+" "+value[2], id)
					} else {
						go report.ReportUpdateRedis(id, "&&"+value[0]+" "+value[1]+" "+value[2])
					}

				}).Catch(func() {
					// 取不到 key 会异常
				})

				conn.Write([]byte("+OK\r\n"))
			} else if value[0] == "GET" || value[0] == "get" {
				try.Try(func() {
					// 模拟 redis get
					key := string(value[1])
					val := string(kvData[key])

					valLen := strconv.Itoa(len(val))
					str := "$" + valLen + "\r\n" + val + "\r\n"

					if is.Rpc() {
						go client.ReportResult("REDIS", "", "", "&&"+value[0]+" "+value[1], id)
					} else {
						go report.ReportUpdateRedis(id, "&&"+value[0]+" "+value[1])
					}

					conn.Write([]byte(str))
				}).Catch(func() {
					conn.Write([]byte("+OK\r\n"))
				})
			} else if value[0] == "info" {
				try.Try(func() {
					// 模拟 redis info
					redis_info := '$3312\r\n# Server\r\nredis_version:5.0.10\r\nredis_git_sha1:1c047b68\r\nredis_git_dirty:0\r\nredis_build_id:76de97c74f6945e9\r\nredis_mode:standalone\r\nos:Windows  \r\narch_bits:64\r\nmultiplexing_api:WinSock_IOCP\r\natomicvar_api:pthread-mutex\r\nprocess_id:17932\r\nrun_id:2e5854c121c940595d66fe178e28505ad3dec02e\r\ntcp_port:6379\r\nuptime_in_seconds:41\r\nuptime_in_days:0\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:9004067\r\nexecutable:C:\\Users\\FigoHu\\Desktop\\fsdownload\\Redis-x64-5.0.10\\redis-server.exe\r\nconfig_file:C:\\Users\\FigoHu\\Desktop\\fsdownload\\Redis-x64-5.0.10\\redis.windows.conf\r\n\r\n# Clients\r\nconnected_clients:1\r\nclient_recent_max_input_buffer:0\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:0\r\n\r\n# Memory\r\nused_memory:723568\r\nused_memory_human:706.61K\r\nused_memory_rss:660408\r\nused_memory_rss_human:644.93K\r\nused_memory_peak:723568\r\nused_memory_peak_human:706.61K\r\nused_memory_peak_perc:109.56%\r\nused_memory_overhead:710358\r\nused_memory_startup:660408\r\nused_memory_dataset:13210\r\nused_memory_dataset_perc:20.92%\r\nallocator_allocated:33272'
							
					if is.Rpc() {
						go client.ReportResult("REDIS", "", "", "&&"+value[0]+" "+value[1], id)
					} else {
						go report.ReportUpdateRedis(id, "&&"+value[0]+" "+value[1])
					}
					
					conn.Write([]byte(redis_info))
				}).Catch(func() {
					conn.Write([]byte("+OK\r\n"))
				})
			} else {
				try.Try(func() {
					if is.Rpc() {
						go client.ReportResult("REDIS", "", "", "&&"+value[0]+" "+value[1], id)
					} else {
						go report.ReportUpdateRedis(id, "&&"+value[0]+" "+value[1])
					}
				}).Catch(func() {
					if is.Rpc() {
						go client.ReportResult("REDIS", "", "", "&&"+value[0], id)
					} else {
						go report.ReportUpdateRedis(id, "&&"+value[0])
					}
				})

				conn.Write([]byte("+OK\r\n"))
			}
			break
		default:

		}
	}
end:
	conn.Close()
}

// 解析 Redis 协议
func parseRESP(conn net.Conn) interface{} {
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return ""
	}

	cmdType := string(line[0])
	cmdTxt := strings.Trim(string(line[1:]), "\r\n")

	switch cmdType {
	case "*":
		count, _ := strconv.Atoi(cmdTxt)
		var data []string
		for i := 0; i < count; i++ {
			line, _ := r.ReadString('\n')
			cmd_txt := strings.Trim(string(line[1:]), "\r\n")
			c, _ := strconv.Atoi(cmd_txt)
			length := c + 2
			str := ""
			for length > 0 {
				block, _ := r.Peek(length)
				if length != len(block) {

				}
				r.Discard(length)
				str += string(block)
				length -= len(block)
			}

			data = append(data, strings.Trim(str, "\r\n"))
		}
		return data
	default:
		return cmdTxt
	}
}
