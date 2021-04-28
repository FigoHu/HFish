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
					redis_info := `# Server
redis_version:4.0.9
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:30c23efe798c5412
redis_mode:standalone
os:Linux 3.10.0-1127.10.1.el7.x86_64 x86_64
arch_bits:64
multiplexing_api:epoll
atomicvar_api:atomic-builtin
gcc_version:4.8.5
process_id:3822
run_id:c11504dea153d1d30514c7181918686be04e0712
tcp_port:6399
uptime_in_seconds:4941041
uptime_in_days:57
hz:10
lru_clock:8990540
executable:/root/redis-server
config_file:/root/redis.conf

# Clients
connected_clients:14
client_longest_output_list:0
client_biggest_input_buf:0
blocked_clients:0

# Memory
used_memory:1174184
used_memory_human:1.12M
used_memory_rss:4210688
used_memory_rss_human:4.02M
used_memory_peak:1815008
used_memory_peak_human:1.73M
used_memory_peak_perc:64.69%
used_memory_overhead:1055536
used_memory_startup:786488
used_memory_dataset:118648
used_memory_dataset_perc:30.60%
total_system_memory:16374308864
total_system_memory_human:15.25G
used_memory_lua:37888
used_memory_lua_human:37.00K
maxmemory:0
maxmemory_human:0B
maxmemory_policy:noeviction
mem_fragmentation_ratio:3.59
mem_allocator:jemalloc-4.0.3
active_defrag_running:0
lazyfree_pending_objects:0

# Persistence
loading:0
rdb_changes_since_last_save:4577
rdb_bgsave_in_progress:0
rdb_last_save_time:1614662235
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:-1
rdb_current_bgsave_time_sec:-1
rdb_last_cow_size:0
aof_enabled:0
aof_rewrite_in_progress:0
aof_rewrite_scheduled:0
aof_last_rewrite_time_sec:-1
aof_current_rewrite_time_sec:-1
aof_last_bgrewrite_status:ok
aof_last_write_status:ok
aof_last_cow_size:0

# Stats
total_connections_received:272
total_commands_processed:14062
instantaneous_ops_per_sec:0
total_net_input_bytes:2023140
total_net_output_bytes:178309125
instantaneous_input_kbps:0.00
instantaneous_output_kbps:0.00
rejected_connections:0
sync_full:0
sync_partial_ok:0
sync_partial_err:0
expired_keys:0
expired_stale_perc:0.00
expired_time_cap_reached_count:0
evicted_keys:0
keyspace_hits:7920
keyspace_misses:290
pubsub_channels:0
pubsub_patterns:0
latest_fork_usec:0
migrate_cached_sockets:0
slave_expires_tracked_keys:0
active_defrag_hits:0
active_defrag_misses:0
active_defrag_key_hits:0
active_defrag_key_misses:0

# Replication
role:master
connected_slaves:0
master_replid:29beea308bb5c4b4c2c72019aed2a5e74e8349c6
master_replid2:0000000000000000000000000000000000000000
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:0

# CPU
used_cpu_sys:1204.59
used_cpu_user:667.20
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Cluster
cluster_enabled:0

# Keyspace
db0:keys=3,expires=0,avg_ttl=0
db8:keys=2,expires=0,avg_ttl=0
					`
							
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
