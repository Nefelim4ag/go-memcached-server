package memcachedprotocol

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"nefelim4ag/go-memcached-server/memstore"
	"os"
	"strconv"
	"strings"
	"time"
)

func sendError(client *bufio.Writer) error {
	client.Write([]byte("ERROR\r\n"))
	client.Flush()

	return nil
}

func sendClientError(client *bufio.Writer, msg string) error {
	client.Write([]byte(fmt.Sprintf("CLIENT_ERROR %s\r\n", msg)))
	client.Flush()

	return fmt.Errorf("CLIENT_ERROR %s", msg)
}

func sendServerError(client *bufio.Writer, msg string) error {
	client.Write([]byte(fmt.Sprintf("SERVER_ERROR %s\r\n", msg)))
	client.Flush()

	return fmt.Errorf("SERVER_ERROR %s", msg)
}

func CommandAscii(magic byte, client *bufio.ReadWriter, store *memstore.SharedStore) error {
		request, err := client.Reader.ReadString('\n')
		if err != nil {
            return err
        }

		request = strings.TrimSpace(request)
		request_parsed := strings.Split(request, " ")
		command := string(magic) + request_parsed[0]
		args := request_parsed[1:]

		log.Println(command, args)

		switch command {
		case "quit":
			return fmt.Errorf("quit")

		case "version":
			client.Writer.Write([]byte("VERSION 1.6.2\r\n"))
			client.Writer.Flush()
			return nil

		case "verbosity":
			if len(args) > 0 {
				if args[len(args) - 1] == "noreply" {
					return nil
				}
				switch args[0] {
				case "0", "1":
					client.Writer.Write([]byte("OK\r\n"))
					client.Writer.Flush()
					return nil
				}
			}
			return sendError(client.Writer)

		case "set", "add", "replace":
			return set_add_replace(command, args, client, store)

		case "append", "prepend":
			return append_prepend(command, args, client, store)

		case "cas":
			return cas(args, client, store)

		case "get", "gets":
			if len(args) == 0 {
				return sendError(client.Writer)
			}

			for _, v := range args {
				value, exist := store.Get(v)
				if !exist {
					continue
				}
				var entry memcachedEntry = value.(memcachedEntry)
				// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
				// <data block>\r\n
				if command == "get" {
					resp := fmt.Sprintf("VALUE %s %d %d\r\n", entry.key, entry.flags, entry.len)
					client.Writer.Write([]byte(resp))
				} else {
					resp := fmt.Sprintf("VALUE %s %d %d %d\r\n", entry.key, entry.flags, entry.len, entry.cas)
					client.Writer.Write([]byte(resp))
				}
				client.Writer.Write(entry.value)
				client.Writer.Write([]byte("\r\n"))
			}

			client.Writer.Write([]byte("END\r\n"))
			client.Writer.Flush()

			return nil
		case "delete": //delete <key> [noreply]\r\n
		    switch len(args) {
			case 0:
				return sendError(client.Writer)
			case 1:
				key := args[0]
				_, exist := store.Get(key)
				if !exist{
					client.Writer.Write([]byte("NOT_FOUND\r\n"))
					client.Writer.Flush()
					return nil
				}

				store.Delete(key)
				client.Writer.Write([]byte("DELETED\r\n"))
				client.Writer.Flush()
			default:
				if args[1] != "noreply" || len(args) > 2 {
                    sendError(client.Writer)
                }
				key := args[0]
				store.Delete(key)
			}

			return nil

		// incr|decr <key> <value> [noreply]\r\n
		case "incr", "decr":
			return incr_decr(command, args, client, store)

		case "flush_all":
			store.Flush()
			if len(args) > 0 && args[len(args) - 1] == "noreply" {
				return nil
			}
			client.Writer.Write([]byte("OK\r\n"))
			client.Writer.Flush()
			return nil
		case "stats":
			return stats(args, client, store)

		default:
			return sendError(client.Writer)
		}

		// err = HandleCommand(clientRequest, client)
		// if err!= nil {
		// 	log.Println(clientRequest, err)
		// 	client.Writer.Write([]byte("ERROR\r\n"))
		// 	client.Writer.Flush()
		// 	return err
		// }
}

type memcachedEntry struct {
	key string
	flags uint32
	exptime uint32
	len uint64
	cas uint64
	value []byte
}

// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
func set_add_replace(command string, args []string, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	key := args[0]
	flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	exptime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	bytes, err := strconv.ParseUint(args[3], 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	entry := memcachedEntry{
		key: key,
		flags: uint32(flags),
		exptime: uint32(exptime),
		len: bytes,
		cas: uint64(time.Now().UnixNano()),
		value: make([]byte, bytes),
	}

	if bytes > 0 {
		_, err := io.ReadFull(client.Reader, entry.value)
		if err != nil {
			return sendClientError(client.Writer, err.Error())
		}
	}
	// Read message last \r\n possibly
	client.Reader.ReadString('\n')

	_, exist := store.Get(key)
	switch command {
		case "add":
			if exist {
				if args[len(args) - 1] == "noreply" {
					return nil
				}
				client.Writer.Write([]byte("NOT_STORED\r\n"))
				client.Writer.Flush()
				return nil
			}
		case "replace":
			if !exist {
                if args[len(args) - 1] == "noreply" {
                    return nil
                }
                client.Writer.Write([]byte("NOT_STORED\r\n"))
                client.Writer.Flush()
                return nil
            }
    }

	err = store.Set(entry.key, entry, entry.len)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	client.Writer.Write([]byte("STORED\r\n"))
	client.Writer.Flush()

	return nil
}

// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
func append_prepend(command string, args []string, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	key := args[0]
	flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	exptime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	bytes, err := strconv.ParseUint(args[3], 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	entry := memcachedEntry{
		key: key,
		flags: uint32(flags),
		exptime: uint32(exptime),
		len: bytes,
		cas: uint64(time.Now().UnixNano()),
		value: make([]byte, bytes),
	}

	if bytes > 0 {
		_, err := io.ReadFull(client.Reader, entry.value)
		if err != nil {
			return sendClientError(client.Writer, err.Error())
		}
	}
	// Read message last \r\n possibly
	client.Reader.ReadString('\n')

	v, exist := store.Get(key)
	if !exist {
		if args[len(args) - 1] == "noreply" {
			return nil
		}
		client.Writer.Write([]byte("NOT_STORED\r\n"))
		client.Writer.Flush()
		return nil
	}

	entry.flags = v.(memcachedEntry).flags
	entry.exptime = v.(memcachedEntry).exptime

	switch command {
		case "append":
			old_data := v.(memcachedEntry).value
			new_data := entry.value
			entry.value = append(old_data, new_data[:]...)
		case "prepend":
			old_data := v.(memcachedEntry).value
			new_data := entry.value
			entry.value = append(new_data, old_data[:]...)
    }
	entry.len = uint64(len(entry.value))

	err = store.Set(entry.key, entry, entry.len)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	client.Writer.Write([]byte("STORED\r\n"))
	client.Writer.Flush()

	return nil
}

// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
func cas(args []string, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	key := args[0]
	flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	exptime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return sendClientError(client.Writer, err.Error())
	}
	bytes, err := strconv.ParseUint(args[3], 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}
	cas, err := strconv.ParseUint(args[4], 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	entry := memcachedEntry{
		key: key,
		flags: uint32(flags),
		exptime: uint32(exptime),
		len: bytes,
		cas: uint64(time.Now().UnixNano()),
		value: make([]byte, bytes),
	}

	if bytes > 0 {
		_, err := io.ReadFull(client.Reader, entry.value)
		if err != nil {
			return sendClientError(client.Writer, err.Error())
		}
	}
	// Read message last \r\n possibly
	client.Reader.ReadString('\n')

	// Racy implementation item can be modified between get & set
	v, exist := store.Get(key)
	if !exist {
		if args[len(args) - 1] == "noreply" {
			return nil
		}

		client.Writer.Write([]byte("NOT_FOUND\r\n"))
		client.Writer.Flush()
		return nil
	}

	if v.(memcachedEntry).cas != cas {
		if args[len(args) - 1] == "noreply" {
			return nil
		}

		client.Writer.Write([]byte("EXISTS\r\n"))
		client.Writer.Flush()
		return nil
	}

	err = store.Set(entry.key, entry, entry.len)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	client.Writer.Write([]byte("STORED\r\n"))
	client.Writer.Flush()

	return nil
}

func incr_decr(command string, args []string, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	key := args[0]
	change, err := strconv.ParseUint(args[1], 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	_v, exist := store.Get(key)
	if !exist {
		client.Writer.Write([]byte("NOT_FOUND\r\n"))
		client.Writer.Flush()
		return nil
	}

	v := _v.(memcachedEntry)
	old_value, err := strconv.ParseUint(string(v.value), 10, 64)
	if err!= nil {
		return sendClientError(client.Writer, err.Error())
	}

	const MaxUint = ^uint64(0)
	const MinUint = uint64(0)

	new_value := uint64(0)
	if command == "incr" {
		if MaxUint - old_value < change {
			new_value = MaxUint
		} else {
			new_value = old_value + change
		}
	} else {
		if MinUint + old_value < change {
			new_value = MinUint
		} else {
			new_value = old_value - change
		}
	}

	v.value = []byte(fmt.Sprintf("%d", new_value))
	v.len = uint64(len(v.value))
	store.Set(key, v, v.len)

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	client.Writer.Write([]byte(fmt.Sprintf("%d\r\n", new_value)))
	client.Writer.Flush()

	return nil
}

func stats(args []string, client *bufio.ReadWriter, store *memstore.SharedStore) error {
	if len(args) == 0 {
		client.Writer.Write([]byte(fmt.Sprintf("STAT pid %d\r\n", os.Getpid())))
		// STAT uptime 6710
		client.Writer.Write([]byte(fmt.Sprintf("STAT time %d\r\n", time.Now().Unix())))
		client.Writer.Write([]byte("STAT version 1.6.19\r\n"))
		client.Writer.Write([]byte("END\r\n"))
        client.Writer.Flush()
        return nil
	}

	switch args[0] {
	case "noreply":
		return sendError(client.Writer)
	case "items":
		return fmt.Errorf("not supported")
	case "slabs":
		return fmt.Errorf("not supported")
	case "sizes":
		return fmt.Errorf("not supported")
	default:
		return fmt.Errorf("not supported")
	}
	// STAT libevent 2.1.12-stable
	// STAT pointer_size 64
	// STAT rusage_user 0.508315
	// STAT rusage_system 0.202256
	// STAT max_connections 1024
	// STAT curr_connections 2
	// STAT total_connections 20
	// STAT rejected_connections 0
	// STAT connection_structures 3
	// STAT response_obj_oom 0
	// STAT response_obj_count 1
	// STAT response_obj_bytes 65536
	// STAT read_buf_count 8
	// STAT read_buf_bytes 131072
	// STAT read_buf_bytes_free 49152
	// STAT read_buf_oom 0
	// STAT reserved_fds 20
	// STAT cmd_get 1
	// STAT cmd_set 8
	// STAT cmd_flush 2
	// STAT cmd_touch 0
	// STAT cmd_meta 0
	// STAT get_hits 1
	// STAT get_misses 0
	// STAT get_expired 0
	// STAT get_flushed 0
	// STAT delete_misses 1
	// STAT delete_hits 1
	// STAT incr_misses 1
	// STAT incr_hits 3
	// STAT decr_misses 0
	// STAT decr_hits 0
	// STAT cas_misses 0
	// STAT cas_hits 0
	// STAT cas_badval 0
	// STAT touch_hits 0
	// STAT touch_misses 0
	// STAT store_too_large 0
	// STAT store_no_memory 0
	// STAT auth_cmds 0
	// STAT auth_errors 0
	// STAT bytes_read 945
	// STAT bytes_written 482
	// STAT limit_maxbytes 2147483648
	// STAT accepting_conns 1
	// STAT listen_disabled_num 0
	// STAT time_in_listen_disabled_us 0
	// STAT threads 4
	// STAT conn_yields 0
	// STAT hash_power_level 16
	// STAT hash_bytes 524288
	// STAT hash_is_expanding 0
	// STAT slab_reassign_rescues 0
	// STAT slab_reassign_chunk_rescues 0
	// STAT slab_reassign_evictions_nomem 0
	// STAT slab_reassign_inline_reclaim 0
	// STAT slab_reassign_busy_items 0
	// STAT slab_reassign_busy_deletes 0
	// STAT slab_reassign_running 0
	// STAT slabs_moved 0
	// STAT lru_crawler_running 0
	// STAT lru_crawler_starts 15
	// STAT lru_maintainer_juggles 8743
	// STAT malloc_fails 0
	// STAT log_worker_dropped 0
	// STAT log_worker_written 0
	// STAT log_watcher_skipped 0
	// STAT log_watcher_sent 0
	// STAT log_watchers 0
	// STAT unexpected_napi_ids 0
	// STAT round_robin_fallback 0
	// STAT bytes 153
	// STAT curr_items 2
	// STAT total_items 6
	// STAT slab_global_page_pool 0
	// STAT expired_unfetched 0
	// STAT evicted_unfetched 0
	// STAT evicted_active 0
	// STAT evictions 0
	// STAT reclaimed 0
	// STAT crawler_reclaimed 0
	// STAT crawler_items_checked 3
	// STAT lrutail_reflocked 0
	// STAT moves_to_cold 6
	// STAT moves_to_warm 0
	// STAT moves_within_lru 0
	// STAT direct_reclaims 0
	// STAT lru_bumps_dropped 0
}

// func HandleCommand(request string, client *bufio.ReadWriter) error {
// 	store := store

// 	switch command {

// 	case "touch": //touch <key> <exptime> [noreply]\r\n
// 		key := args[0]
// 		exptime, err := strconv.ParseUint(args[1], 10, 32)
// 		if err != nil {
// 			return err
// 		}

// 		_v, exist := store.Get(key)
// 		v := _v.(memcachedEntry)
// 		if exptime > 0 && exist {
// 			v.exptime = uint32(exptime)
// 			store.Set(key, v, v.len)
// 			client.Writer.Write([]byte("TOUCHED\r\n"))
// 		} else {
// 			client.Writer.Write([]byte("NOT_FOUND\r\n"))
// 		}

// 	case "lru_crawler":
// 		switch args[0] {
// 		case "metadump":
// 			switch args[1] {
// 			case "all":
// 				// key=fake%2Fee49a9a0d462d1fa%2F18a6af34196%3A18a6af34253%3Afa5766e2 exp=1694013261 la=1694012361 cas=12434 fetch=no cls=12 size=1139
// 				// key=fake%2F886f3db85b3da0c2%2F18a6af60139%3A18a6af60c05%3A97e2dba9 exp=1694013435 la=1694012535 cas=12440 fetch=no cls=13 size=1420
// 				// key=fake%2Fc437f5f7aa7cb20b%2F18a6b03682a%3A18a6b03be70%3A123ad4e4 exp=1694013435 la=1694012535 cas=12439 fetch=no cls=39 size=1918339
// 				client.Writer.Write([]byte("END\r\n"))
// 			default:
//                 return fmt.Errorf("not supported")
// 			}
// 		}
// 	}
// }
