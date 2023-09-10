package memcachedprotocol

import (
	"bufio"
	"fmt"
	"io"
	"nefelim4ag/go-memcached-server/memstore"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type ASCIIProcessor struct {
    store *memstore.SharedStore
    rb    *bufio.Reader
    wb    *bufio.Writer
}

func CreateASCIIProcessor(rb *bufio.Reader, wb *bufio.Writer, store *memstore.SharedStore) *ASCIIProcessor {
    return &ASCIIProcessor{
        store:        store,
        rb:           rb,
        wb:           wb,
    }
}

func (ctx *ASCIIProcessor) sendEnd() error {
	ctx.wb.Write([]byte("END\r\n"))

	return nil
}

func (ctx *ASCIIProcessor) sendError() error {
	ctx.wb.Write([]byte("ERROR\r\n"))

	return nil
}

func (ctx *ASCIIProcessor) sendClientError(msg string) error {
	ctx.wb.Write([]byte(fmt.Sprintf("CLIENT_ERROR %s\r\n", msg)))

	return fmt.Errorf("CLIENT_ERROR %s", msg)
}

func (ctx *ASCIIProcessor) sendServerError(msg string) error {
	ctx.wb.Write([]byte(fmt.Sprintf("SERVER_ERROR %s\r\n", msg)))

	return fmt.Errorf("SERVER_ERROR %s", msg)
}

func (ctx *ASCIIProcessor) CommandAscii() error {
		request, err := ctx.rb.ReadString('\n')
		if err != nil {
            return err
        }

		defer ctx.wb.Flush()

		request = strings.TrimSpace(request)
		request_parsed := strings.Split(request, " ")
		command := request_parsed[0]
		args := request_parsed[1:]

		if log.GetLevel() == log.DebugLevel {
			log.Debugf("cmd: %s %s", command, args)
		}

		switch command {
		case "quit":
			return fmt.Errorf("quit")

		case "version":
			ctx.wb.Write([]byte("VERSION 1.6.2\r\n"))
			return nil

		case "verbosity":
			if len(args) > 0 {
				if args[len(args) - 1] == "noreply" {
					return nil
				}
				switch args[0] {
				case "0", "1":
					ctx.wb.Write([]byte("OK\r\n"))
					return nil
				}
			}
			return ctx.sendError()

		case "set", "add", "replace":
			return ctx.set_add_replace(command, args)

		case "append", "prepend":
			return ctx.append_prepend(command, args)

		case "cas":
			return ctx.cas(args)

		case "get", "gets":
			if len(args) == 0 {
				return ctx.sendError()
			}

			for _, v := range args {
				entry, exist := ctx.store.Get(v)
				if !exist {
					continue
				}

				// VALUE <key> <Flags> <bytes> [<cas unique>]\r\n
				// <data block>\r\n
				if command == "get" {
					resp := fmt.Sprintf("VALUE %s %d %d\r\n", entry.Key, entry.Flags, entry.Size)
					ctx.wb.Write([]byte(resp))
				} else {
					resp := fmt.Sprintf("VALUE %s %d %d %d\r\n", entry.Key, entry.Flags, entry.Size, entry.Cas)
					ctx.wb.Write([]byte(resp))
				}
				ctx.wb.Write(entry.Value)
				ctx.wb.Write([]byte("\r\n"))
			}

			return ctx.sendEnd()
		case "delete": //delete <key> [noreply]\r\n
		    switch len(args) {
			case 0:
				return ctx.sendError()
			case 1:
				key := args[0]
				_, exist := ctx.store.Get(key)
				if !exist{
					ctx.wb.Write([]byte("NOT_FOUND\r\n"))
					return nil
				}

				ctx.store.Delete(key)
				ctx.wb.Write([]byte("DELETED\r\n"))
			default:
				if args[1] != "noreply" || len(args) > 2 {
                    ctx.sendError()
                }
				key := args[0]
				ctx.store.Delete(key)
			}

			return nil

		// incr|decr <key> <value> [noreply]\r\n
		case "incr", "decr":
			return ctx.incr_decr(command, args)

		case "flush_all":
			ctx.store.Flush()
			if len(args) > 0 && args[len(args) - 1] == "noreply" {
				return nil
			}
			ctx.wb.Write([]byte("OK\r\n"))
			return nil
		case "stats":
			return ctx.stats(args)

		default:
			return ctx.sendError()
		}

		// err = HandleCommand(clientRequest, client)
		// if err!= nil {
		// 	// log.Println(clientRequest, err)
		// 	ctx.wb.Write([]byte("ERROR\r\n"))
		// 	return err
		// }
}



// <command name> <key> <Flags> <ExpTime> <bytes> [noreply]\r\n
func (ctx *ASCIIProcessor) set_add_replace(command string, args []string) error {
	key := args[0]
	Flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	ExpTime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	nbytes, err := strconv.ParseUint(args[3], 10, 32)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	entry := memstore.MEntry{
		Key: key,
		Flags: uint32(Flags),
		ExpTime: uint32(ExpTime),
		Size: uint32(nbytes),
		Cas: uint64(time.Now().UnixNano()),
		Value: make([]byte, nbytes),
	}

	if nbytes > 0 {
		_, err := io.ReadFull(ctx.rb, entry.Value)
		if err != nil {
			return ctx.sendClientError(err.Error())
		}
	}
	// Read message last \r\n possibly
	ctx.rb.ReadString('\n')

	_, exist := ctx.store.Get(key)
	switch command {
		case "add":
			if exist {
				if args[len(args) - 1] == "noreply" {
					return nil
				}
				ctx.wb.Write([]byte("NOT_STORED\r\n"))
				return nil
			}
		case "replace":
			if !exist {
                if args[len(args) - 1] == "noreply" {
                    return nil
                }
                ctx.wb.Write([]byte("NOT_STORED\r\n"))
                return nil
            }
    }

	err = ctx.store.Set(entry.Key, &entry, entry.Size)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	ctx.wb.Write([]byte("STORED\r\n"))

	return nil
}

// <command name> <key> <Flags> <ExpTime> <bytes> [noreply]\r\n
func (ctx *ASCIIProcessor) append_prepend(command string, args []string) error {
	key := args[0]
	Flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	ExpTime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	nbytes, err := strconv.ParseUint(args[3], 10, 64)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	entry := memstore.MEntry{
		Key: key,
		Flags: uint32(Flags),
		ExpTime: uint32(ExpTime),
		Size: uint32(nbytes),
		Cas: uint64(time.Now().UnixNano()),
		Value: make([]byte, nbytes),
	}

	if nbytes > 0 {
		_, err := io.ReadFull(ctx.rb, entry.Value)
		if err != nil {
			return ctx.sendClientError(err.Error())
		}
	}
	// Read message last \r\n possibly
	ctx.rb.ReadString('\n')

	v, exist := ctx.store.Get(key)
	if !exist {
		if args[len(args) - 1] == "noreply" {
			return nil
		}
		ctx.wb.Write([]byte("NOT_STORED\r\n"))
		return nil
	}

	entry.Flags = v.Flags
	entry.ExpTime = v.ExpTime

	switch command {
		case "append":
			old_data := v.Value
			new_data := entry.Value
			entry.Value = append(old_data, new_data[:]...)
		case "prepend":
			old_data := v.Value
			new_data := entry.Value
			entry.Value = append(new_data, old_data[:]...)
    }
	entry.Size = uint32(len(entry.Value))

	err = ctx.store.Set(entry.Key, &entry, entry.Size)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	ctx.wb.Write([]byte("STORED\r\n"))

	return nil
}

// cas <key> <Flags> <ExpTime> <bytes> <cas unique> [noreply]\r\n
func (ctx *ASCIIProcessor) cas(args []string) error {
	if len(args) < 5 {
		return ctx.sendClientError("not enough arguments for cas")
	}

	key := args[0]
	Flags, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	ExpTime, err := strconv.ParseUint(args[2], 10, 32)
	if err != nil {
		return ctx.sendClientError(err.Error())
	}
	bytes, err := strconv.ParseUint(args[3], 10, 32)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}
	cas, err := strconv.ParseUint(args[4], 10, 64)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	entry := memstore.MEntry{
		Key: key,
		Flags: uint32(Flags),
		ExpTime: uint32(ExpTime),
		Size: uint32(bytes),
		Cas: uint64(time.Now().UnixNano()),
		Value: make([]byte, bytes),
	}

	if bytes > 0 {
		_, err := io.ReadFull(ctx.rb, entry.Value)
		if err != nil {
			return ctx.sendClientError(err.Error())
		}
	}
	// Read message last \r\n possibly
	ctx.rb.ReadString('\n')

	// Racy implementation item can be modified between get & set
	v, exist := ctx.store.Get(key)
	if !exist {
		if args[len(args) - 1] == "noreply" {
			return nil
		}

		ctx.wb.Write([]byte("NOT_FOUND\r\n"))
		return nil
	}

	if v.Cas != cas {
		if args[len(args) - 1] == "noreply" {
			return nil
		}

		ctx.wb.Write([]byte("EXISTS\r\n"))
		return nil
	}

	err = ctx.store.Set(entry.Key, &entry, entry.Size)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	ctx.wb.Write([]byte("STORED\r\n"))

	return nil
}

func (ctx *ASCIIProcessor) incr_decr(command string, args []string) error {
	key := args[0]
	change, err := strconv.ParseUint(args[1], 10, 64)
	if err!= nil {
		return ctx.sendClientError(err.Error())
	}

	_v, exist := ctx.store.Get(key)
	if !exist {
		ctx.wb.Write([]byte("NOT_FOUND\r\n"))
		return nil
	}

	v := _v
	old_value, err := strconv.ParseUint(string(v.Value), 10, 64)
	if err!= nil {
		return ctx.sendClientError(err.Error())
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

	v.Value = []byte(fmt.Sprintf("%d", new_value))
	v.Size = uint32(len(v.Value))
	ctx.store.Set(key, v, uint32(v.Size))

	if args[len(args) - 1] == "noreply" {
		return nil
	}

	ctx.wb.Write([]byte(fmt.Sprintf("%d\r\n", new_value)))

	return nil
}

func (ctx *ASCIIProcessor) stats(args []string) error {
	if len(args) == 0 {
		ctx.wb.Write([]byte(fmt.Sprintf("STAT pid %d\r\n", os.Getpid())))
		// STAT uptime 6710
		ctx.wb.Write([]byte(fmt.Sprintf("STAT time %d\r\n", time.Now().Unix())))
		ctx.wb.Write([]byte("STAT version 1.6.19\r\n"))
        return ctx.sendError()
	}

	switch args[0] {
	case "noreply":
		return ctx.sendError()
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

// 	case "touch": //touch <key> <ExpTime> [noreply]\r\n
// 		key := args[0]
// 		ExpTime, err := strconv.ParseUint(args[1], 10, 32)
// 		if err != nil {
// 			return err
// 		}

// 		_v, exist := store.Get(key)
// 		v := _v.(MemcachedEntry)
// 		if ExpTime > 0 && exist {
// 			v.ExpTime = uint32(ExpTime)
// 			store.Set(key, v, v.len)
// 			ctx.wb.Write([]byte("TOUCHED\r\n"))
// 		} else {
// 			ctx.wb.Write([]byte("NOT_FOUND\r\n"))
// 		}

// 	case "lru_crawler":
// 		switch args[0] {
// 		case "metadump":
// 			switch args[1] {
// 			case "all":
// 				// key=fake%2Fee49a9a0d462d1fa%2F18a6af34196%3A18a6af34253%3Afa5766e2 exp=1694013261 la=1694012361 cas=12434 fetch=no cls=12 size=1139
// 				// key=fake%2F886f3db85b3da0c2%2F18a6af60139%3A18a6af60c05%3A97e2dba9 exp=1694013435 la=1694012535 cas=12440 fetch=no cls=13 size=1420
// 				// key=fake%2Fc437f5f7aa7cb20b%2F18a6b03682a%3A18a6b03be70%3A123ad4e4 exp=1694013435 la=1694012535 cas=12439 fetch=no cls=39 size=1918339
// 				ctx.wb.Write([]byte("END\r\n"))
// 			default:
//                 return fmt.Errorf("not supported")
// 			}
// 		}
// 	}
// }
