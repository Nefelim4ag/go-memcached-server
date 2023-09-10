package memcachedprotocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"nefelim4ag/go-memcached-server/memstore"
	"sync"
	"time"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

// opcodes
type OpcodeType byte

const (
    Get                         OpcodeType = 0x00
    Set                         OpcodeType = 0x01
    Add                         OpcodeType = 0x02
    Replace                     OpcodeType = 0x03
    Delete                      OpcodeType = 0x04
    Increment                   OpcodeType = 0x05
    Decrement                   OpcodeType = 0x06
    Quit                        OpcodeType = 0x07
    Flush                       OpcodeType = 0x08
    GetQ                        OpcodeType = 0x09
    NoOp                        OpcodeType = 0x0a
    Version                     OpcodeType = 0x0b
    GetK                        OpcodeType = 0x0c
    GetKQ                       OpcodeType = 0x0d
    Append                      OpcodeType = 0x0e
    Prepend                     OpcodeType = 0x0f
    Stat                        OpcodeType = 0x10
    SetQ                        OpcodeType = 0x11
    AddQ                        OpcodeType = 0x12
    ReplaceQ                    OpcodeType = 0x13
    DeleteQ                     OpcodeType = 0x14
    IncrementQ                  OpcodeType = 0x15
    DecrementQ                  OpcodeType = 0x16
    QuitQ                       OpcodeType = 0x17
    FlushQ                      OpcodeType = 0x18
    AppendQ                     OpcodeType = 0x19
    PrependQ                    OpcodeType = 0x1a
    Verbosity_Unstable          OpcodeType = 0x1b
    Touch_Unstable              OpcodeType = 0x1c
    GAT_Unstable                OpcodeType = 0x1d
    GATQ_Unstable               OpcodeType = 0x1e
    SASLlistmechs               OpcodeType = 0x20
    SASLAuth                    OpcodeType = 0x21
    SASLStep                    OpcodeType = 0x22
    RGet                        OpcodeType = 0x30
    RSet                        OpcodeType = 0x31
    RSetQ                       OpcodeType = 0x32
    RAppend                     OpcodeType = 0x33
    RAppendQ                    OpcodeType = 0x34
    RPrepend                    OpcodeType = 0x35
    RPrependQ                   OpcodeType = 0x36
    RDelete                     OpcodeType = 0x37
    RDeleteQ                    OpcodeType = 0x38
    RIncr                       OpcodeType = 0x39
    RIncrQ                      OpcodeType = 0x3a
    RDecr                       OpcodeType = 0x3b
    RDecrQ                      OpcodeType = 0x3c
    SetVBucket_Unstable         OpcodeType = 0x3d
    GetVBucket_Unstable         OpcodeType = 0x3e
    DelVBucket_Unstable         OpcodeType = 0x3f
    TAPConnect_Unstable         OpcodeType = 0x40
    TAPMutation_Unstable        OpcodeType = 0x41
    TAPDelete_Unstable          OpcodeType = 0x42
    TAPFlush_Unstable           OpcodeType = 0x43
    TAPOpaque_Unstable          OpcodeType = 0x44
    TAPVBucketSet_Unstable      OpcodeType = 0x45
    TAPCheckpointStart_Unstable OpcodeType = 0x46
    TAPCheckpointEnd_Unstable   OpcodeType = 0x47
)

// Response status
type ResponseStatus uint16

const (
    NoErr      ResponseStatus = 0x0000 // No error
    NEnt       ResponseStatus = 0x0001 // Key not found
    Exist      ResponseStatus = 0x0002 // Key exists
    TooLarg    ResponseStatus = 0x0003 // Value too large
    InvArg     ResponseStatus = 0x0004 // Invalid arguments
    ItemNoStor ResponseStatus = 0x0005 // Item not stored
    EType      ResponseStatus = 0x0006 // Incr/Decr on non-numeric value.
    // 0x0007	The vbucket belongs to another server
    // 0x0008	Authentication error
    // 0x0009	Authentication continue
    EUnknown ResponseStatus = 0x0081 // Unknown command
    EOOM     ResponseStatus = 0x0082 //	Out of memory
    ENSupp   ResponseStatus = 0x0083 // Not supported
    EInter   ResponseStatus = 0x0084 // Internal error
    EBusy    ResponseStatus = 0x0085 // Busy
    ETmpF    ResponseStatus = 0x0086 // Temporary failure
)

// Magic
type Magic byte

const (
    RequestMagic  Magic = 0x80
    ResponseMagic Magic = 0x81
)

type flagsType uint32

func (f flagsType) Bytes() []byte {
    b := make([]byte, 4)
    binary.BigEndian.PutUint32(b, uint32(f))
    return b
}

type RequestHeader struct {
    magic     Magic
    opcode    OpcodeType
    keyLen    uint16
    extrasLen uint8
    dataType  uint8
    vBucketId uint16
    totalBody uint32
    opaque    [4]byte
    cas       uint64
}

type ResponseHeader struct {
    magic     Magic
    opcode    OpcodeType
    keyLen    uint16
    extrasLen uint8
    dataType  uint8
    status    ResponseStatus
    totalBody uint32
    opaque    [4]byte
    cas       uint64
}

type BinaryProcessor struct {
    store *memstore.SharedStore
    rb    *bufio.Reader
    wb    *bufio.Writer

    raw_request  []byte
    flags        []byte
    exptime      []byte
    request      RequestHeader
    response     ResponseHeader
    response_raw []byte
    key          []byte

    sync.Mutex
    writers      sync.WaitGroup
    write_added  sync.Cond
    write_buffer []byte
    shutdown     bool


}

type RawResponse struct {
    Bytes []byte
}

func CreateBinaryProcessor(rb *bufio.Reader, wb *bufio.Writer, store *memstore.SharedStore) *BinaryProcessor {
    b := BinaryProcessor{
        store:        store,
        rb:           rb,
        wb:           wb,
        // cw:           make(chan RawResponse, 1024),
        raw_request:  make([]byte, unsafe.Sizeof(RequestHeader{})),
        flags:        make([]byte, 4),
        exptime:      make([]byte, 4),
        response_raw: make([]byte, unsafe.Sizeof(ResponseHeader{})),
        shutdown:     false,
    }
    b.write_added.L = &b

    b.writers.Add(1)
    go b.ASyncWriter()
    return &b
}

func (ctx *BinaryProcessor) Close() {
    ctx.shutdown = true
    ctx.write_added.Broadcast()
    ctx.writers.Wait()
    // close(ctx.cw)
}

func (ctx *BinaryProcessor) ASyncWriter() {
    defer ctx.writers.Done()
    for !ctx.shutdown{
        ctx.Lock()
        if len(ctx.write_buffer) == 0 {
            ctx.write_added.Wait()
        }
        rsp := ctx.write_buffer
        ctx.write_buffer = make([]byte, 0)
        ctx.Unlock()
        ctx.wb.Write(rsp)
        ctx.wb.Flush()
    }

    // for b := range ctx.cw {
    //     // fmt.Printf("%048x\n", b.Bytes)
    //     ctx.wb.Write(b.Bytes)
    //     ctx.wb.Flush()
    // }
}

func (ctx *BinaryProcessor) CommandBinary() error {
    err := ctx.ReadRequest()
    if err != nil {
        return err
    }

    // defer ctx.wb.Flush()

    ctx.DecodeRequestHeader()

    // By protocol opcode & opaque same as client request
    ctx.response = ResponseHeader{
        magic:  ResponseMagic,
        opcode: ctx.request.opcode,
        status: NoErr,
        opaque: ctx.request.opaque,
    }

    switch ctx.request.opcode {
    case Set, SetQ, Add, AddQ:
        flags := ctx.flags
        exptime := ctx.exptime
        key := ctx.key
        if uint16(len(ctx.key)) != ctx.request.keyLen {
            key = make([]byte, ctx.request.keyLen)
        }
        bodyLen := ctx.request.totalBody - uint32(ctx.request.keyLen) - uint32(ctx.request.extrasLen)
        value := make([]byte, bodyLen)
        err_s := make([]error, 4)
        _, err_s[0] = ctx.rb.Read(flags)
        _, err_s[1] = ctx.rb.Read(exptime)
        _, err_s[2] = ctx.rb.Read(key)
        if bodyLen > 0 {
            _, err_s[3] = io.ReadFull(ctx.rb, value)
        }
        for _, err := range err_s {
            if err != nil {
                ctx.response.status = EInter
                ctx.Response()
                return err
            }
        }
        if log.GetLevel() == log.DebugLevel {
            log.Debugf("Flags:  0x%08x\n", flags)
            log.Debugf("ExpTim: 0x%08x\n", exptime)
            log.Debugf("Key:    %s\n\n", key)
        }

        entry := memstore.MEntry{
            Key:     string(key[:]),
            Flags:   binary.BigEndian.Uint32(flags),
            ExpTime: binary.BigEndian.Uint32(exptime),
            Size:     bodyLen,
            Cas:     uint64(time.Now().UnixNano()),
            Value:   value,
        }

        if ctx.request.cas != 0 {
            _v, ok := ctx.store.Get(entry.Key)
            if ok {
                v := *_v
                if ctx.request.cas != v.Cas {
                    ctx.response.status = Exist
                    return ctx.Response()
                }
            }
        }

        if ctx.request.opcode == Add || ctx.request.opcode == AddQ {
            _, ok := ctx.store.Get(entry.Key)
            if ok {
                ctx.response.status = Exist
                // v := _v.(memstore.MEntry)
                ctx.Response()
                // ctx.wb.Write(v.value)
                return nil
            }
        }

        err = ctx.store.Set(entry.Key, &entry, entry.Size)
        if err != nil {
            return err
        }
        ctx.response.cas = entry.Cas

        if ctx.request.opcode == SetQ || ctx.request.opcode == AddQ {
            return nil
        }

        return ctx.Response()
    case Get, GetQ:
        key := ctx.key
        if uint16(len(ctx.key)) != ctx.request.keyLen {
            key = make([]byte, ctx.request.keyLen)
        }
        _, err = ctx.rb.Read(key)
        if err != nil {
            ctx.response.status = EInter
            ctx.Response()
            return err
        }

        _v, ok := ctx.store.Get(string(key[:]))

        if !ok {
            if ctx.request.opcode == GetQ {
                return nil
            }
            ctx.response.status = NEnt
            return ctx.Response()
        }
        v := _v

        ctx.response.cas = v.Cas
        ctx.response.extrasLen = 4
        ctx.response.totalBody = 4 + uint32(len(v.Value))
        flags := flagsType(v.Flags).Bytes()

        ctx.Response(flags, v.Value)

        return nil
    case Flush, FlushQ:
        exptime := ctx.exptime
        if ctx.request.extrasLen == 4 {
            _, err = ctx.rb.Read(exptime)
            if err!= nil {
                ctx.response.status = EInter
                ctx.Response()
                return err
            }
            if log.GetLevel() == log.DebugLevel {
                log.Debugf("ExpTim: 0x%08x\n", exptime)
            }
        }

        ctx.store.Flush()

        if ctx.request.opcode == FlushQ {
            return nil
        }

        return ctx.Response()
    case Quit:
        ctx.Response()
        return fmt.Errorf("Quit")
    case QuitQ:
        return fmt.Errorf("QuitQ")
    case NoOp:
        return ctx.Response()
    }

    return fmt.Errorf("not implemented opcode: %02x", ctx.request.opcode)
}

func (ctx *BinaryProcessor) ReadRequest() error {
    _, err := ctx.rb.Read(ctx.raw_request)

    if err != nil {
        return err
    }

    return nil
}

func (ctx *BinaryProcessor) Response(bytes ...[]byte) error {
    ctx.PrepareResponse()
    rsp := ctx.response_raw
    for _, b := range bytes {
        rsp = append(rsp, b[:]...)
    }

    ctx.Lock()
    ctx.write_buffer = append(ctx.write_buffer, rsp...)
    ctx.Unlock()
    ctx.write_added.Signal()

    // ctx.cw <- RawResponse{
    //     Bytes: rsp,
    // }

    // ctx.wb.Write(flags)
    // //ctx.cw <- &flags
    // ctx.wb.Write(v.Value)
    // //ctx.cw <- &v.value
    // _, err := ctx.wb.Write(ctx.response_raw)
    // if err != nil {
    //     return err
    // }

    return nil
}

func (ctx *BinaryProcessor) DecodeRequestHeader() {
    ctx.request.magic = Magic(ctx.raw_request[0])
    ctx.request.opcode = OpcodeType(ctx.raw_request[1])
    ctx.request.keyLen = binary.BigEndian.Uint16(ctx.raw_request[2:4])
    ctx.request.extrasLen = ctx.raw_request[4]
    ctx.request.dataType = ctx.raw_request[5]
    ctx.request.vBucketId = binary.BigEndian.Uint16(ctx.raw_request[6:8])
    ctx.request.totalBody = binary.BigEndian.Uint32(ctx.raw_request[8:12])
    ctx.request.opaque[0] = ctx.raw_request[12]
    ctx.request.opaque[1] = ctx.raw_request[13]
    ctx.request.opaque[2] = ctx.raw_request[14]
    ctx.request.opaque[3] = ctx.raw_request[15]
    ctx.request.cas = binary.BigEndian.Uint64(ctx.raw_request[16:24])

    if log.GetLevel() == log.DebugLevel {
        log.Debugf("Magic:  0x%02x\n", ctx.request.magic)
        log.Debugf("Opcode: 0x%02x\n", ctx.request.opcode)
        log.Debugf("KeyLen: 0x%04x\n", ctx.request.keyLen)
        log.Debugf("ExtraL: 0x%02x\n", ctx.request.extrasLen)
        log.Debugf("DataT:  0x%02x\n", ctx.request.dataType)
        log.Debugf("vBuckt: 0x%04x\n", ctx.request.vBucketId)
        log.Debugf("TBody:  0x%08x\n", ctx.request.totalBody)
        log.Debugf("Opaque: 0x%08x\n", ctx.request.opaque)
        log.Debugf("Cas:    0x%016x\n\n", ctx.request.cas)
    }
}

func (ctx *BinaryProcessor) PrepareResponse() {
    ctx.response_raw[0] = byte(ctx.response.magic)
    ctx.response_raw[1] = byte(ctx.response.opcode)

    binary.BigEndian.PutUint16(ctx.raw_request, uint16(ctx.response.keyLen))
    ctx.response_raw[2] = ctx.raw_request[0]
    ctx.response_raw[3] = ctx.raw_request[1]

    ctx.response_raw[4] = ctx.response.extrasLen
    ctx.response_raw[5] = ctx.response.dataType

    binary.BigEndian.PutUint16(ctx.raw_request, uint16(ctx.response.status))
    ctx.response_raw[6] = ctx.raw_request[0]
    ctx.response_raw[7] = ctx.raw_request[1]

    binary.BigEndian.PutUint32(ctx.raw_request, uint32(ctx.response.totalBody))
    ctx.response_raw[8] = ctx.raw_request[0]
    ctx.response_raw[9] = ctx.raw_request[1]
    ctx.response_raw[10] = ctx.raw_request[2]
    ctx.response_raw[11] = ctx.raw_request[3]

    ctx.response_raw[12] = ctx.response.opaque[0]
    ctx.response_raw[13] = ctx.response.opaque[1]
    ctx.response_raw[14] = ctx.response.opaque[2]
    ctx.response_raw[15] = ctx.response.opaque[3]

    binary.BigEndian.PutUint64(ctx.raw_request, uint64(ctx.response.cas))
    ctx.response_raw[16] = ctx.raw_request[0]
    ctx.response_raw[17] = ctx.raw_request[1]
    ctx.response_raw[18] = ctx.raw_request[2]
    ctx.response_raw[19] = ctx.raw_request[3]
    ctx.response_raw[20] = ctx.raw_request[4]
    ctx.response_raw[21] = ctx.raw_request[5]
    ctx.response_raw[22] = ctx.raw_request[6]
    ctx.response_raw[23] = ctx.raw_request[7]
    if log.GetLevel() == log.DebugLevel {
        log.Debugf("Magic:  0x%02x\n", ctx.response_raw[0])
        log.Debugf("Opcode: 0x%02x\n", ctx.response_raw[1])
        log.Debugf("KeyLen: 0x%04x\n", ctx.response_raw[2:4])
        log.Debugf("Extra:  0x%02x\n", ctx.response_raw[4])
        log.Debugf("DType:  0x%02x\n", ctx.response_raw[5])
        log.Debugf("Status: 0x%04x\n", ctx.response_raw[6:8])
        log.Debugf("BodyL:  0x%08x\n", ctx.response_raw[8:12])
        log.Debugf("Opaque: 0x%08x\n", ctx.response_raw[12:16])
        log.Debugf("CAS:    0x%016x\n\n", ctx.response_raw[16:24])
    }
}
