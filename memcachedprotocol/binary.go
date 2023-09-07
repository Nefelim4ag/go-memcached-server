package memcachedprotocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"nefelim4ag/go-memcached-server/memstore"
	"time"
	"unsafe"
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

func CommandBinary(magic byte, client *bufio.ReadWriter, store *memstore.SharedStore) error {
    raw_request := make([]byte, unsafe.Sizeof(RequestHeader{})-1)
    _, err := client.Reader.Read(raw_request)
    if err != nil {
        return err
    }
    request := DecodeRequestHeader(Magic(magic), raw_request)

    // By protocol opcode & opaque same as client request
    response := ResponseHeader{
        magic:  ResponseMagic,
        opcode: request.opcode,
        status: NoErr,
        opaque: request.opaque,
    }

    switch request.opcode {
    case Set:
        flags := make([]byte, 4)
        exptime := make([]byte, 4)
        key := make([]byte, request.keyLen)
        bodyLen := request.totalBody - uint32(request.keyLen) - uint32(request.extrasLen)
        value := make([]byte, bodyLen)
        err_s := make([]error, 4)
        _, err_s[0] = client.Reader.Read(flags)
        _, err_s[1] = client.Reader.Read(exptime)
        _, err_s[2] = client.Reader.Read(key)
        _, err_s[3] = client.Reader.Read(value)
        for _, err := range err_s {
            if err != nil {
                response.status = EInter
                Response(client.Writer, &response)
                return err
            }
        }
        fmt.Printf("Flags:  0x%08x\n", flags)
        fmt.Printf("ExpTim: 0x%08x\n", exptime)
        fmt.Printf("Key:    %s\n\n", key)

        entry := memcachedEntry{
            key: string(key[:]),
            flags: binary.BigEndian.Uint32(flags),
            exptime: binary.BigEndian.Uint32(exptime),
            len: bodyLen,
            cas: uint64(time.Now().UnixNano()),
            value: value,
        }

        if request.cas != 0 {
            v, ok := store.Get(entry.key)
            if ok && request.cas != v.(memcachedEntry).cas {
                response.status = Exist
                return Response(client.Writer, &response)
            }
        }

        err = store.Set(entry.key, entry, uint64(entry.len))
        if err != nil {
            return err
        }
        response.cas = entry.cas

        return Response(client.Writer, &response)
    case Get:
        key := make([]byte, request.keyLen)
        _, err = client.Reader.Read(key)
        if err != nil {
            response.status = EInter
            Response(client.Writer, &response)
            return err
        }

        _v, ok := store.Get(string(key[:]))
        if !ok {
            response.status = NEnt
            return Response(client.Writer, &response)
        }

        v := _v.(memcachedEntry)
        response.cas = v.cas
        response.extrasLen = 4
        response.totalBody = 4 + uint32(len(v.value))
        flags := flagsType(v.flags)

        Response(client.Writer, &response)
        client.Writer.Write(flags.Bytes())
        client.Writer.Write(v.value)
        client.Writer.Flush()

        return nil
    case Quit:
        Response(client.Writer, &response)
        return fmt.Errorf("Quit")
    case QuitQ:
        return fmt.Errorf("QuitQ")
    case NoOp:
        return Response(client.Writer, &response)
    }

    return fmt.Errorf("not implemented")
}

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

func Response(w *bufio.Writer, rsp *ResponseHeader) error {
    rsp_bytes := rsp.GetBytes()

    _, err := w.Write(rsp_bytes)
    if err != nil {
        return err
    }
    w.Flush()

    return nil
}

func DecodeRequestHeader(magic Magic, request_bytes []byte) RequestHeader {
    opcode := request_bytes[0]
    keyLen := request_bytes[1:3]
    request_bytes = request_bytes[3:] // Shift by 3 bytes

    request := RequestHeader{
        magic:     Magic(magic),
        opcode:    OpcodeType(opcode),
        keyLen:    binary.BigEndian.Uint16(keyLen),
        extrasLen: request_bytes[0],
        dataType:  request_bytes[1],
        vBucketId: binary.BigEndian.Uint16(request_bytes[2:4]),
        totalBody: binary.BigEndian.Uint32(request_bytes[4:8]),
        cas:       binary.BigEndian.Uint64(request_bytes[12:20]),
    }
    request.opaque[0] = request_bytes[8]
    request.opaque[1] = request_bytes[9]
    request.opaque[2] = request_bytes[10]
    request.opaque[3] = request_bytes[11]

    fmt.Printf("Magic:  0x%02x\n", request.magic)
    fmt.Printf("Opcode: 0x%02x\n", request.opcode)
    fmt.Printf("KeyLen: 0x%04x\n", request.keyLen)
    fmt.Printf("ExtraL: 0x%02x\n", request.extrasLen)
    fmt.Printf("DataT:  0x%02x\n", request.dataType)
    fmt.Printf("vBuckt: 0x%04x\n", request.vBucketId)
    fmt.Printf("TBody:  0x%08x\n", request.totalBody)
    fmt.Printf("Opaque: 0x%08x\n", request.opaque)
    fmt.Printf("Cas:    0x%016x\n\n", request.cas)

    return request
}

func (rsp *ResponseHeader) GetBytes() []byte {
    rsp_bytes := make([]byte, unsafe.Sizeof(ResponseHeader{}))
    rsp_bytes[0] = byte(rsp.magic)
    rsp_bytes[1] = byte(rsp.opcode)
    fmt.Printf("Magic:  0x%02x\n", rsp_bytes[0])
    fmt.Printf("Opcode: 0x%02x\n", rsp_bytes[1])

    keyLen_bytes := make([]byte, 2)
    binary.BigEndian.PutUint16(keyLen_bytes, uint16(rsp.keyLen))
    rsp_bytes[2] = keyLen_bytes[0]
    rsp_bytes[3] = keyLen_bytes[1]
    fmt.Printf("KeyLen: 0x%04x\n", rsp_bytes[2:4])

    rsp_bytes[4] = rsp.extrasLen
    rsp_bytes[5] = rsp.dataType
    fmt.Printf("Extra:  0x%02x\n", rsp_bytes[4])
    fmt.Printf("DType:  0x%02x\n", rsp_bytes[5])

    status_bytes := make([]byte, 2)
    binary.BigEndian.PutUint16(status_bytes, uint16(rsp.status))
    rsp_bytes[6] = status_bytes[0]
    rsp_bytes[7] = status_bytes[1]
    fmt.Printf("Status: 0x%04x\n", rsp_bytes[6:8])

    totalBody_bytes := make([]byte, 4)
    binary.BigEndian.PutUint32(totalBody_bytes, uint32(rsp.totalBody))
    rsp_bytes[8] = totalBody_bytes[0]
    rsp_bytes[9] = totalBody_bytes[1]
    rsp_bytes[10] = totalBody_bytes[2]
    rsp_bytes[11] = totalBody_bytes[3]
    fmt.Printf("BodyL:  0x%08x\n", rsp_bytes[8:12])

    rsp_bytes[12] = rsp.opaque[0]
    rsp_bytes[13] = rsp.opaque[1]
    rsp_bytes[14] = rsp.opaque[2]
    rsp_bytes[15] = rsp.opaque[3]
    fmt.Printf("Opaque: 0x%08x\n", rsp_bytes[12:16])

    cas_bytes := make([]byte, 8)
    binary.BigEndian.PutUint64(cas_bytes, uint64(rsp.cas))
    rsp_bytes[16] = cas_bytes[0]
    rsp_bytes[17] = cas_bytes[1]
    rsp_bytes[18] = cas_bytes[2]
    rsp_bytes[19] = cas_bytes[3]
    rsp_bytes[20] = cas_bytes[4]
    rsp_bytes[21] = cas_bytes[5]
    rsp_bytes[22] = cas_bytes[6]
    rsp_bytes[23] = cas_bytes[7]
    fmt.Printf("CAS:    0x%016x\n\n", rsp_bytes[16:24])

    return rsp_bytes
}
