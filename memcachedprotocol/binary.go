package memcachedprotocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"nefelim4ag/go-memcached-server/memstore"
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
    opcode := raw_request[0]
    keyLen := raw_request[1:3]
    raw_request = raw_request[3:] // Shift by 3 bytes

    request := RequestHeader{
        magic:     Magic(magic),
        opcode:    OpcodeType(opcode),
        keyLen:    binary.LittleEndian.Uint16(keyLen),
        extrasLen: raw_request[0],
        dataType:  raw_request[1],
        vBucketId: binary.LittleEndian.Uint16(raw_request[2:4]),
        totalBody: binary.LittleEndian.Uint32(raw_request[4:8]),
        opaque:    binary.LittleEndian.Uint32(raw_request[8:12]),
        cas:       binary.LittleEndian.Uint64(raw_request[12:20]),
    }

    switch request.opcode {
    case Quit:
        Response(client.Writer, request.opcode, NoErr, request.opaque)
        return fmt.Errorf("Quit")
    case QuitQ:
        return fmt.Errorf("QuitQ")
    case NoOp:
        Response(client.Writer, request.opcode, NoErr, request.opaque)
    }

    return fmt.Errorf("Not implemented")
}

type RequestHeader struct {
    magic     Magic
    opcode    OpcodeType
    keyLen    uint16
    extrasLen uint8
    dataType  uint8
    vBucketId uint16
    totalBody uint32
    opaque    uint32
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
    opaque    uint32
    cas       uint64
}

func Response(w *bufio.Writer, opcode OpcodeType, status ResponseStatus, opaque uint32) error {
    rsp := ResponseHeader{
        magic:  ResponseMagic,
        opcode: opcode,
        status: status,
        opaque: opaque,
    }

    rsp_bytes := make([]byte, unsafe.Sizeof(ResponseHeader{}))
    rsp_bytes[0] = byte(rsp.magic)
    rsp_bytes[1] = byte(rsp.opcode)
    fmt.Printf("0x%x\n", rsp_bytes[0])
    fmt.Printf("0x%x\n", rsp_bytes[1])

    keyLen_bytes := make([]byte, 2)
    binary.LittleEndian.PutUint16(keyLen_bytes, uint16(rsp.keyLen))
    rsp_bytes[2] = keyLen_bytes[0]
    rsp_bytes[3] = keyLen_bytes[1]
    fmt.Printf("0x%x%x\n", rsp_bytes[2], rsp_bytes[3])

    rsp_bytes[4] = rsp.extrasLen
    rsp_bytes[5] = rsp.dataType
    fmt.Printf("0x%x\n", rsp_bytes[4])
    fmt.Printf("0x%x\n", rsp_bytes[5])

    status_bytes := make([]byte, 2)
    binary.LittleEndian.PutUint16(status_bytes, uint16(rsp.status))
    rsp_bytes[6] = status_bytes[0]
    rsp_bytes[7] = status_bytes[1]
    fmt.Printf("0x%x%x\n", rsp_bytes[6], rsp_bytes[7])

    totalBody_bytes := make([]byte, 4)
    binary.LittleEndian.PutUint32(totalBody_bytes, uint32(rsp.totalBody))
    rsp_bytes[8] = totalBody_bytes[0]
    rsp_bytes[9] = totalBody_bytes[1]
    rsp_bytes[10] = totalBody_bytes[2]
    rsp_bytes[11] = totalBody_bytes[3]
    fmt.Printf("0x%x%x%x%x\n", rsp_bytes[8], rsp_bytes[9], rsp_bytes[10], rsp_bytes[11])

    opaque_bytes := make([]byte, 4)
    binary.LittleEndian.PutUint32(opaque_bytes, uint32(rsp.opaque))
    rsp_bytes[12] = opaque_bytes[0]
    rsp_bytes[13] = opaque_bytes[1]
    rsp_bytes[14] = opaque_bytes[2]
    rsp_bytes[15] = opaque_bytes[3]
    fmt.Printf("0x%x%x%x%x\n", rsp_bytes[12], rsp_bytes[13], rsp_bytes[14], rsp_bytes[15])

    cas_bytes := make([]byte, 8)
    binary.LittleEndian.PutUint64(cas_bytes, uint64(rsp.cas))
    rsp_bytes[16] = cas_bytes[0]
    rsp_bytes[17] = cas_bytes[1]
    rsp_bytes[18] = cas_bytes[2]
    rsp_bytes[19] = cas_bytes[3]
    rsp_bytes[20] = cas_bytes[4]
    rsp_bytes[21] = cas_bytes[5]
    rsp_bytes[22] = cas_bytes[6]
    rsp_bytes[23] = cas_bytes[7]
    fmt.Printf("0x%x%x%x%x", rsp_bytes[16], rsp_bytes[17], rsp_bytes[18], rsp_bytes[19])
    fmt.Printf("%x%x%x%x\n", rsp_bytes[20], rsp_bytes[21], rsp_bytes[22], rsp_bytes[23])

    _, err := w.Write(rsp_bytes)
    if err != nil {
        return err
    }
    w.Flush()

    return nil
}
