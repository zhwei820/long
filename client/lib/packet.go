package lib

import (
	"fmt"
	"time"
	"github.com/henrylee2cn/teleport"
	"encoding/binary"
)

const format = `{"seq":%q,"ptype":%d,"uri":%q,"meta":%q,"body_codec":%d,"body":"%s"}`

var (
	Leave = 1
	Come  = 0
)

// 组装符合 json proto的消息
func PreparePacket(way int) ([]byte) {
	trail := ""

	switch way {
	case 0:
		trail = "index"
	case 1:
		trail = "leave"
	default:
		trail = "index"
	}
	var msg = fmt.Sprintf(format,
		fmt.Sprintf("%v", (time.Now().Unix() )),
		tp.TypePull,
		"/home/"+trail,
		"",
		'j',
		[]byte("{\\\"biz\\\":\\\"voice\\\",\\\"barid\\\":\\\"10001\\\",\\\"pc\\\":\\\"A001\\\"}"))
	var b = []byte(msg)
	llen := uint32(1 + len(b))
	var all = make([]byte, llen+4)
	binary.BigEndian.PutUint32(all, llen)
	all[4] = 0
	copy(all[4+1:], b)
	return all

}
