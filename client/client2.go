package main

import (
	"fmt"
	"net"
	"time"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	"back/long/util"
	"back/long/client/lib"
)

const LengthBytes = 4

var config = util.InitConf()

//

type ConnInfo struct {
	nc         net.Conn
	readChain  chan []byte
	writeChain chan []byte
	info       string
	closeOnce  *sync.Once // close the conn, once, per instance
	closeFlag  int32      // close flag
	closeChan  chan int   // close chanel
	breakChan  chan int   // break chanel

	waitGroup util.WaitGroupWrapper
	connId    int // 连接标识， 哪个客户端
}

func (c *ConnInfo) Close() {

	c.closeOnce.Do(func() {

		atomic.StoreInt32(&c.closeFlag, 1)
		close(c.closeChan)
		close(c.readChain)

		//empty the two packet channel.
		for bufferLen := len(c.writeChain); bufferLen != 0; bufferLen = len(c.writeChain) {
			p := <-c.writeChain

			c.writePacket(p)
		}
		close(c.writeChain)

		c.writePacket(lib.PreparePacket(lib.Leave))  // 通知agent biz断开了


		//guarantee send all msg to client.
		time.Sleep(time.Second)
		c.nc.Close()
		close(c.breakChan)  // 关闭连接结束
		util.Llog.Info("关闭连接完成!!", c.GetConnInfo())

	})
}

func (c *ConnInfo) GetConnInfo() string {
	if c.info != "" {
		return c.info
	} else {
		c.info = fmt.Sprintf("%s ---> %s ", c.nc.LocalAddr(), c.nc.RemoteAddr())
		return c.info
	}
}

//
func (c *ConnInfo) readPacket() ([]byte, error) {
	pcacketLength := make([]byte, LengthBytes)

	if _, err := io.ReadFull(c.nc, pcacketLength); err != nil {
		util.Llog.Error("读取包长度失败", err)
		return []byte(""), err
	}

	buf := make([]byte, LengthBytes+binary.BigEndian.Uint32(pcacketLength))

	if _, err := io.ReadFull(c.nc, buf[LengthBytes:]); err != nil {
		util.Llog.Error("读取包内容失败", err)
		return []byte(""), err

	}
	return buf, nil
}
func (c *ConnInfo) writePacket(msg []byte) {

	_, err := c.nc.Write(msg)
	if err != nil {
		util.Llog.Error("write string failed, ", err)
		return
	}
}
func (c *ConnInfo) writeLoop() {
	defer func() {
		if err := recover(); err != nil {
			util.Llog.Error(c.GetConnInfo(), "writeLoop err:", err)
		}
		c.Close()
	}()
	for {
		select {
		case <-c.closeChan: // 连接是否关闭
			util.Llog.Error("连接关闭", c.GetConnInfo())
			return
		case p, ok := <-c.writeChain:
			if ok {
				c.writePacket(p)
			}

		}
	}

}
func (c *ConnInfo) readLoop() {
	defer func() {
		if err := recover(); err != nil {
			util.Llog.Error(c.GetConnInfo(), "clientReadLoop err:", err)
		}
		c.Close()
	}()
	for {
		select {
		case <-c.closeChan:
			util.Llog.Error("连接关闭", c.GetConnInfo())
			return
		default:

		}

		buf, err := c.readPacket()
		if err != nil {
			util.Llog.Error("read packet error, ", err)
			return
		}
		util.Llog.Info(string(buf[LengthBytes+1:]))
		c.readChain <- buf
	}
}

func (c *ConnInfo) handleLoop() {
	defer func() {
		if err := recover(); err != nil {
			util.Llog.Error(c.GetConnInfo(), "handleLoop err:", err)
		}
		c.Close()
	}()
	for {
		select {
		case <-c.closeChan:
			util.Llog.Error("连接关闭", c.GetConnInfo())
			return

		case p, ok := <-c.readChain:
			if ok {
				util.Llog.Info(string(p[LengthBytes+1:]))
			}

		}
	}
}

func run(ii int) (*ConnInfo, error) {

	agentAddr, _ := config.String("agent.agentAddr")
	conn, err := net.Dial("tcp", agentAddr)
	if err != nil {
		util.Llog.Info("Error dialing", err.Error())
		return &ConnInfo{}, err
	}

	connInfo := &ConnInfo{
		nc:         conn,
		readChain:  make(chan []byte, 100),
		writeChain: make(chan []byte, 100),
		info:       "",
		closeOnce:  &sync.Once{},
		closeChan:  make(chan int),
		connId:     ii,
		breakChan:make(chan int),
	}

	connInfo.waitGroup.Wrap(connInfo.readLoop)
	connInfo.waitGroup.Wrap(connInfo.writeLoop)
	connInfo.waitGroup.Wrap(connInfo.handleLoop)

	connInfo.writeChain <- lib.PreparePacket(lib.Come) // 注册

	return connInfo, nil

}

func main() {

	util.InitLog()
	ii := 0
	for {
		c, err := run(ii)
		if err != nil {
			util.Llog.Error(err)
			time.Sleep(1 * time.Second)
			continue
		}
		<-c.breakChan
		util.Llog.Info("breakChan")
		ii ++

	}

	select {}
}
