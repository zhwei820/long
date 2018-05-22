package main

import (
	"github.com/henrylee2cn/teleport"
	"github.com/henrylee2cn/tp-ext/proto-jsonproto"
	"time"
	"github.com/garyburd/redigo/redis"
	"back/long/biz/lib"
	"back/long/util"
	"os"
	"os/signal"
	"github.com/nsqio/go-nsq"
	"sync"
	"strconv"
	"github.com/tidwall/gjson"
	"strings"
	"github.com/pkg/errors"
	"fmt"
)

var (
	ErrClientRegister = 95003
	ErrAgentConnect   = 95004
)

func getErrorMessage(code int) (string) {
	switch code {
	case ErrClientRegister:
		return "ErrClientRegister"
	case ErrAgentConnect:
		return "ErrAgentConnect"
	default:
		return "unkown error"
	}
}

var config = util.InitConf()

type ServerInfo struct {
	srv    tp.Peer     // interface for perr
	rpool  *redis.Pool // redis connection interface
	exitCh chan int    // exit channel
}

func (l *ServerInfo) Close() {
	l.srv.Close()
	l.rpool.Close()

}

var svrInfo *ServerInfo // 全局对象

func main() {
	util.InitLog()
	util.Llog.Info("init log done.")

	bizAddr, _ := config.String("biz.bizAddr")

	srv := tp.NewPeer(tp.PeerConfig{ListenAddress: bizAddr})

	rpool, err := lib.InitRedisPool(config)
	if err != nil {
		util.Llog.Error("redis conn err", err)
	}
	svrInfo = &ServerInfo{
		srv:    srv,
		rpool:  rpool,
		exitCh: make(chan int, 1),
	}

	srv.RoutePush(new(Register))
	go srv.ListenAndServe(jsonproto.NewJsonProtoFunc)

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for _ = range ticker.C {
			util.Llog.Info(srv.CountSession())
			srv.RangeSession(func(sess tp.Session) bool {
				util.Llog.Info(sess.Id())
				return true
			})
		}
	}()

	consumeNsq() // 消费nsq消息队列

	ticker.Stop()
	util.Llog.Info("Ticker stopped")

}

const sep = "||"

type Register struct {
	tp.PushCtx
}

// 客户端注册， 写入redis
func (h *Register) Index(args *map[string]interface{}) (*tp.Rerror) {

	barid := (*args)["barid"].(string)
	biz := (*args)["biz"].(string)
	clientAddr := (*args)["clientAddr"].(string)

	route := strings.Split(h.Session().Id(), ":")[0] + sep + clientAddr
	_, err := svrInfo.rpool.Get().Do("HSET", util.ClientInfoKey, barid+biz, route) // 记录客户端路由信息
	if err != nil {
		util.Llog.Error(err)
		util.Llog.Error("客户端注册， 写入redis失败！", barid, biz, strings.Split(h.Session().Id(), ":")[0])
		return tp.NewRerror(int32(ErrClientRegister), getErrorMessage(ErrClientRegister), fmt.Sprintf("客户端注册， 写入redis, %s, %s", barid, biz))

	}

	return nil
}

// 客户端离开， 写入redis
func (h *Register) Leave(args *map[string]interface{}) (*tp.Rerror) {

	barid := (*args)["barid"].(string)
	biz := (*args)["biz"].(string)

	_, err := svrInfo.rpool.Get().Do("HDEL", util.ClientInfoKey, barid+biz) // 记录客户端路由信息
	if err != nil {
		util.Llog.Error(err)
		util.Llog.Error("客户端离开， 写入redis失败！", barid, biz, strings.Split(h.Session().Id(), ":")[0])
		return tp.NewRerror(int32(ErrClientRegister), getErrorMessage(ErrClientRegister), fmt.Sprintf("客户端离开， 写入redis失败！, %s, %s", barid, biz))

	}

	return nil
}

type Home struct {
	tp.PushCtx
}

// ping
func (h *Home) ping(args *map[string]interface{}) (*tp.Rerror) {

	_, err := svrInfo.rpool.Get().Do("SET", strings.Split(h.Session().Id(), ":")[0], h.Session().Id()) // set agent id
	if err != nil {
		util.Llog.Error("set agentId in redis error", err, h.Session().Id())
		return tp.NewRerror(int32(ErrAgentConnect), getErrorMessage(ErrAgentConnect), "set agentId in redis error")
	}

	return nil
}

// 消费nsq消息队列
func consumeNsq() {

	agentNum, _ := config.Int("biz.agentNum")

	for {
		if agentNum == svrInfo.srv.CountSession() { // agent 连上来了
			break
		}
		select {
		case <-time.After(1 * time.Second):
			util.Llog.Warn("waiting for agent!!")
			continue
		}
	}

	topicName := "publishtest"
	channelCount := 2
	for i := 0; i < channelCount; i++ {
		go readMessage(topicName, i)
	}

	cleanup := make(chan os.Signal)
	signal.Notify(cleanup, os.Interrupt)
	util.Llog.Info("server is running....")

	quit := make(chan bool)
	go func() {
		select {
		case <-cleanup:
			util.Llog.Info("Received an interrupt , stoping service ...")
			for _, ele := range consumers {
				ele.StopChan <- 1
				ele.Stop()

			}
			quit <- true
		}
	}()
	<-quit
	util.Llog.Info("Shutdown server....")
}

type ConsumerHandle struct {
	q           *nsq.Consumer
	chId        int
	maxAttempts uint16
	maxTimeout  int64
}

var consumers = make([]*nsq.Consumer, 0)
var mux = &sync.Mutex{}

// 读取nsq消息, 获取到agent id, 发送 消息给agent, 在转发给客户端
func (h *ConsumerHandle) HandleMessage(message *nsq.Message) error {

	if message.Attempts >= h.maxAttempts && message.Timestamp < (time.Now().UnixNano()-h.maxTimeout*int64(time.Second)) { // 超时 和 重试检查
		util.Llog.Error("消息重试和超时达到限制:", string(message.Body), message.Timestamp, message.Attempts, string(message.ID[:]), "丢弃此消息!!!") // 丢弃此消息
		return nil
	}

	body := string(message.Body)
	barid := gjson.Get(body, "barid").String()
	biz := gjson.Get(body, "biz").String()

	route, err := redis.String(svrInfo.rpool.Get().Do("HGET", util.ClientInfoKey, barid+biz, ))
	if err != nil {
		util.Llog.Error(barid+biz, body, "get route in redis error", err)
		return err
	}
	addrs := strings.Split(route, sep)
	if (len(addrs) != 2) {
		util.Llog.Error(barid+biz, body, "addr: ", addrs)
		return err
	}
	agentId, err := redis.String(svrInfo.rpool.Get().Do("GET", addrs[0], )) // get agent id
	if err != nil {
		util.Llog.Error(barid+biz, body, "get agentId in redis error", err)
		return err
	}

	sess, ok := svrInfo.srv.GetSession(agentId)
	if !ok {
		util.Llog.Error("获取session 失败", barid, biz, route, agentId)
		return errors.New("获取session 失败")
	}

	var reply map[string]interface{}
	rerr := sess.Pull("/handle/biz",
		map[string]interface{}{
			"clientAddr": addrs[1],
			"body":       body,
		},
		&reply,
	).Rerror()

	if rerr != nil {
		util.Llog.Error("%v", rerr)
		return errors.New(rerr.Message + " " + rerr.Detail)
	}
	if reply["code"] != 0 {
		util.Llog.Error("%v", rerr)
		return errors.New(fmt.Sprintf("reply error: %v", reply))
	}

	util.Llog.Info(body + ": " + strconv.Itoa(h.chId))
	return nil
}

// 初始化consumer
func readMessage(topicName string, channelCount int) {

	defer func() {
		if err := recover(); err != nil {
			util.Llog.Info("error: ", err)
		}
	}()

	nsqConf := nsq.NewConfig()
	nsqConf.MaxInFlight = 1000
	nsqConf.MaxBackoffDuration = 500 * time.Second

	q, _ := nsq.NewConsumer(topicName, "channel"+strconv.Itoa(channelCount), nsqConf)

	maxAttempts, _ := config.Int("nsq.maxAttempts")
	maxTimeout, _ := config.Int("nsq.maxTimeout")

	h := &ConsumerHandle{
		q:           q,
		chId:        channelCount,
		maxAttempts: uint16(maxAttempts),
		maxTimeout:  int64(maxTimeout),
	}
	q.AddHandler(h)

	err := q.ConnectToNSQLookupd("192.168.1.5:4161")

	if err != nil {
		util.Llog.Error("conect nsqd error", err)
	}
	mux.Lock()
	consumers = append(consumers, q)
	mux.Unlock()
	<-q.StopChan
	util.Llog.Info("end....")
}
