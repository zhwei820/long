package main

import (
	"github.com/henrylee2cn/teleport"
	"github.com/henrylee2cn/tp-ext/proto-jsonproto"
	"os"
	"os/signal"
	"back/long/util"
	"github.com/pkg/errors"
	"github.com/henrylee2cn/goutil"
	"strings"
	"time"
)

var (
	ErrSessionNotFound = 7404
	ErrPushClientError = 7500
	ErrBizSession      = 7600
)

func getErrorMessage(code int) (string) {
	switch code {
	case ErrSessionNotFound:
		return "ErrSessionNotFound"
	case ErrPushClientError:
		return "ErrPushClientError"
	case ErrBizSession:
		return "ErrBizSession"
	default:
		return "unkown error"
	}
}

var config = util.InitConf()

type ServerInfo struct {
	srv    tp.Peer  // interface for perr
	exitCh chan int // exit channel
}

var svrInfo *ServerInfo
var bizSessInfo *SessionInfo

func main() {
	util.InitLog() // 初始化日志

	agentAddr, _ := config.String("agent.agentAddr")
	srv := tp.NewPeer(tp.PeerConfig{ListenAddress: agentAddr})

	svrInfo = &ServerInfo{// agent 服务端
		srv: srv,
		exitCh: make(chan int, 1),
	}
	srv.RoutePull(new(Home))

	go srv.ListenAndServe(jsonproto.NewJsonProtoFunc)

	var err error
	bizSessInfo, err = NewBizClients() // 初始化biz client
	if err != nil {
		util.Llog.Fatal(err)
	}

	cleanup := make(chan os.Signal)
	signal.Notify(cleanup, os.Interrupt)
	util.Llog.Info("server is running....")

	quit := make(chan bool)
	go func() {
		select {
		case <-cleanup:
			util.Llog.Info("Received an interrupt , stoping service ...")
			svrInfo.srv.Close() // 关闭agent服务端

			bizSessInfo.Close() // 关闭biz session
			quit <- true
		}
	}()
	<-quit
	util.Llog.Info("Shutdown server....")

}

type Home struct {
	tp.PullCtx
}

func (h *Home) Index(args *map[string]interface{}) (map[string]interface{}, *tp.Rerror) {
	barid := (*args)["barid"].(string)
	biz := (*args)["biz"].(string)
	sess, ok := bizSessInfo.getBizSession()

	if !ok {
		util.Llog.Error("客户端注册, 获取 biz session失败", barid, biz, getErrorMessage(ErrBizSession))
		return map[string]interface{}{"code": ErrBizSession}, nil
	}
	rerr := sess.Push("/register/index", // 客户端注册
		map[string]interface{}{
			"barid":      barid,
			"biz":        biz,
			"clientAddr": h.Session().Id(), // 客户端ip
		},
	)

	if rerr != nil {
		tp.Fatalf("%v", rerr)
	}

	return map[string]interface{}{"code": 0}, nil

}


func (h *Home) Leave(args *map[string]interface{}) (map[string]interface{}, *tp.Rerror) {
	barid := (*args)["barid"].(string)
	biz := (*args)["biz"].(string)
	sess, ok := bizSessInfo.getBizSession()

	if !ok {
		util.Llog.Error("客户端离开, 获取 biz session失败", barid, biz, getErrorMessage(ErrBizSession))
		return map[string]interface{}{"code": ErrBizSession}, nil
	}
	rerr := sess.Push("/register/leave", // 客户端离开
		map[string]interface{}{
			"barid":      barid,
			"biz":        biz,
			"clientAddr": h.Session().Id(), // 客户端ip
		},
	)

	if rerr != nil {
		tp.Fatalf("%v", rerr)
	}

	return map[string]interface{}{"code": 0}, nil

}

type Handle struct {
	tp.PullCtx
}

func (h *Handle) Biz(args *map[string]interface{}) (map[string]interface{}, *tp.Rerror) {

	clientAddr := (*args)["clientAddr"].(string)

	sess, ok := svrInfo.srv.GetSession(clientAddr)
	if !ok {
		util.Llog.Error("sess not find", clientAddr, *args)
		return map[string]interface{}{
			"code":    ErrSessionNotFound,
			"message": getErrorMessage(ErrSessionNotFound),
		}, nil

	}
	rerr := sess.Push("/push/biz", map[string]interface{}{// 推送给客户端
		"data": *args,
	})
	if rerr != nil {
		return map[string]interface{}{
			"code":    ErrPushClientError,
			"message": getErrorMessage(ErrPushClientError),
		}, nil
	}

	return map[string]interface{}{"code": 0}, nil
}

type SessionInfo struct {
	sessions goutil.Map
	cli      tp.Peer

	exitCh chan int // exit channel
	wg util.WaitGroupWrapper

}

// 随机选择 biz server 负载均衡
func (l *SessionInfo) getBizSession() (tp.Session, bool) {
	_, _sess, exist := l.sessions.Random()
	if !exist {
		return nil, false
	}
	return _sess.(tp.Session), true
}

// 关闭biz session
func (l *SessionInfo) Close() (error) {
	l.exitCh <- 1
	l.sessions.Range(func(key, value interface{}) bool {
		value.(tp.Session).Close()
		return true
	})
	return nil
}

func (l *SessionInfo) checkBizSession() {
	for {
		select {
		case <-l.exitCh:
			return
		case <-time.After(1 * time.Second):

		}
		l.sessions.Range(func(key, value interface{}) bool {
			if !value.(tp.Session).Health() {
				bizAddr := key.(string)

				sess, err := l.cli.Dial(bizAddr, jsonproto.NewJsonProtoFunc)
				if err != nil {
					util.Llog.Error("biz agent 重连失败！！", err, bizAddr)
					return false
				}
				rerr := sess.Push("/home/ping", map[string]interface{}{
					"foo": "bar",
				})
				if rerr != nil {
					util.Llog.Error("biz agent 重连失败！！", rerr, bizAddr)
					return false
				}

				l.sessions.Store(key, sess)
				util.Llog.Error("biz agent 重连成功！！", bizAddr)

			}
			return true
		})

	}

}

// newSessionInfo creates a new sessions hub.
func newSessionInfo(cli tp.Peer) *SessionInfo {
	chub := &SessionInfo{
		cli:      cli,
		sessions: goutil.AtomicMap(),
		exitCh:   make(chan int),
	}
	return chub
}
func NewBizClients() (*SessionInfo, error) {

	cli := tp.NewPeer(tp.PeerConfig{})
	cli.RoutePull(new(Handle))

	bizAddrList, _ := config.String("biz.bizAddrList")
	listBizAddr := strings.Split(bizAddrList, ",")

	bizSessInfo := newSessionInfo(cli)
	for _, bizAddr := range listBizAddr {
		sess, err := cli.Dial(bizAddr, jsonproto.NewJsonProtoFunc)
		if err != nil {
			tp.Fatalf("%v", err)
			return nil, errors.New(err.Detail)
		}
		rerr := sess.Push("/home/ping", map[string]interface{}{
			"foo": "bar",
		})
		if rerr != nil {
			util.Llog.Error(rerr)
			return nil, errors.New(rerr.Detail)
		}
		bizSessInfo.sessions.Store(bizAddr, sess)
		util.Llog.Info("连接 biz 成功", bizAddr)
	}
	bizSessInfo.wg.Wrap(bizSessInfo.checkBizSession)  // biz session 健康检查， 重连

	return bizSessInfo, nil

}
