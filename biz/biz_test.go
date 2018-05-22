package main

import (
	"testing"
	"github.com/nsqio/go-nsq"
	"time"
	"back/long/util"
)

func TestProducer(t *testing.T) {
	config := nsq.NewConfig()
	// 随便给哪个ip发都可以
	w1, _ := nsq.NewProducer("192.168.1.5:4150", config)

	err1 := w1.Ping()
	if err1 != nil {
		util.Llog.Fatalf("should not be able to ping after Stop()")
		return
	}
	defer w1.Stop()
	topicName := "publishtest"
	msgCount := 10
	for i := 0; i < msgCount; i++ {
		time.Sleep(time.Millisecond * 15)

		err1 := w1.Publish(topicName, []byte("{\"barid\":\"10001\",\"biz\":\"voice\",\"data\":\"123\"}"))
		if err1 != nil {
			util.Llog.Error("error%s", err1)
		}
		//err2 := w1.DeferredPublish(topicName,time.Second * 10, []byte(strconv.Itoa(i)))
		//if err2 != nil {
		//	util.Llog.Error("error%s", err2)
		//}

	}
	println("end")
}
