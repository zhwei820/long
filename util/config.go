package util

import (
"github.com/zpatrick/go-config"
)

func InitConf() (*config.Config) {
	iniFile := config.NewINIFile("../config/config.ini")
	c := config.NewConfig([]config.Provider{iniFile})
	if err := c.Load(); err != nil{
		Llog.Fatal(err)
	}
	return c
}