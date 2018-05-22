package lib

import (
	"github.com/garyburd/redigo/redis"
	"github.com/zpatrick/go-config"
	"back/long/util"
)

func InitRedisPool(config *config.Config) (*redis.Pool, error) {

	redisAddr, _ := config.String("redis.redisAddr")
	redisPassword, _ := config.String("redis.redisPassword")
	redisDb, _ := config.Int("redis.redisDb")

	pool := util.NewRedisPool(redisAddr, redisPassword, redisDb)
	return pool, nil

}
