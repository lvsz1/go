/**
该文件主要用于counter前端接口处理，支持：
1、计数器增加
2、计数器减小
3、获得某个计数器的数据
 */

package main

import (
	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
	"time"
)

const (
	DefaultReadTimeOut = 10 * time.Second
	DefaultWriteTimeOut = 10 * time.Second
)
var store = NewCounterStore()

func CounterServe(addr string) {
	engine := gin.New()
	engine.Use(gin.Recovery())

	endless.DefaultReadTimeOut = DefaultReadTimeOut
	endless.DefaultWriteTimeOut = DefaultWriteTimeOut
	server := endless.NewServer(addr, engine)
	counterGroup := engine.Group("counter")
	{
		counterGroup.Any("/test", HelloWorld)
		counterGroup.Any("/increase", Increase)
		counterGroup.Any("/decrease", Decrease)
		counterGroup.Any("/get", Get)
	}
	server.ListenAndServe()
}

func HelloWorld(c *gin.Context) {
	c.JSON(c.Writer.Status(), map[string]interface{}{
		"msg": "hello world",
	})
}

//计数器加
func Increase(c *gin.Context) {
	params := &struct{
		Key string 	`form:"key" json:"key"`
		Num int64 	`form:"num" json:"num"`
	}{}
	if err := c.Bind(params); err != nil || params.Key == ""{
		c.JSON(c.Writer.Status(), map[string]interface{}{
			"err": "params is not valid",
		})
		return
	}
	cmd := &Cmd{CmdType:CmdIncrease, Key:params.Key, Num:params.Num}
	data, err := SerializeCmd(cmd)
	if err != nil {
		c.JSON(c.Writer.Status(), map[string]interface{}{
			"err": "params is not valid",
		})
		return
	}

	store.proposeC <- string(data)
	c.JSON(c.Writer.Status(), map[string]interface{}{
		"msg": "success",
	})
}

//计数器减
func Decrease(c *gin.Context) {
	params := &struct{
		Key string 	`form:"key" json:"key"`
		Num int64 	`form:"num" json:"num"`
	}{}
	if err := c.Bind(params); err != nil || params.Key == ""{
		c.JSON(c.Writer.Status(), map[string]interface{}{
			"err": "params is not valid",
		})
		return
	}
	cmd := &Cmd{CmdType:CmdDecrease, Key:params.Key, Num:params.Num}

	data, err := SerializeCmd(cmd)
	if err != nil {
		c.JSON(c.Writer.Status(), map[string]interface{}{
			"err": "params is not valid",
		})
		return
	}

	store.proposeC <- string(data)
	c.JSON(c.Writer.Status(), map[string]interface{}{
		"msg": "success",
	})
}

func Get(c *gin.Context) {
	params := &struct{
		Key string 	`form:"key" json:"key"`
	}{}
	if err := c.Bind(params); err != nil || params.Key == ""{
		c.JSON(c.Writer.Status(), map[string]interface{}{
			"err": "params is not valid",
		})
		return
	}

	v, ok := store.Get(params.Key)
	if !ok {
		v = 0
	}
	c.JSON(c.Writer.Status(), map[string]interface{}{
		"v": v,
	})
}
