package yclient

/*
Usage:// {{{
	//默认超时时间3秒，连接池数量 10
	c,err := yclient.NewYClient("127.0.0.1:5501", "live", "f9KDF93fhsd")

	//设置超时 1 秒
	c,err := yclient.NewYClient("127.0.0.1:5501", "live", "f9KDF93fhsd", 1)

	//设置连接池 20 个
	c,err := yclient.NewYClient("127.0.0.1:5501", "live", "f9KDF93fhsd", 1, 20)

	if err != nil {
		fmt.Printf("%#v\n", err)
		return
	}

	data, error := c.Request("topic/GetTopicInfo", map[string]string{"tid": "423423423"})

	if error != nil {
		fmt.Printf("%#v", error)

		//获取错误码
		errno := c.Errno()
		fmt.Printf("%#v", errno)

	} else {

		fmt.Println("ok")
		fmt.Printf("%#v", data)
	}// }}}
*/
import (
	"fmt"
	"github.com/mlaoji/yclient/lib"
	"github.com/mlaoji/yclient/rpc"
	ylib "github.com/mlaoji/ygo/lib"
	"strings"
	"sync"
	"time"
)

type YClient struct {
	Appid   string
	Secret  string
	Address string
	rpc     *rpc.RpcPool
	logger  *ylib.FileLogger
}

var (
	DefaultLogpath  = "/storage/logs/rpc_client"
	DefaultLoglevel = 0
	Debug           = false
)

var yclient_instance = map[string]*YClient{}
var mutex sync.RWMutex

//NewYClient {{{
func NewYClient(address, appid, secret string, opts ...int /*timeout(seconds), maxIdleConns, maxOpenConns*/) (*YClient, error) {
	mutex.RLock()
	if nil == yclient_instance[address] {
		mutex.RUnlock()
		mutex.Lock()
		defer mutex.Unlock()

		if nil == yclient_instance[address] {
			var err error
			yclient_instance[address], err = newYClient(address, appid, secret, opts...)
			if nil != err {
				return nil, err
			}
			fmt.Printf("newins:%#v\n", address)
		}
	} else {
		mutex.RUnlock()
	}

	return yclient_instance[address], nil
} // }}}

//newYClient {{{
func newYClient(address, appid, secret string, opts ...int /*timeout(seconds), maxIdleConns, maxOpenConns*/) (*YClient, error) {
	timeout := 3
	max_idle_conns := 10
	max_open_conns := 2000

	if len(opts) > 0 {
		timeout = opts[0]
	}

	if len(opts) > 1 {
		max_idle_conns = opts[1]
	}

	if len(opts) > 2 {
		max_open_conns = opts[2]
	}

	host := strings.Split(address, ":")
	logger := ylib.NewLogger(DefaultLogpath, host[0], DefaultLoglevel)

	rpclient, err := rpc.New(address, time.Duration(timeout)*time.Second, max_idle_conns, max_open_conns)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	rpc.Debug = Debug

	yclient := &YClient{
		Address: address,
		Appid:   appid,
		Secret:  secret,
		rpc:     rpclient,
		logger:  logger,
	}

	fmt.Println("yclient init")

	return yclient, nil
} // }}}

func (this *YClient) Request(method string, args ...interface{}) (map[string]interface{}, error) {
	return this.RequestTimeout(time.Duration(0), method, args...)
}

func (this *YClient) RequestTimeout(timeout time.Duration, method string, args ...interface{}) (map[string]interface{}, error) { //{{{
	var params map[string]string
	var err error
	for _, v := range args {
		params, err = lib.PreParams(params, v)
		if nil != err {
			this.logger.Error(err, v)
			return nil, err
		}
	}

	params["appid"] = this.Appid
	params["secret"] = this.Secret
	params["guid"] = lib.GetGuid(params)

	start_time := time.Now()

	res, consume, err := this.rpc.Request(timeout, method, params)

	consume_t := int(time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000)
	delete(params, "secret")
	if nil == err {
		this.logger.Access(map[string]interface{}{"uri": method}, params, map[string]interface{}{"code": 0, "consume_t": consume_t, "consume": consume, "data": res})
	} else {
		errno := rpc.Code(err)
		errmsg := rpc.Msg(err)
		if errno > 0 {
			this.logger.Warn(map[string]interface{}{"uri": method}, params, map[string]interface{}{"code": errno, "consume_t": consume_t, "consume": consume, "msg": errmsg})
		} else {
			this.logger.Error(map[string]interface{}{"uri": method}, params, map[string]interface{}{"code": errno, "consume_t": consume_t, "consume": consume, "msg": errmsg})
		}
	}

	return res, err
} // }}}

func (this *YClient) Errno(err error) int {
	return int(rpc.Code(err))
}

func (this *YClient) Error(err error) string {
	return rpc.Msg(err)
}

//obj 必须为指针 单条记录指向 struct , 多条记录指向map[xx]struct, map[xx]*struct, []struct 或 []*struct
func (this *YClient) ParseResult(obj interface{}, data interface{}) error { // {{{
	return lib.ParseResult(obj, data)
} // }}}
