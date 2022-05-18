package yclient

import (
	"context"
	"fmt"
	"github.com/mlaoji/yclient/lib"
	"github.com/mlaoji/yclient/rpc"
	"github.com/mlaoji/ygo/x/log"
	"google.golang.org/grpc/metadata"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type YClient struct {
	Address string
	appid   string
	secret  string
	rpc     *rpc.RpcPool
	logger  *log.Logger
}

var (
	//默认日志输出到文件
	DefaultLogOutput io.Writer

	//默认日志前缀
	DefaultLogPrefix = ""

	//默认日志路径
	DefaultLogPath = "/storage/logs/rpc_client"

	//默认日志级别
	DefaultLogLevel = 7

	//默认GRPC连接超时时间3秒
	DefaultDialTimeout = time.Duration(3) * time.Second

	//默认请求超时时间3秒
	DefaultTimeout = time.Duration(3) * time.Second

	//默认最大空闲连接数
	DefaultMaxIdleConns = 10

	//默认最大连接数
	DefaultMaxOpenConns = 2000
)

var yclient_instance = map[string]*YClient{}
var mutex sync.RWMutex

type FuncOption func(r *rpc.RpcOptions)

//设置参数MaxIdleConns, 支持方法: NewYClient
func WithMaxIdleConns(mic int) FuncOption { // {{{
	return func(r *rpc.RpcOptions) {
		r.MaxIdleConns = mic
	}
} // }}}

//设置参数MaxOpenConns, 支持方法: NewYClient
func WithMaxOpenConns(moc int) FuncOption { // {{{
	return func(r *rpc.RpcOptions) {
		r.MaxOpenConns = moc
	}
} // }}}

//设置参数 Timeout, 支持方法: NewYClient, Request, 用在NewYClient 表示GRPC连接超时时间, 用在Request表示请求超时时间
func WithTimeout(timeout time.Duration) FuncOption { // {{{
	return func(r *rpc.RpcOptions) {
		r.Timeout = timeout
	}
} // }}}

//设置参数Ctx, 支持方法: Request
func WithContext(ctx context.Context) FuncOption { // {{{
	return func(r *rpc.RpcOptions) {
		r.Ctx = ctx
	}
} // }}}

//设置参数Headers, 支持方法: Request
func WithHeaders(headers map[string]string) FuncOption { // {{{
	return func(r *rpc.RpcOptions) {
		r.Headers = headers
	}
} // }}}

func NewYClient(address, appid, secret string, opts ...FuncOption) (*YClient, error) { //{{{
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

func newYClient(address, appid, secret string, opts ...FuncOption) (*YClient, error) { //{{{
	host := strings.Split(address, ":")
	log_path := DefaultLogPath
	log_level := DefaultLogLevel

	env_log_path := os.Getenv("YGO_LOG_PATH")
	if env_log_path != "" {
		log_path = env_log_path + "/rpc_client"
	}

	env_log_level := os.Getenv("YGO_LOG_LEVEL")
	if env_log_level != "" {
		log_level, _ = strconv.Atoi(env_log_level)
	}

	logger := log.NewLogger(log_path, host[0], log_level)

	if DefaultLogOutput != nil {
		logger.SetOutput(DefaultLogOutput)
	}

	if DefaultLogPrefix != "" {
		logger.SetPrefix(DefaultLogPrefix)
	}

	options := &rpc.RpcOptions{
		MaxIdleConns: DefaultMaxIdleConns,
		MaxOpenConns: DefaultMaxOpenConns,
		Timeout:      DefaultDialTimeout,
	}

	for _, opt := range opts {
		opt(options)
	}

	rpclient, err := rpc.New(address, options)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if log_level&log.LevelDebug > 0 {
		rpc.Debug = true
	}

	yclient := &YClient{
		Address: address,
		appid:   appid,
		secret:  secret,
		rpc:     rpclient,
		logger:  logger,
	}

	fmt.Println("yclient init")

	return yclient, nil
} // }}}

func (this *YClient) Request(method string, params interface{}, opts ...FuncOption) (*Response, error) { // {{{
	var data map[string]string
	var err error

	if params != nil {
		data, err = lib.PreParams(params)
		if nil != err {
			this.logger.Error(err, map[string]interface{}{"appid": this.appid, "uri": method}, data)
			return nil, err
		}
	}

	options := &rpc.RpcOptions{
		Timeout: DefaultTimeout,
		Headers: map[string]string{},
	}

	for _, opt := range opts {
		opt(options)
	}

	options.Headers["appid"] = this.appid
	options.Headers["secret"] = this.secret
	if options.Headers["guid"] == "" {
		options.Headers["guid"] = lib.GetGuid(data)
	}

	start_time := time.Now()

	ret, header, trailer, err := this.rpc.Request(method, data, options)
	if nil != err {
		this.logger.Error(err, map[string]interface{}{"appid": this.appid, "uri": method, "options": options}, data)
		return nil, err
	}

	retcode, ok := ret["code"]

	if !ok {
		return nil, fmt.Errorf("response data format error")
	}

	var res map[string]interface{}

	code := int(retcode.(float64))
	if 0 == code {
		retdata, ok := ret["data"]
		if !ok {
			return nil, fmt.Errorf("response data format error")
		}

		retdata = lib.ConvertFloat(retdata)
		res, ok = retdata.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("response data format need map[string]interface{}")
		}
	}

	msg := ret["msg"].(string)
	consume := int(ret["consume"].(float64))
	consume_t := int(time.Now().Sub(start_time).Nanoseconds() / 1000 / 1000)

	this.logger.Access(map[string]interface{}{"appid": this.appid, "uri": method}, data, map[string]interface{}{"code": code, "consume_t": consume_t, "consume": consume, "data": res, "msg": msg})
	if code > 0 {
		this.logger.Warn(map[string]interface{}{"appid": this.appid, "uri": method}, data, map[string]interface{}{"code": code, "consume_t": consume_t, "consume": consume, "data": res, "msg": msg})
	}

	return &Response{
		code:    code,
		msg:     msg,
		data:    res,
		header:  header,
		trailer: trailer,
	}, nil
} //}}}

type Response struct {
	data    map[string]interface{}
	code    int
	msg     string
	header  metadata.MD
	trailer metadata.MD
}

func (r *Response) GetData() map[string]interface{} {
	return r.data
}

func (r *Response) GetCode() int {
	return r.code
}

func (r *Response) GetMsg() string {
	return r.msg
}

func (r *Response) GetHeaders() metadata.MD {
	return r.header
}

func (r *Response) GetHeader(k string) string { // {{{
	h := r.header.Get(k)
	if len(h) > 0 {
		return h[0]
	}

	return ""
} // }}}

func (r *Response) GetTrailers() metadata.MD {
	return r.trailer
}

func (r *Response) GetTrailer(k string) string { // {{{
	t := r.trailer.Get(k)
	if len(t) > 0 {
		return t[0]
	}

	return ""
} // }}}

//obj 必须为指针 单条记录指向 struct , 多条记录指向map[xx]struct, map[xx]*struct, []struct 或 []*struct
func (this *YClient) ParseResult(obj interface{}, data interface{}) error { // {{{
	return lib.ParseResult(obj, data)
} // }}}
