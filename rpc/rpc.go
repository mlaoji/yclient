package rpc

import (
	"encoding/json"
	"fmt"
	pb "github.com/mlaoji/yclient/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type RpcPool struct {
	Addr         string
	pool         chan *RpcClient
	df           DialFunc
	timeout      time.Duration
	maxIdleConns int
	maxOpenConns int
	connRequests int
	lock         sync.RWMutex
}

var (
	Debug = false
)

type DialFunc func(addr string, timout time.Duration) (*RpcClient, error)

func NewRpcPool(addr string, timeout time.Duration, maxIdleConns, maxOpenConns int, df DialFunc) (*RpcPool, error) {
	var client *RpcClient
	var err error
	if maxIdleConns <= 0 {
		maxIdleConns = 1
	}

	if maxOpenConns <= 0 {
		maxOpenConns = 1
	}

	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}

	pool := make([]*RpcClient, 0, maxIdleConns)
	for i := 0; i < maxIdleConns; i++ {
		client, err = df(addr, timeout)
		if err != nil {
			for _, client = range pool {
				client.Close()
			}
			pool = pool[0:]
			break
		}
		pool = append(pool, client)
	}
	p := RpcPool{
		Addr:         addr,
		pool:         make(chan *RpcClient, maxOpenConns),
		df:           df,
		timeout:      timeout,
		maxIdleConns: maxIdleConns,
		maxOpenConns: maxOpenConns,
	}
	for i := range pool {
		p.pool <- pool[i]
	}
	return &p, err
}

func New(addr string, timeout time.Duration, maxIdleConns, maxOpenConns int) (*RpcPool, error) {
	return NewRpcPool(addr, timeout, maxIdleConns, maxOpenConns, DialGrpc)
}

//从连接池中取已建立的连接，不存在则创建
func (p *RpcPool) Get() (*RpcClient, error) {
	Println("get ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
	p.lock.Lock()
	p.connRequests++
	if p.connRequests > p.maxOpenConns {
		p.lock.Unlock()
		Println("wait...")
		select {
		case <-time.After(3 * time.Second):
			return nil, Errorf(ERRNO_UNKNOWN, "pool is full")
		case conn := <-p.pool:
			return conn, nil
		}
	} else {
		p.lock.Unlock()
		select {
		case <-time.After(3 * time.Second):
			return nil, Errorf(ERRNO_UNKNOWN, "pool is full")
		case conn := <-p.pool:
			return conn, nil
		default:
			Println("gen a new conn")
			return p.df(p.Addr, p.timeout)
		}
	}
}

//将连接放回池子
func (p *RpcPool) Put(conn *RpcClient) {
	p.lock.RLock()
	//当前可用连接数大于最大空闲数则直接抛弃
	if len(p.pool) >= p.maxIdleConns {
		p.lock.RUnlock()
		conn.Close()
	} else {
		p.lock.RUnlock()

		if !conn.rpcFail {
			select {
			case p.pool <- conn:
			default:
				conn.Close()
			}
		} else {
			Errorf(ERRNO_UNKNOWN, "rpcClient is error")
			conn.Close()
		}
	}

	p.lock.Lock()
	p.connRequests--
	p.lock.Unlock()

	Println("put ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
}

func (p *RpcPool) Request(timeout time.Duration, method string, params map[string]string) (map[string]interface{}, int, error) {
	c, err := p.Get()
	if err != nil {
		return nil, 0, err
	}
	defer p.Put(c)

	if timeout <= 0 {
		timeout = p.timeout
	}

	data, consume, errs := c.Request(timeout, method, params)

	return data, consume, errs
}

//清空池子并关闭连接
func (p *RpcPool) Empty() {
	var conn *RpcClient
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		default:
			return
		}
	}
}

type RpcClient struct {
	Address string
	client  pb.YGOServiceClient
	conn    *grpc.ClientConn
	rpcFail bool
}

const (
	ERRNO_OK int = iota
	ERRNO_GRPC
	ERRNO_UNKNOWN
)

func DialGrpc(addr string, timeout time.Duration) (*RpcClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}

	return &RpcClient{
		Address: addr,
		client:  pb.NewYGOServiceClient(conn),
		conn:    conn,
	}, nil
}

func (c *RpcClient) Close() error {
	return c.conn.Close()
}

//Request {{{
func (c *RpcClient) Request(timeout time.Duration, method string, params map[string]string) (map[string]interface{}, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	r, err := c.client.Call(ctx, &pb.Request{Method: method, Params: params})

	if err != nil {
		c.rpcFail = true
		return nil, 0, Errorf(ERRNO_GRPC, "%v", err)
	}

	return c.process(r.Response)
} // }}}

//process {{{
func (c *RpcClient) process(res string) (map[string]interface{}, int, error) {
	if res == "" {
		return nil, 0, Errorf(ERRNO_UNKNOWN, "response is empty")
	}

	var ret = make(map[string]interface{}, 0)
	err := json.Unmarshal([]byte(res), &ret)

	if err != nil {
		return nil, 0, Errorf(ERRNO_UNKNOWN, "%v", err)
	}

	if nil == ret["code"] {
		return nil, 0, Errorf(ERRNO_UNKNOWN, "data format error")
	}

	if 0 == ret["code"].(float64) {
		if nil == ret["data"] {
			return nil, 0, Errorf(ERRNO_UNKNOWN, "data format error")
		}

		data := c.convertFloat(ret["data"])
		value, ok := data.(map[string]interface{})
		if !ok {
			return nil, 0, Errorf(ERRNO_UNKNOWN, "response data format need map[string]interface{}")
		}
		return value, int(ret["consume"].(float64)), nil
	}

	return nil, int(ret["consume"].(float64)), Errorf(int(ret["code"].(float64)), ret["msg"].(string))
} // }}}

func (c *RpcClient) convertFloat(r interface{}) interface{} { // {{{
	switch val := r.(type) {
	case map[string]interface{}:
		s := map[string]interface{}{}
		for k, v := range val {
			s[k] = c.convertFloat(v)
		}
		return s
	case []interface{}:
		s := []interface{}{}
		for _, v := range val {
			s = append(s, c.convertFloat(v))
		}
		return s
	case float64:
		return int(val)
	default:
		return r
	}
} // }}}

type rpcError struct {
	code int
	msg  string
}

func (e *rpcError) Error() string {
	return e.msg
}

func Code(err error) int {
	if err == nil {
		return ERRNO_OK
	}
	if e, ok := err.(*rpcError); ok {
		return e.code
	}
	return ERRNO_UNKNOWN
}

func Msg(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(*rpcError); ok {
		return e.msg
	}
	return err.Error()
}

func Errorf(code int, format string, a ...interface{}) error {
	if code == ERRNO_OK {
		return nil
	}
	return &rpcError{
		code: code,
		msg:  fmt.Sprintf(format, a...),
	}
}

func Println(a ...interface{}) (n int, err error) {
	if Debug {
		return fmt.Println(a...)
	}

	return
}
