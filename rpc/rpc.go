package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mlaoji/ygo/x/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"sync"
	"time"
)

type RpcPool struct {
	Addr         string
	pool         chan *RpcClient
	df           DialFunc
	connRequests int
	lock         sync.RWMutex
	options      *RpcOptions
}

type RpcOptions struct {
	Timeout      time.Duration
	MaxIdleConns int
	MaxOpenConns int
	Headers      map[string]string
	Ctx          context.Context
}

var (
	Debug = false
)

type DialFunc func(addr string, timout time.Duration) (*RpcClient, error)

func NewRpcPool(addr string, df DialFunc, options *RpcOptions) (*RpcPool, error) { // {{{
	var client *RpcClient
	var err error

	if options.MaxIdleConns <= 0 {
		options.MaxIdleConns = 1
	}

	if options.MaxOpenConns <= 0 {
		options.MaxOpenConns = 1
	}

	if options.MaxIdleConns > options.MaxOpenConns {
		options.MaxIdleConns = options.MaxOpenConns
	}

	pool := make([]*RpcClient, 0, options.MaxIdleConns)
	for i := 0; i < options.MaxIdleConns; i++ {
		client, err = df(addr, options.Timeout)
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
		Addr:    addr,
		pool:    make(chan *RpcClient, options.MaxOpenConns),
		df:      df,
		options: options,
	}
	for i := range pool {
		p.pool <- pool[i]
	}
	return &p, err
} // }}}

func New(addr string, options *RpcOptions) (*RpcPool, error) {
	return NewRpcPool(addr, DialGrpc, options)
}

//从连接池中取已建立的连接，不存在则创建
func (p *RpcPool) Get() (*RpcClient, error) { // {{{
	Println("get ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
	p.lock.Lock()
	p.connRequests++
	if p.connRequests > p.options.MaxOpenConns {
		p.lock.Unlock()
		Println("wait...")
		select {
		case <-time.After(3 * time.Second):
			return nil, fmt.Errorf("pool is full")
		case conn := <-p.pool:
			return conn, nil
		}
	} else {
		p.lock.Unlock()
		select {
		case <-time.After(3 * time.Second):
			return nil, fmt.Errorf("pool is full")
		case conn := <-p.pool:
			return conn, nil
		default:
			Println("gen a new conn")
			return p.df(p.Addr, p.options.Timeout)
		}
	}
} // }}}

//将连接放回池子
func (p *RpcPool) Put(conn *RpcClient) { // {{{
	p.lock.RLock()
	//当前可用连接数大于最大空闲数则直接抛弃
	if len(p.pool) >= p.options.MaxIdleConns {
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
			fmt.Errorf("rpcClient error")
			conn.Close()
		}
	}

	p.lock.Lock()
	p.connRequests--
	p.lock.Unlock()

	Println("put ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
} // }}}

func (p *RpcPool) Request(method string, params map[string]string, options *RpcOptions) (map[string]interface{}, metadata.MD, metadata.MD, error) { // {{{
	c, err := p.Get()
	if err != nil {
		return nil, nil, nil, err
	}
	defer p.Put(c)

	return c.Request(method, params, options)
} // }}}

//清空池子并关闭连接
func (p *RpcPool) Empty() { // {{{
	var conn *RpcClient
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		default:
			return
		}
	}
} // }}}

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

func DialGrpc(addr string, timeout time.Duration) (*RpcClient, error) { // {{{
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}

	return &RpcClient{
		Address: addr,
		client:  pb.NewYGOServiceClient(conn),
		conn:    conn,
	}, nil
} // }}}

func (c *RpcClient) Close() error {
	return c.conn.Close()
}

func (c *RpcClient) Request(method string, params map[string]string, options *RpcOptions) (map[string]interface{}, metadata.MD, metadata.MD, error) { //{{{
	if options.Ctx == nil {
		options.Ctx = context.Background()
	}

	if options.Timeout > 0 {
		var cancel context.CancelFunc
		options.Ctx, cancel = context.WithTimeout(options.Ctx, options.Timeout)
		defer cancel()
	}

	if len(options.Headers) > 0 {
		md := metadata.MD{}
		for k, v := range options.Headers {
			md.Append(k, v)
		}

		options.Ctx = metadata.NewOutgoingContext(options.Ctx, md)
	}

	var header, trailer metadata.MD
	r, err := c.client.Call(options.Ctx, &pb.Request{Method: method, Params: params}, grpc.Header(&header), grpc.Trailer(&trailer))

	if err != nil {
		c.rpcFail = true
		return nil, nil, nil, fmt.Errorf("grpc call error: %+v", err)
	}

	data, err := c.process(r.Response)
	if err != nil {
		return nil, nil, nil, err
	}

	return data, header, trailer, nil
} // }}}

func (c *RpcClient) process(res []byte) (map[string]interface{}, error) { //{{{
	if res == nil {
		return nil, fmt.Errorf("response is empty")
	}

	var ret = make(map[string]interface{}, 0)
	err := json.Unmarshal(res, &ret)
	if err != nil {
		return nil, fmt.Errorf("result parse error: %+v", err)
	}

	return ret, nil
} // }}}

func Println(a ...interface{}) (n int, err error) { // {{{
	if Debug {
		return fmt.Println(a...)
	}

	return
} // }}}
