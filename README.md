 # YClient
 ### version 1.0

 go client for YGO

 ## Quick Start
	//设置日志路径和级别:
	//如果在ygo框架中使用, 无需配置, 会自动继承框架中的配置
	yclient.DefaultLogpath = "logs"
	yclient.DefaultLoglevel= 7


	//使用默认配置
	c,err := yclient.NewYClient("127.0.0.1:9002", "appid", "secret")

	//设置grpc连接超时时间
	c,err := yclient.NewYClient("127.0.0.1:9002", "appid", "secret", yclient.WithTimeout(time.Duration(1) * time.Second))

	//设置连接池: 空闲连接数,  最大连接数
	c,err := yclient.NewYClient("127.0.0.1:9002", "appid", "secret", yclient.WithMaxIdleConns(5), yclient.WithMaxOpenConns(200))

	if err != nil {
		fmt.Printf("%#v\n", err)
		return
	}

	//无参数请求
	data, err := c.Request("user/GetUserInfo", nil)

	//有参数请求
	data, err := c.Request("user/GetUserInfo", map[string]interface{}{"uid": "123"})

	//设置请求超时时间
	data, err := c.Request("user/GetUserInfo", nil, yclient.WithTimeout(time.Duration(1) * time.Second))

	//设置请求context
	data, err := c.Request("user/GetUserInfo", nil, yclient.WithContext(ctx))

	//设置请求header
	res, err := c.Request("user/GetUserInfo", nil, yclient.WithHeaders(map[string]string{"test-header":"header-value"}))

	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	fmt.Printf("code: %#v", res.GetCode())
	fmt.Printf("msg: %#v", res.GetMsg())
	fmt.Printf("data: %#v", res.GetData())
	fmt.Printf("header: %#v", res.GetHeaders())
	fmt.Printf("trailer: %#v", res.GetTrailers())
