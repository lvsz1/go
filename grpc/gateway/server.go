package main

import (
	"encoding/json"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	pb "github.com/lvsz1/go/grpc/gateway/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strings"
)

type Hello struct{}

func (h *Hello) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	fmt.Println("hello world")

	//获取incoming metadata信息
	incomingMd, ok := metadata.FromIncomingContext(ctx)
	if ok {
		fmt.Println("req-name:", incomingMd.Get("req-name"))
	}

	//设置header头,通过grpc-gateway设置的OutgoingHeaderMatcher，将name转换为Grpc-Metadata-Name
	//可以参考https://github.com/grpc/grpc-go/blob/master/Documentation/grpc-metadata.md#sending-and-receiving-metadata---server-side
	outingMd := metadata.Pairs("name", "zhangsan")
	err := grpc.SendHeader(ctx, outingMd) //Grpc-Metadata-Name
	if err != nil {
		fmt.Println("SayHello SendHeader error:", err)
	}

	//添加trailer信息,与http chunked有关，服务端流式时起作用
	//如果添加trailer信息，返回体的header会添加Transfer-Encoding：chunked
	//Trailer 是一个响应首部，允许发送方在分块发送的消息后面添加额外的元信息，
	//这些元信息可能是随着消息主体的发送动态生成的(划重点)，比如消息的完整性校验，消息的数字签名，或者消息经过处理之后的最终状态等。
	trailerMd := metadata.Pairs("school", "neu") //Grpc-trailer-School
	err = grpc.SetTrailer(ctx, trailerMd)
	if err != nil {
		fmt.Println("SayHello SetTrailer error:", err)
	}

	//Grpc-timeout header头设置超时测试，如果http header头设置了Grpc-timeout:1S,则会超时报错：context deadline exceeded
	//time.Sleep(2 * time.Second)

	//@todo 获取client ip

	/**
	error code 测试, 返回如下：
	{
		error: "你的服务器不理你啦：测试错误",
		message: "测试错误",
		code: 1024
	}
	*/
	//return nil, status.New(1024, "测试错误").Err()

	return &pb.HelloResponse{Reply: "hello world"}, nil
}

//根据pb的定义顺序，"/hello/world"不会执行该函数，只会执行SayHello。谁先定义谁被执行
func (h *Hello) SayHello2(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	fmt.Println("hello world2")
	return &pb.HelloResponse{Reply: "hello world2"}, nil
}

func main() {
	rpcListen, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic("failed to listen rpc: " + err.Error())
	}

	svr := grpc.NewServer()
	pb.RegisterHelloServiceServer(svr, &Hello{})

	go func() {
		err = svr.Serve(rpcListen)
		if err != nil {
			panic("rpc server error:" + err.Error())
		}
	}()

	mux := newServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	internalClient, err := grpc.Dial("localhost:8888", opts...)
	if err != nil {
		panic("dial rpc error:" + err.Error())
	}
	err = pb.RegisterHelloServiceHandler(context.Background(), mux, internalClient)
	if err != nil {
		panic("endpoint error:" + err.Error())
	}

	err = http.ListenAndServe(":8889", mux)
	if err != nil {
		panic("gw listen and server error:" + err.Error())
	}

	select {}
}

//创建mux
func newServeMux() *runtime.ServeMux {
	opts := []runtime.ServeMuxOption{
		runtime.WithMarshalerOption("application/plain", &MyMarshaler{}), //默认编解码方式为 &runtime.JSONPb{}
		runtime.WithIncomingHeaderMatcher(IncomingHeaderMatcher),
		runtime.WithOutgoingHeaderMatcher(OutgoingHeaderMatcher),
		runtime.WithMetadata(MyMetadataAnnotator),
		runtime.WithProtoErrorHandler(MyProtoErrorHandlerFunc),
	}
	mux := runtime.NewServeMux(opts...)
	return mux
}

/**
marshaler编解码相关
该模块主要用于编解码http body数据，将post方法的body解析为rpc request。
入口采用http header的Content-Type字段（仅POST方法可用，因为post可以传输body），出口采用http header的Accept字段；
如果marshalers没有配置相关的编解码函数，则采用默认的编解码函数（jsonpb）
*/
//实现runtime.Marshaler的接口
type MyMarshaler struct{}

// Marshal marshals "v" into byte sequence.
func (m *MyMarshaler) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
func (m *MyMarshaler) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (m *MyMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return json.NewDecoder(r)
}
func (m *MyMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return json.NewEncoder(w)
}

func (m *MyMarshaler) ContentType() string {
	return "MyMarshaler"
}

/**
header头处理
主要作用：
1、判断header是否可以传给grpc层；
2、如果传给grpc层，对header做名称映射处理，这样可以将http常规的header与grpc自定义的header头做区分；
例如：
1、在http中加入自定义的header头，一般不会传给grpc层；如果要传到grpc层，需要incomingHeaderMatcher返回true；
2、在http中加入grpc-metadata-device_id,如果grpc层只识别device_id，可以在incomingHeaderMatcher中将metadata返回device_id
*/

/**
与runtime.DefaultHeaderMatcher类似
默认行为：
1、标准header头 => grpcgateway-***
2、grpc规范的header头(Grpc-Metadata-***) => ***
3、其它类型的header头不传给grpc层
*/
func IncomingHeaderMatcher(key string) (string, bool) {
	key = textproto.CanonicalMIMEHeaderKey(key)
	if isPermanentHTTPHeader(key) {
		return runtime.MetadataPrefix + key, true
	} else if strings.HasPrefix(key, runtime.MetadataHeaderPrefix) {
		return key[len(runtime.MetadataHeaderPrefix):], true
	}
	return "", false
}

// 判断是否是标准的header头
func isPermanentHTTPHeader(hdr string) bool {
	switch hdr {
	case
		"Accept",
		"Accept-Charset",
		"Accept-Language",
		"Accept-Ranges",
		"Authorization",
		"Cache-Control",
		"Content-Type",
		"Cookie",
		"Date",
		"Expect",
		"From",
		"Host",
		"If-Match",
		"If-Modified-Since",
		"If-None-Match",
		"If-Schedule-Tag-Match",
		"If-Unmodified-Since",
		"Max-Forwards",
		"Origin",
		"Pragma",
		"Referer",
		"User-Agent",
		"Via",
		"Warning":
		return true
	}
	return false
}

/**
该与runtime中默认的OutgoingHeaderMatcher相同。
将grpc层设置的header加前缀Grpc-Metadata-，例如grpc设置 name：zhangsan，则返回给客户端 Grpc-Metadata-Name：zhangsan
*/
func OutgoingHeaderMatcher(key string) (string, bool) {
	return fmt.Sprintf("%s%s", runtime.MetadataHeaderPrefix, key), true
}

/**
metadataAnnotators []func(context.Context, *http.Request) metadata.MD
作用：
自定义函数，根据context、http.Request，添加键值对到metadata中
*/
/**
例如，如下函数，读取req是否有name字段，如果有name字段，则将name字段的value信息添加到metadata中
*/
func MyMetadataAnnotator(ctx context.Context, req *http.Request) metadata.MD {
	req.ParseForm()
	value := req.FormValue("name")
	if value != "" {
		return metadata.Pairs("req-name", value)
	}
	return nil
}

/**
protoErrorHandler
作用：
处理err错误，根根err，返回http错误码等
*/
func MyProtoErrorHandlerFunc(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, err error) {
	s, ok := status.FromError(err)
	if !ok {
		s = status.New(codes.Unknown, err.Error())
	}

	//根据业务错误代码返回信息
	body := &struct {
		Error   string `json:"error"`
		Message string `json:"message"`
		Code    int32  `json:"code"`
	}{
		Error:   "你的服务器不理你啦：" + s.Message(),
		Message: s.Message(),
		Code:    int32(s.Code()),
	}

	buf, merr := marshaler.Marshal(body)
	if merr != nil {
		grpclog.Infof("Failed to marshal error message %q: %v", body, merr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	st := runtime.HTTPStatusFromCode(s.Code())
	//写http code
	w.WriteHeader(st)
	//返回错误数据
	if _, err := w.Write(buf); err != nil {
		grpclog.Infof("Failed to write response: %v", err)
	}
}

//todo 实现自己的http code判断
func HTTPStatusFromCode(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	}

	return http.StatusInternalServerError
}
