target:
	@protoc -Iproto \
    		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
    		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway \
    		--go_out=plugins=grpc:proto \
    		--grpc-gateway_out=logtostderr=true:proto \
    		proto/*.proto