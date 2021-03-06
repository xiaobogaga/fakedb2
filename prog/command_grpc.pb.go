// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package prog

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ServiceMethodsClient is the client API for ServiceMethods service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ServiceMethodsClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Response, error)
	Get2(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Response, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*Response, error)
	Begin(ctx context.Context, in *BeginRequest, opts ...grpc.CallOption) (*Response, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*Response, error)
	Rollback(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*Response, error)
	Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (*Response, error)
	Close(ctx context.Context, in *CloseRequest, opts ...grpc.CallOption) (*Response, error)
	Del(ctx context.Context, in *DelRequest, opts ...grpc.CallOption) (*Response, error)
	Flush(ctx context.Context, in *FlushRequest, opts ...grpc.CallOption) (*Response, error)
	FlushDirtyPage(ctx context.Context, in *FlushDirtyPageRequest, opts ...grpc.CallOption) (*Response, error)
	CheckPoint(ctx context.Context, in *CheckPointRequest, opts ...grpc.CallOption) (*Response, error)
}

type serviceMethodsClient struct {
	cc grpc.ClientConnInterface
}

func NewServiceMethodsClient(cc grpc.ClientConnInterface) ServiceMethodsClient {
	return &serviceMethodsClient{cc}
}

func (c *serviceMethodsClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Get2(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Get2", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Begin(ctx context.Context, in *BeginRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Begin", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Commit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Rollback(ctx context.Context, in *RollbackRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Rollback", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Init", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Close(ctx context.Context, in *CloseRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Close", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Del(ctx context.Context, in *DelRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Del", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) Flush(ctx context.Context, in *FlushRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/Flush", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) FlushDirtyPage(ctx context.Context, in *FlushDirtyPageRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/FlushDirtyPage", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serviceMethodsClient) CheckPoint(ctx context.Context, in *CheckPointRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/prog.ServiceMethods/CheckPoint", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ServiceMethodsServer is the server API for ServiceMethods service.
// All implementations must embed UnimplementedServiceMethodsServer
// for forward compatibility
type ServiceMethodsServer interface {
	Get(context.Context, *GetRequest) (*Response, error)
	Get2(context.Context, *GetRequest) (*Response, error)
	Set(context.Context, *SetRequest) (*Response, error)
	Begin(context.Context, *BeginRequest) (*Response, error)
	Commit(context.Context, *CommitRequest) (*Response, error)
	Rollback(context.Context, *RollbackRequest) (*Response, error)
	Init(context.Context, *InitRequest) (*Response, error)
	Close(context.Context, *CloseRequest) (*Response, error)
	Del(context.Context, *DelRequest) (*Response, error)
	Flush(context.Context, *FlushRequest) (*Response, error)
	FlushDirtyPage(context.Context, *FlushDirtyPageRequest) (*Response, error)
	CheckPoint(context.Context, *CheckPointRequest) (*Response, error)
	mustEmbedUnimplementedServiceMethodsServer()
}

// UnimplementedServiceMethodsServer must be embedded to have forward compatible implementations.
type UnimplementedServiceMethodsServer struct {
}

func (UnimplementedServiceMethodsServer) Get(context.Context, *GetRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedServiceMethodsServer) Get2(context.Context, *GetRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get2 not implemented")
}
func (UnimplementedServiceMethodsServer) Set(context.Context, *SetRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Set not implemented")
}
func (UnimplementedServiceMethodsServer) Begin(context.Context, *BeginRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Begin not implemented")
}
func (UnimplementedServiceMethodsServer) Commit(context.Context, *CommitRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Commit not implemented")
}
func (UnimplementedServiceMethodsServer) Rollback(context.Context, *RollbackRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Rollback not implemented")
}
func (UnimplementedServiceMethodsServer) Init(context.Context, *InitRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Init not implemented")
}
func (UnimplementedServiceMethodsServer) Close(context.Context, *CloseRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Close not implemented")
}
func (UnimplementedServiceMethodsServer) Del(context.Context, *DelRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Del not implemented")
}
func (UnimplementedServiceMethodsServer) Flush(context.Context, *FlushRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Flush not implemented")
}
func (UnimplementedServiceMethodsServer) FlushDirtyPage(context.Context, *FlushDirtyPageRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FlushDirtyPage not implemented")
}
func (UnimplementedServiceMethodsServer) CheckPoint(context.Context, *CheckPointRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CheckPoint not implemented")
}
func (UnimplementedServiceMethodsServer) mustEmbedUnimplementedServiceMethodsServer() {}

// UnsafeServiceMethodsServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ServiceMethodsServer will
// result in compilation errors.
type UnsafeServiceMethodsServer interface {
	mustEmbedUnimplementedServiceMethodsServer()
}

func RegisterServiceMethodsServer(s grpc.ServiceRegistrar, srv ServiceMethodsServer) {
	s.RegisterService(&ServiceMethods_ServiceDesc, srv)
}

func _ServiceMethods_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Get2_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Get2(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Get2",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Get2(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Set",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Begin_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BeginRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Begin(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Begin",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Begin(ctx, req.(*BeginRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Commit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Commit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Commit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Commit(ctx, req.(*CommitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Rollback_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RollbackRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Rollback(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Rollback",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Rollback(ctx, req.(*RollbackRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Init_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Init(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Init",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Init(ctx, req.(*InitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Close_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Close(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Close",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Close(ctx, req.(*CloseRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Del_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Del(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Del",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Del(ctx, req.(*DelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_Flush_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FlushRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).Flush(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/Flush",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).Flush(ctx, req.(*FlushRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_FlushDirtyPage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FlushDirtyPageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).FlushDirtyPage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/FlushDirtyPage",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).FlushDirtyPage(ctx, req.(*FlushDirtyPageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ServiceMethods_CheckPoint_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CheckPointRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ServiceMethodsServer).CheckPoint(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/prog.ServiceMethods/CheckPoint",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ServiceMethodsServer).CheckPoint(ctx, req.(*CheckPointRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ServiceMethods_ServiceDesc is the grpc.ServiceDesc for ServiceMethods service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ServiceMethods_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "prog.ServiceMethods",
	HandlerType: (*ServiceMethodsServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _ServiceMethods_Get_Handler,
		},
		{
			MethodName: "Get2",
			Handler:    _ServiceMethods_Get2_Handler,
		},
		{
			MethodName: "Set",
			Handler:    _ServiceMethods_Set_Handler,
		},
		{
			MethodName: "Begin",
			Handler:    _ServiceMethods_Begin_Handler,
		},
		{
			MethodName: "Commit",
			Handler:    _ServiceMethods_Commit_Handler,
		},
		{
			MethodName: "Rollback",
			Handler:    _ServiceMethods_Rollback_Handler,
		},
		{
			MethodName: "Init",
			Handler:    _ServiceMethods_Init_Handler,
		},
		{
			MethodName: "Close",
			Handler:    _ServiceMethods_Close_Handler,
		},
		{
			MethodName: "Del",
			Handler:    _ServiceMethods_Del_Handler,
		},
		{
			MethodName: "Flush",
			Handler:    _ServiceMethods_Flush_Handler,
		},
		{
			MethodName: "FlushDirtyPage",
			Handler:    _ServiceMethods_FlushDirtyPage_Handler,
		},
		{
			MethodName: "CheckPoint",
			Handler:    _ServiceMethods_CheckPoint_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "command.proto",
}
