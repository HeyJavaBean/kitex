/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"runtime/debug"

	"github.com/cloudwego/kitex/pkg/remote/trans"

	"github.com/cloudwego/kitex/pkg/stats"

	json "github.com/bytedance/sonic"
	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/utils"
)

type svrTransHandlerFactory struct{}

// NewSvrTransHandlerFactory ...
func NewSvrTransHandlerFactory() remote.ServerTransHandlerFactory {
	return &svrTransHandlerFactory{}
}

func (f *svrTransHandlerFactory) NewTransHandler(opt *remote.ServerOption) (remote.ServerTransHandler, error) {
	return newSvrTransHandler(opt)
}

func newSvrTransHandler(opt *remote.ServerOption) (*svrTransHandler, error) {
	return &svrTransHandler{
		opt:           opt,
		targetSvcInfo: opt.TargetSvcInfo,
	}, nil
}

var _ remote.ServerTransHandler = &svrTransHandler{}

type svrTransHandler struct {
	opt           *remote.ServerOption
	inkHdlFunc    endpoint.Endpoint
	targetSvcInfo *serviceinfo.ServiceInfo

	transPipe *remote.TransPipeline
	// fixme 不能放在全局
	responseWriter *httpResponseWriter
	ext            trans.Extension
}

var httpReg = regexp.MustCompile(`^(?:GET |POST|PUT|DELE|HEAD|OPTI|CONN|TRAC|PATC)$`)

func (t *svrTransHandler) ProtocolMatch(ctx context.Context, conn net.Conn) (err error) {
	c, ok := conn.(netpoll.Connection)
	if ok {
		// todo handler https
		pre, _ := c.Reader().Peek(4)
		if httpReg.Match(pre) {
			return nil
		}
	}

	return errors.New("error protocol not match")
}

func (t *svrTransHandler) Write(ctx context.Context, conn net.Conn, sendMsg remote.Message) (nctx context.Context, err error) {
	ri := sendMsg.RPCInfo()
	rpcinfo.Record(ctx, ri, stats.WriteStart, nil)
	defer func() {
		rpcinfo.Record(ctx, ri, stats.WriteFinish, err)
	}()

	respStruct := sendMsg.Data().(utils.KitexResult).GetResult()

	data, err := json.Marshal(respStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal http response: %w", err)
	}

	t.responseWriter.WriteHeader(http.StatusOK)
	_, _ = t.responseWriter.Write(data)
	// user http extension middleware?

	return ctx, nil
}

var TLSConfig *tls.Config

func init() {
	return

	TLSConfig = &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
	}
	cert, _ := tls.LoadX509KeyPair("/Users/bytedance/server.crt", "/Users/bytedance/server.key")
	TLSConfig.Certificates = append(TLSConfig.Certificates, cert)

	TLSConfig.BuildNameToCertificate()
}

func (t *svrTransHandler) Read(ctx context.Context, conn net.Conn, recvMsg remote.Message) (nctx context.Context, err error) {
	ri := recvMsg.RPCInfo()
	rpcinfo.Record(ctx, ri, stats.ReadStart, nil)

	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			panicErr := kerrors.ErrPanic.WithCauseAndStack(fmt.Errorf("[happened in Read] %s", r), stack)
			rpcinfo.AsMutableRPCStats(ri.Stats()).SetPanicked(panicErr)
			err = remote.NewTransError(remote.ProtocolError, panicErr)
			nctx = ctx
		}
		// t.ext.ReleaseBuffer(bufReader, err)
		if err != nil {
			recvMsg.Tags()[remote.ReadFailed] = true
		}
		rpcinfo.Record(ctx, ri, stats.ReadFinish, err)
	}()

	// todo http1.1 连接复用、断开链接 循环处理

	httpReq, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to read http request: %w", err)
	}
	defer httpReq.Body.Close()

	methodName, method := t.targetSvcInfo.HTTPMethodInfo(httpReq.Method, httpReq.URL.Path)
	if method == nil {
		return nil, fmt.Errorf("not found")
	}

	if ink, ok := ri.Invocation().(rpcinfo.InvocationSetter); ok {
		ink.SetServiceName(t.targetSvcInfo.ServiceName)
		ink.SetMethodName(methodName)
	}

	if err = BindRequest(recvMsg, methodName, httpReq); err != nil {
		return nil, err
	}

	return ctx, nil
}

// OnRead 只 return write err
func (t *svrTransHandler) OnRead(ctx context.Context, conn net.Conn) (err error) {
	ctx, ri := t.newCtxWithRPCInfo(ctx, conn)
	// t.ext.SetReadTimeout(ctx, conn, ri.Config(), remote.Server)
	var recvMsg remote.Message
	var sendMsg remote.Message
	defer func() {
		var panicErr error
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			if conn != nil {
				ri := rpcinfo.GetRPCInfo(ctx)
				rService, rAddr := getRemoteInfo(ri, conn)
				klog.CtxErrorf(ctx, "KITEX: panic happened, remoteAddress=%s, remoteService=%s, error=%v\nstack=%s", rAddr, rService, r, stack)
			} else {
				klog.CtxErrorf(ctx, "KITEX: panic happened, error=%v\nstack=%s", r, stack)
			}
			panicErr = kerrors.ErrPanic.WithCauseAndStack(fmt.Errorf("[happened in OnRead] %v", r), stack)
			if err == nil {
				err = panicErr
			}
		}
		if err != nil {
			t.responseWriter.WriteHeader(http.StatusInternalServerError)
			t.responseWriter.Write([]byte(err.Error()))
		}

		// todo 调整 flush 和读写的位置实现？
		// send http request and reset writer
		err = t.responseWriter.flush()
		if err != nil {
			klog.CtxErrorf(ctx, "KITEX: http write response failed, error=%v", err)
		}
		t.responseWriter = nil

		t.finishTracer(ctx, ri, err, panicErr)
		t.finishProfiler(ctx)
		remote.RecycleMessage(recvMsg)
		remote.RecycleMessage(sendMsg)
		// reset rpcinfo for reuse
		if rpcinfo.PoolEnabled() {
			t.opt.InitOrResetRPCInfoFunc(ri, conn.RemoteAddr())
		}
	}()
	ctx = t.startTracer(ctx, ri)
	ctx = t.startProfiler(ctx)

	recvMsg = remote.NewMessageWithNewer(t.targetSvcInfo, t.opt.SvcSearcher, ri, remote.Call, remote.Server)

	if TLSConfig != nil {
		tlsConn := tls.Server(conn, TLSConfig)
		if err = tlsConn.Handshake(); err != nil {
			return fmt.Errorf("handshake error: %w", err)
		}
		conn = tlsConn
	}

	t.responseWriter = newHTTPResponseWriter(conn)

	// 跳转上面的 Read，匹配 method，解码 binding request 到 recvMsg Data 里
	ctx, err = t.transPipe.Read(ctx, conn, recvMsg)
	if err != nil {
		return err
	}

	// todo 构造 sendMsg 填充
	svcInfo := recvMsg.ServiceInfo()
	methodInfo := svcInfo.MethodInfo(ri.Invocation().MethodName())
	sendMsg = remote.NewMessage(methodInfo.NewResult(), svcInfo, ri, remote.Reply, remote.Server)

	ctx, err = t.transPipe.OnMessage(ctx, recvMsg, sendMsg)
	if err != nil {
		return err
	}

	ctx, err = t.transPipe.Write(ctx, conn, sendMsg)
	if err != nil {
		return err
	}

	// todo 跨域支持、cookie、session管理？压缩设置？

	return
}

func (t *svrTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	err := t.inkHdlFunc(ctx, args.Data(), result.Data())
	return ctx, err
}

func (t *svrTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	// init rpcinfo
	ri := t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
	return rpcinfo.NewCtxWithRPCInfo(ctx, ri), nil
}

func (t *svrTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	// recycle rpcinfo
	rpcinfo.PutRPCInfo(rpcinfo.GetRPCInfo(ctx))
}

func (t *svrTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	var de *kerrors.DetailedError
	if ok := errors.As(err, &de); ok && de.Stack() != "" {
		klog.CtxErrorf(ctx, "KITEX: processing HTTP request error, remoteAddr=%s, error=%s\nstack=%s", conn.RemoteAddr(), err.Error(), de.Stack())
	} else {
		klog.CtxErrorf(ctx, "KITEX: processing HTTP request error, remoteAddr=%s, error=%s", conn.RemoteAddr(), err.Error())
	}
}

func (t *svrTransHandler) SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint) {
	t.inkHdlFunc = inkHdlFunc
}

// SetPipeline implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) SetPipeline(p *remote.TransPipeline) {
	t.transPipe = p
}

func (t *svrTransHandler) GracefulShutdown(ctx context.Context) error {
	return nil
}

func (t *svrTransHandler) startTracer(ctx context.Context, ri rpcinfo.RPCInfo) context.Context {
	c := t.opt.TracerCtl.DoStart(ctx, ri)
	return c
}

func (t *svrTransHandler) finishTracer(ctx context.Context, ri rpcinfo.RPCInfo, err error, panicErr interface{}) {
	rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats())
	if rpcStats == nil {
		return
	}
	if panicErr != nil {
		rpcStats.SetPanicked(panicErr)
	}
	t.opt.TracerCtl.DoFinish(ctx, ri, err)
	rpcStats.Reset()
}

func (t *svrTransHandler) startProfiler(ctx context.Context) context.Context {
	if t.opt.Profiler == nil {
		return ctx
	}
	return t.opt.Profiler.Prepare(ctx)
}

func (t *svrTransHandler) finishProfiler(ctx context.Context) {
	if t.opt.Profiler == nil {
		return
	}
	t.opt.Profiler.Untag(ctx)
}

func getRemoteInfo(ri rpcinfo.RPCInfo, conn net.Conn) (string, net.Addr) {
	rAddr := conn.RemoteAddr()
	if ri == nil {
		return "", rAddr
	}
	if rAddr != nil && rAddr.Network() == "unix" {
		if ri.From().Address() != nil {
			rAddr = ri.From().Address()
		}
	}
	return ri.From().ServiceName(), rAddr
}

func (t *svrTransHandler) newCtxWithRPCInfo(ctx context.Context, conn net.Conn) (context.Context, rpcinfo.RPCInfo) {
	var ri rpcinfo.RPCInfo
	if rpcinfo.PoolEnabled() { // reuse per-connection rpcinfo
		ri = rpcinfo.GetRPCInfo(ctx)
		// delayed reinitialize for faster response
	} else {
		// new rpcinfo if reuse is disabled
		ri = t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
		ctx = rpcinfo.NewCtxWithRPCInfo(ctx, ri)
	}
	//if atomic.LoadUint32(&t.inGracefulShutdown) == 1 {
	//	// If server is in graceful shutdown status, mark connection reset flag to all responses to let client close the connections.
	//	if ei := rpcinfo.AsTaggable(ri.To()); ei != nil {
	//		ei.SetTag(rpcinfo.ConnResetTag, "1")
	//	}
	//}
	return ctx, ri
}
