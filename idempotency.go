package idempotency

import (
	"context"
	"github.com/gogo/status"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"unicode/utf8"
)

const KeyHeader = "Idempotency-Key"
const XKeyHeader = "X-Idempotency-Key"

type Storage interface {
	Get(key string, data interface{}) (res interface{}, err error)
	Set(key string, data interface{})
}

type ACL interface {
	Check(ctx context.Context, method string) interface{}
}

var ErrHeaderNotFound = status.Errorf(codes.InvalidArgument, "header %s or %s not found", KeyHeader, XKeyHeader)

func UnaryServerInterceptor(storage Storage, acl ACL) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		obj := acl.Check(ctx, info.FullMethod)
		if obj == nil {
			return handler(ctx, req)
		}
		key, err := getKey(ctx)
		if err != nil {
			return nil, err
		}
		if result, err := storage.Get(key, obj); result != nil || err != nil {
			return result, err
		}
		resp, err = handler(ctx, req)
		if err != nil {
			storage.Set(key, status.Convert(err).Proto())
		} else {
			storage.Set(key, resp)
		}
		return resp, err
	}
}

func IncomingHeaderMatcher(key string) (string, bool) {
	switch key {
	case KeyHeader, XKeyHeader:
		return key, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

func getKey(ctx context.Context) (string, error) {
	md := metautils.ExtractIncoming(ctx)
	if key := md.Get(KeyHeader); utf8.RuneCountInString(key) != 0 {
		return key, nil
	}
	if key := md.Get(XKeyHeader); utf8.RuneCountInString(key) != 0 {
		return key, nil
	}
	return "", ErrHeaderNotFound
}
