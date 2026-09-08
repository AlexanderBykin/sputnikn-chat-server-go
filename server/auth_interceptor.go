package server

import (
	"context"
	"log"

	"chatserver/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chatServicePath = "/ru.alexshniperson.sputnikn.api.contract.v1.ChatService/"

var (
	nonAuthRPCMethods = map[string]bool{
		chatServicePath + "AuthUser": true,
	}
)

// Made with article:
// https://dev.to/techschoolguru/use-grpc-interceptor-for-authorization-with-jwt-1c5h
type AuthInterceptor struct {
	tokenManager *JWTManager
}

func NewAuthInterceptor(tokenManager *JWTManager) *AuthInterceptor {
	return &AuthInterceptor{
		tokenManager: tokenManager,
	}
}

func (e *AuthInterceptor) authorize(ctx context.Context, method string) error {
	// Check method granted access without AccessToken
	if val, ok := nonAuthRPCMethods[method]; ok && val {
		return nil
	}

	accessToken, err := utils.GetAccessTokenFromContext(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "%v", err.Error())
	}

	claims, err := e.tokenManager.VerifyToken(*accessToken)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "access token is invalid: %v", err)
	}
	if len(claims.UserId) > 0 {
		// User granted access
		return nil
	}

	return status.Error(codes.PermissionDenied, "no permission to access this RPC")
}

func (e *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		log.Println("--> unary interceptor: ", info.FullMethod)

		err := e.authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func (e *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		log.Println("--> stream interceptor: ", info.FullMethod)

		err := e.authorize(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}

		return handler(srv, stream)
	}
}
