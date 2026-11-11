package utils

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
)

func GetUserIdFromContext(ctx context.Context) (*string, error) {
	return nil, errors.New("error")
}

func GetAccessTokenFromContext(ctx context.Context) (*string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("metadata is not provided")
	}

	values := md["authorization"]
	if len(values) == 0 {
		return nil, errors.New("authorization token is not provided")
	}

	accessToken := values[0]
	if len(accessToken) == 0 {
		return nil, errors.New("token can't be empty")
	}

	return &accessToken, nil
}
