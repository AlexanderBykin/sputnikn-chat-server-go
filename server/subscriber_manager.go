package server

import (
	pb "chatserver/contract/v1"

	"google.golang.org/grpc"
)

type SubscriberManager struct {
	subscribers map[string]grpc.ServerStreamingServer[pb.RoomEventResponse]
}

func NewSubscriberManager() *SubscriberManager {
	return &SubscriberManager{
		subscribers: make(map[string]grpc.ServerStreamingServer[pb.RoomEventResponse]),
	}
}

func (e *SubscriberManager) subscribe(userId string, stream grpc.ServerStreamingServer[pb.RoomEventResponse]) {
	e.subscribers[userId] = stream
}

func (e *SubscriberManager) unsubscribe(userId string) {
	delete(e.subscribers, userId)
}
