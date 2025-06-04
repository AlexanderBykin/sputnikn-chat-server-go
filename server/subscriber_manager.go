package server

import (
	pb "chatserver/contract/v1"
	"slices"

	"google.golang.org/grpc"
)

type SubscriberManagerListener interface {
	onConnectedSubscriber(userId string)
	onDisconnectedSubscriber(userId string)
}

type SubscriberManager struct {
	subscribers map[string]grpc.ServerStreamingServer[pb.RoomEventResponse]
	listeners   []SubscriberManagerListener
}

func NewSubscriberManager() *SubscriberManager {
	return &SubscriberManager{
		subscribers: make(map[string]grpc.ServerStreamingServer[pb.RoomEventResponse]),
		listeners:   make([]SubscriberManagerListener, 0),
	}
}

func (e *SubscriberManager) subscribe(userId string, stream grpc.ServerStreamingServer[pb.RoomEventResponse]) {
	e.subscribers[userId] = stream
	for _, listener := range e.listeners {
		listener.onConnectedSubscriber(userId)
	}
}

func (e *SubscriberManager) unsubscribe(userId string) {
	delete(e.subscribers, userId)
	for _, listener := range e.listeners {
		listener.onDisconnectedSubscriber(userId)
	}
}

func (e *SubscriberManager) addListener(listener SubscriberManagerListener) {
	e.listeners = append(e.listeners, listener)
}

func (e *SubscriberManager) removeListener(listener SubscriberManagerListener) {
	index := slices.Index(e.listeners, listener)
	if index >= 0 {
		e.listeners = slices.Delete(e.listeners, index, 1)
	}
}
