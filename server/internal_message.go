package server

import (
	pb "chatserver/contract/v1"
	"time"
)

type MessageToRoom struct {
	Message any
	OutChan *chan any
}

type GetRoomDetailInternal struct{}

type UserConnectedInternal struct {
	UserId string
}

type UserDisconnectedInternal struct {
	UserId string
}

type RoomDetailReplyInternal struct {
	Reply *pb.RoomDetail
}

type SetRoomReadMarkerInternal struct {
	UserId     string
	ReadMarker time.Time
}

type InviteRoomMemberInternal struct {
	UserId    string
	MemberIds []string
}

type RemoveRoomMemberInternal struct {
	UserId    string
	MemberIds []string
}

type SyncRoomEventsInternal struct {
	UserId string
	Filter *pb.SyncRoomFilter
}

type SyncRoomEventsReplyInternal struct {
	RoomId        string
	MessageEvents []*pb.RoomEventMessageDetail
	SystemEvents  []*pb.RoomEventSystemDetail
}

type AddMessageInternal struct {
	UserId        string
	ClientEventId int32
	Attachments   []string
	Content       string
	Version       int32
}

type AddMessageReplyInternal struct {
	Reply *pb.RoomEventMessageDetail
}
