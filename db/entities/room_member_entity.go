package entities

import "time"

type RoomMemberEntity struct {
	UserId         string
	FullName       string
	MemberStatus   MemberStatus
	Avatar         *string
	LastReadMarker time.Time
}
