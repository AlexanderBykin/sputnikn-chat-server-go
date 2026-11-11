package entities

import "time"

type RoomMessageEventReactionEntity struct {
	Id             string
	RoomId         string
	MessageEventId string
	UserId         string
	Content        string
	DateCreate     time.Time
}
