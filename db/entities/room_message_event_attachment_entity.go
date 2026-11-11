package entities

import "time"

type RoomMessageEventAttachmentEntity struct {
	Id             string
	RoomId         string
	MessageEventId string
	MimeType       string
	DateCreate     time.Time
}
