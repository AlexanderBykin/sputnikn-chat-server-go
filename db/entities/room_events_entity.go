package entities

type RoomEventsEntity struct {
	RoomId           string
	MessageEvents    []*RoomMessageEventEntity
	AttachmentEvents []*RoomMessageEventAttachmentEntity
	ReactionEvents   []*RoomMessageEventReactionEntity
	SystemEvents     []*RoomSystemEventEntity
}
