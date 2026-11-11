package entities

type RoomMemberUnreadEntity struct {
	MemberId           string
	EventMessageUnread int
	EventSystemUnread  int
}
