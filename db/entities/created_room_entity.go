package entities

type CreateRoomEntity struct {
	Room    *RoomEntity
	Members []*RoomMemberEntity
}
