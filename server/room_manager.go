package server

import (
	"fmt"
	"log"

	db "chatserver/db"
	"chatserver/db/entities"

	"github.com/samber/mo"
)

type RoomManager struct {
	database          *db.SputnikDB
	subscriberManager *SubscriberManager
	rooms             map[string]*ChatRoom
}

func NewRoomManager(database *db.SputnikDB, subscriberManager *SubscriberManager) *RoomManager {
	return &RoomManager{
		database:          database,
		subscriberManager: subscriberManager,
		rooms:             make(map[string]*ChatRoom),
	}
}

func (e *RoomManager) GetRooms(roomIds []string) map[string]*ChatRoom {
	result := make(map[string]*ChatRoom)
	for k, v := range e.rooms {
		if len(roomIds) == 0 {
			result[k] = v
		} else {
			for _, roomId := range roomIds {
				if k == roomId {
					result[k] = v
				}
			}
		}
	}
	return result
}

func (e *RoomManager) GetRoom(roomId string) mo.Option[*ChatRoom] {
	var result *ChatRoom
	if room, ok := e.rooms[roomId]; ok {
		result = room
	}
	return mo.PointerToOption(&result)
}

func (e *RoomManager) StartRoom(room *entities.RoomEntity) error {
	if _, roomExists := e.rooms[room.RoomId]; roomExists {
		return fmt.Errorf("room(%v) already started", room.RoomId)
	}
	createdRoom := NewRoom(e.database, e.subscriberManager, room.RoomId, room.Title, room.Avatar)
	e.rooms[room.RoomId] = createdRoom
	go createdRoom.Run()
	return nil
}

func (e *RoomManager) Start() {
	rooms, err := e.database.RoomDao.GetRooms()
	if err != nil {
		log.Fatal(err)
	}

	for _, room := range rooms {
		err := e.StartRoom(room)
		if err != nil {
			log.Println(err)
		}
	}
}
