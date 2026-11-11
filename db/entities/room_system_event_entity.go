package entities

import "time"

type RoomSystemEventEntity struct {
	Id         string
	RoomId     string
	Version    int
	Content    string
	DateCreate time.Time
}
