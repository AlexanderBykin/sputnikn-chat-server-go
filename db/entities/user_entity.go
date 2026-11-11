package entities

import "time"

type UserEntity struct {
	Id         string
	Login      string
	FullName   string
	Avatar     *string
	DateCreate time.Time
	DateUpdate *time.Time
}
