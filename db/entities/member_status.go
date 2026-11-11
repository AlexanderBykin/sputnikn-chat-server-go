package entities

type MemberStatus int

const (
	MEMBER_STATUS_UNKNOWN MemberStatus = -1
	MEMBER_STATUS_INVITED MemberStatus = 0
	MEMBER_STATUS_JOINED  MemberStatus = 1
	MEMBER_STATUS_LEFT    MemberStatus = 2
	MEMBER_STATUS_KICKED  MemberStatus = 3
	MEMBER_STATUS_BANNED  MemberStatus = 4
)

var (
	MemberStatus_name = map[MemberStatus]string{
		MEMBER_STATUS_INVITED: "MEMBER_STATUS_INVITED",
		MEMBER_STATUS_JOINED:  "MEMBER_STATUS_JOINED",
		MEMBER_STATUS_LEFT:    "MEMBER_STATUS_LEFT",
		MEMBER_STATUS_KICKED:  "MEMBER_STATUS_KICKED",
		MEMBER_STATUS_BANNED:  "MEMBER_STATUS_BANNED",
	}
	MemberStatus_value = map[string]MemberStatus{
		"MEMBER_STATUS_INVITED": MEMBER_STATUS_INVITED,
		"MEMBER_STATUS_JOINED":  MEMBER_STATUS_JOINED,
		"MEMBER_STATUS_LEFT":    MEMBER_STATUS_LEFT,
		"MEMBER_STATUS_KICKED":  MEMBER_STATUS_KICKED,
		"MEMBER_STATUS_BANNED":  MEMBER_STATUS_BANNED,
	}
)

func ParseMemberStatus(value string) MemberStatus {
	if s, ok := MemberStatus_value[value]; ok {
		return s
	} else {
		return MEMBER_STATUS_UNKNOWN
	}
}

func (e MemberStatus) String() string {
	return MemberStatus_name[e]
}
