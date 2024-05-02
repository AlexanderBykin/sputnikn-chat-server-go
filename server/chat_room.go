package server

import (
	pb "chatserver/contract/v1"
	"chatserver/db"
	"chatserver/db/entities"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/samber/lo"
	"github.com/samber/mo"
)

type ChatRoomOnlineUser struct {
	UserId string
	Stream pb.ChatStreamService_RoomEventStreamServer
}

type ChatRoom struct {
	database    *db.SputnikDB
	Id          string
	title       string
	avatar      *string
	members     map[string]*entities.RoomMemberEntity
	InChan      chan *MessageToRoom
	onlineUsers map[string]*ChatRoomOnlineUser
}

func NewRoom(database *db.SputnikDB, roomId string, roomTitle string, roomAvatar *string) *ChatRoom {
	return &ChatRoom{
		database: database,
		Id:       roomId,
		title:    roomTitle,
		avatar:   roomAvatar,
		InChan:   make(chan *MessageToRoom),
	}
}

func (e *ChatRoom) Run() {
	log.Printf("[ChatRoom] started id=%s\n", e.Id)
	e.initRoomMembers()
	for {
		select {
		case inMsg := <-e.InChan:
			switch v := inMsg.Message.(type) {
			case *GetRoomDetailInternal:
				e.onGetRoomDetail(inMsg.OutChan)
			case *SetRoomReadMarkerInternal:
				e.onSetRoomReadMarker(inMsg.OutChan, v)
			case *SyncRoomEventsInternal:
				e.onSyncRoomEvents(inMsg.OutChan, v)
			case *AddMessageInternal:
				e.onAddMessage(inMsg.OutChan, v)
			default:
				inMsg.OutChan <- fmt.Sprintf("unhandled message %T", v)
			}
		}
	}
}

func (e *ChatRoom) initRoomMembers() {
	roomMembers, err := e.database.RoomDao.GetRoomMembers(e.Id)
	if err != nil {
		log.Fatal(err)
	}

	e.members = make(map[string]*entities.RoomMemberEntity)
	for _, item := range roomMembers {
		e.members[item.UserId] = item
	}
}

func (e *ChatRoom) buildRoomDetail() *pb.RoomDetail {
	members := lo.MapToSlice(
		e.members,
		func(key string, value *entities.RoomMemberEntity) *pb.RoomMemberDetail {
			var userOnline bool
			if _, ok := e.onlineUsers[value.UserId]; ok {
				userOnline = ok
			}
			return &pb.RoomMemberDetail{
				UserId:         value.UserId,
				FullName:       value.FullName,
				IsOnline:       userOnline,
				MemberStatus:   pb.RoomMemberStatusType(value.MemberStatus),
				Avatar:         value.Avatar,
				LastReadMarker: value.LastReadMarker.UnixMilli(),
			}
		})
	return &pb.RoomDetail{
		RoomId:                  e.Id,
		Title:                   e.title,
		Avatar:                  e.avatar,
		Members:                 members,
		EventMessageUnreadCount: -1,
		EventSystemUnreadCount:  -1,
	}
}

func (e *ChatRoom) setMemberReadMarker(userId string, readMarker time.Time) error {
	err := e.database.RoomDao.SetMemberReadMarker(e.Id, userId, readMarker)
	if err != nil {
		return err
	}

	if user, ok := e.members[userId]; ok {
		user.LastReadMarker = readMarker
		return nil
	}

	return errors.New("user not found")
}

func (e *ChatRoom) onGetRoomDetail(outChan chan any) {
	result := e.buildRoomDetail()
	outChan <- &RoomDetailReplyInternal{
		Reply: result,
	}
}

func (e *ChatRoom) onSetRoomReadMarker(outChan chan any, req *SetRoomReadMarkerInternal) {
	e.setMemberReadMarker(req.UserId, req.ReadMarker)
	result := e.buildRoomDetail()
	outChan <- &RoomDetailReplyInternal{
		Reply: result,
	}
	e.sendBroadcastMessage(&pb.RoomEventResponse{
		Payload: &pb.RoomEventResponse_RoomStateChanged{
			RoomStateChanged: &pb.RoomStateChangedResponse{
				Detail: result,
			},
		},
	})
}

func (e *ChatRoom) onSyncRoomEvents(outChan chan any, req *SyncRoomEventsInternal) {
	result := &SyncRoomEventsReplyInternal{
		RoomId: e.Id,
	}
	if roomMember, ok := e.members[req.UserId]; ok {
		// if User has been joined to Room then load events from db and return Events
		if roomMember.MemberStatus == entities.MEMBER_STATUS_JOINED {
			var sinceTime time.Time
			var orderType pb.SinceTimeOrderType
			if req.Filter.SinceFilter != nil {
				sinceTime = time.Unix(req.Filter.SinceFilter.SinceTimestamp, 0)
				orderType = req.Filter.SinceFilter.OrderType
			} else {
				sinceTime = time.Unix(0, 0)
				orderType = pb.SinceTimeOrderType_sinceTimeOrderTypeNewest
			}

			roomEvents, err := e.database.RoomDao.GetSyncEvents(e.Id, req.Filter.EventFilter, int(req.Filter.EventLimit), sinceTime, orderType)

			if err == nil {
				defaultDate := time.Unix(0, 0)

				result.MessageEvents = lo.Map[*entities.RoomMessageEventEntity, *pb.RoomEventMessageDetail](
					roomEvents.MessageEvents,
					func(messageEvent *entities.RoomMessageEventEntity, index int) *pb.RoomEventMessageDetail {

						attachments := lo.FilterMap[*entities.RoomMessageEventAttachmentEntity, *pb.ChatAttachmentDetail](
							roomEvents.AttachmentEvents,
							func(attachEvent *entities.RoomMessageEventAttachmentEntity, index int) (*pb.ChatAttachmentDetail, bool) {
								if attachEvent.MessageEventId == messageEvent.Id {
									result := &pb.ChatAttachmentDetail{
										EventId:      messageEvent.Id,
										AttachmentId: attachEvent.Id,
										MimeType:     attachEvent.MimeType,
									}
									return result, true
								}
								return nil, false
							})

						reactions := lo.FilterMap[*entities.RoomMessageEventReactionEntity, *pb.RoomEventReactionDetail](
							roomEvents.ReactionEvents,
							func(reactionEvent *entities.RoomMessageEventReactionEntity, index int) (*pb.RoomEventReactionDetail, bool) {
								if reactionEvent.MessageEventId == messageEvent.Id {
									result := &pb.RoomEventReactionDetail{}
									return result, true
								}
								return nil, false
							})

						clientEventId := int32(messageEvent.ClientEventId)

						return &pb.RoomEventMessageDetail{
							EventId:         messageEvent.Id,
							RoomId:          messageEvent.RoomId,
							SenderId:        messageEvent.UserId,
							ClientEventId:   &clientEventId,
							Version:         int32(messageEvent.Version),
							Content:         messageEvent.Content,
							Attachment:      attachments,
							Reaction:        reactions,
							CreateTimestamp: messageEvent.DateCreate.UnixMilli(),
							UpdateTimestamp: (mo.EmptyableToOption[*time.Time](messageEvent.DateUpdate).OrElse(&defaultDate)).UnixMilli(),
						}
					})

				result.SystemEvents = lo.Map[*entities.RoomSystemEventEntity, *pb.RoomEventSystemDetail](
					roomEvents.SystemEvents,
					func(systemEvent *entities.RoomSystemEventEntity, index int) *pb.RoomEventSystemDetail {
						return &pb.RoomEventSystemDetail{
							EventId:         systemEvent.Id,
							RoomId:          e.Id,
							Version:         int32(systemEvent.Version),
							Content:         systemEvent.Content,
							CreateTimestamp: systemEvent.DateCreate.UnixMilli(),
						}
					})
			}
		}
	}
	outChan <- result
}

func (e *ChatRoom) onAddMessage(outChan chan any, req *AddMessageInternal) {
	//
}

func (e *ChatRoom) sendBroadcastMessage(message *pb.RoomEventResponse) {
	for _, item := range e.onlineUsers {
		item.Stream.Send(message)
	}
}
