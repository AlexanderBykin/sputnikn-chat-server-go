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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ChatRoom struct {
	SubscriberManagerListener
	database          *db.SputnikDB
	Id                string
	title             string
	avatar            *string
	members           map[string]*entities.RoomMemberEntity
	InChan            chan *MessageToRoom
	subscriberManager *SubscriberManager
}

func NewRoom(database *db.SputnikDB, subscriberManager *SubscriberManager, roomId string, roomTitle string, roomAvatar *string) *ChatRoom {
	return &ChatRoom{
		database:          database,
		subscriberManager: subscriberManager,
		Id:                roomId,
		title:             roomTitle,
		avatar:            roomAvatar,
		InChan:            make(chan *MessageToRoom),
	}
}

func (e *ChatRoom) Run() {
	log.Printf("[ChatRoom] started id=%s\n", e.Id)
	e.updateRoomMembers()
	e.subscriberManager.addListener(e)
	for {
		select {
		case inMsg := <-e.InChan:
			switch v := inMsg.Message.(type) {
			case *GetRoomDetailInternal:
				e.onGetRoomDetail(*inMsg.OutChan)
			case *SetRoomReadMarkerInternal:
				e.onSetRoomReadMarker(*inMsg.OutChan, v)
			case *InviteRoomMemberInternal:
				e.onInviteRoomMember(*inMsg.OutChan, v)
			case *RemoveRoomMemberInternal:
				e.onRemoveRoomMember(*inMsg.OutChan, v)
			case *SyncRoomEventsInternal:
				e.onSyncRoomEvents(*inMsg.OutChan, v)
			case *AddMessageInternal:
				e.onAddMessage(*inMsg.OutChan, v)
			case *UserConnectedInternal:
				e.onUserConnectedOrDisconnected(v.UserId)
			case *UserDisconnectedInternal:
				e.onUserConnectedOrDisconnected(v.UserId)
			default:
				if inMsg.OutChan != nil {
					*inMsg.OutChan <- fmt.Sprintf("unhandled message %T", v)
				}
			}
		}
	}
}

func (e *ChatRoom) onConnectedSubscriber(userId string) {
	e.InChan <- &MessageToRoom{
		Message: UserConnectedInternal{UserId: userId},
		OutChan: nil,
	}
}

func (e *ChatRoom) onDisconnectedSubscriber(userId string) {
	e.InChan <- &MessageToRoom{
		Message: UserDisconnectedInternal{UserId: userId},
		OutChan: nil,
	}
}

func (e *ChatRoom) updateRoomMembers() {
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
			var userOnline = false
			if _, ok := e.subscriberManager.subscribers[value.UserId]; ok {
				userOnline = ok
			}
			return &pb.RoomMemberDetail{
				UserId:         value.UserId,
				FullName:       value.FullName,
				IsOnline:       userOnline,
				MemberStatus:   pb.RoomMemberStatusType(value.MemberStatus),
				Avatar:         value.Avatar,
				LastReadMarker: timestamppb.New(value.LastReadMarker),
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
			RoomStateChanged: result,
		},
	})
}

func (e *ChatRoom) onInviteRoomMember(outChan chan any, req *InviteRoomMemberInternal) {
	members := lo.MapToSlice(e.members, func(_ string, member *entities.RoomMemberEntity) *entities.RoomMemberEntity {
		return member
	})
	absentUserIds := lo.FilterMap(members, func(member *entities.RoomMemberEntity, index int) (string, bool) {
		return member.UserId, lo.Contains(req.MemberIds, member.UserId)
	})
	e.database.RoomDao.AddRoomMembers(e.Id, absentUserIds)
	// TODO: find online users and notify them

	e.updateRoomMembers()
	result := e.buildRoomDetail()
	outChan <- &RoomDetailReplyInternal{
		Reply: result,
	}
	e.sendBroadcastMessage(&pb.RoomEventResponse{
		Payload: &pb.RoomEventResponse_RoomStateChanged{
			RoomStateChanged: result,
		},
	})
}

func (e *ChatRoom) onRemoveRoomMember(outChan chan any, req *RemoveRoomMemberInternal) {
	// TODO: what should we do at DB?
	members := lo.MapToSlice(e.members, func(_ string, member *entities.RoomMemberEntity) *entities.RoomMemberEntity {
		return member
	})
	absentUserIds := lo.FilterMap(members, func(member *entities.RoomMemberEntity, index int) (string, bool) {
		return member.UserId, lo.Contains(req.MemberIds, member.UserId)
	})
	e.database.RoomDao.KickRoomMembers(e.Id, absentUserIds)
	// TODO: find online users and notify them

	e.updateRoomMembers()
	result := e.buildRoomDetail()
	outChan <- &RoomDetailReplyInternal{
		Reply: result,
	}
	e.sendBroadcastMessage(&pb.RoomEventResponse{
		Payload: &pb.RoomEventResponse_RoomStateChanged{
			RoomStateChanged: result,
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
				sinceTime = req.Filter.SinceFilter.Since.AsTime()
				orderType = req.Filter.SinceFilter.OrderType
			} else {
				sinceTime = time.Unix(0, 0)
				orderType = pb.SinceTimeOrderType_sinceTimeOrderTypeNewest
			}

			roomEvents, err := e.database.RoomDao.GetSyncEvents(e.Id, req.Filter.EventFilter, int(req.Filter.EventLimit), sinceTime, orderType)

			if err == nil {
				defaultDate := time.Unix(0, 0)

				result.MessageEvents = lo.Map(
					roomEvents.MessageEvents,
					func(messageEvent *entities.RoomMessageEventEntity, index int) *pb.RoomEventMessageDetail {

						attachments := lo.FilterMap(
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

						reactions := lo.FilterMap(
							roomEvents.ReactionEvents,
							func(reactionEvent *entities.RoomMessageEventReactionEntity, index int) (*pb.RoomEventReactionDetail, bool) {
								var result *pb.RoomEventReactionDetail
								if reactionEvent.MessageEventId == messageEvent.Id {
									result = &pb.RoomEventReactionDetail{}
								}
								return result, result != nil
							})

						return &pb.RoomEventMessageDetail{
							EventId:    messageEvent.Id,
							RoomId:     messageEvent.RoomId,
							SenderId:   messageEvent.UserId,
							Version:    int32(messageEvent.Version),
							Content:    messageEvent.Content,
							Attachment: attachments,
							Reaction:   reactions,
							CreatedAt:  timestamppb.New(messageEvent.DateCreate),
							UpdatedAt:  timestamppb.New(*mo.EmptyableToOption(messageEvent.DateUpdate).OrElse(&defaultDate)),
						}
					})

				result.SystemEvents = lo.Map(
					roomEvents.SystemEvents,
					func(systemEvent *entities.RoomSystemEventEntity, index int) *pb.RoomEventSystemDetail {
						return &pb.RoomEventSystemDetail{
							EventId:   systemEvent.Id,
							RoomId:    e.Id,
							Version:   int32(systemEvent.Version),
							Content:   systemEvent.Content,
							CreatedAt: timestamppb.New(systemEvent.DateCreate),
						}
					})
			}
		}
	}
	outChan <- result
}

func (e *ChatRoom) onAddMessage(outChan chan any, req *AddMessageInternal) {
	if _, ok := e.subscriberManager.subscribers[req.UserId]; ok {
		roomMessage, err := e.database.RoomDao.AddRoomMessage(e.Id, req.UserId, int(req.Version), req.Content)
		if err == nil {
			defaultDate := time.Unix(0, 0)
			messageDetail := &pb.RoomEventMessageDetail{
				EventId:   roomMessage.Id,
				RoomId:    roomMessage.RoomId,
				SenderId:  roomMessage.UserId,
				Version:   int32(roomMessage.Version),
				Content:   roomMessage.Content,
				CreatedAt: timestamppb.New(roomMessage.DateCreate),
				UpdatedAt: timestamppb.New(*mo.EmptyableToOption(roomMessage.DateUpdate).OrElse(&defaultDate)),
			}
			outChan <- &AddMessageReplyInternal{
				Reply: messageDetail,
			}
			for memberUserId := range e.members {
				if subscriber, ok := e.subscriberManager.subscribers[memberUserId]; ok {
					if memberUserId == req.UserId {
						continue
					}
					subscriber.Send(&pb.RoomEventResponse{
						Payload: &pb.RoomEventResponse_MessageEvent{
							MessageEvent: messageDetail,
						},
					})
				}
			}
		}
	}
}

func (e *ChatRoom) onUserConnectedOrDisconnected(userId string) {
	if _, ok := e.members[userId]; ok {
		result := e.buildRoomDetail()
		e.sendBroadcastMessage(&pb.RoomEventResponse{
			Payload: &pb.RoomEventResponse_RoomStateChanged{
				RoomStateChanged: result,
			},
		})
	}
}

func (e *ChatRoom) sendBroadcastMessage(message *pb.RoomEventResponse) {
	for memberUserId := range e.members {
		if subscriber, ok := e.subscriberManager.subscribers[memberUserId]; ok {
			subscriber.Send(message)
		}
	}
}
