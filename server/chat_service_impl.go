package server

import (
	pb "chatserver/contract/v1"
	"chatserver/db/entities"
	"chatserver/utils"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	db "chatserver/db"

	"github.com/samber/lo"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type ChatService struct {
	pb.UnimplementedChatServiceServer
	tokenManager *JWTManager
	database     *db.SputnikDB
	roomManager  *RoomManager
}

func NewChatService(database *db.SputnikDB, tokenManager *JWTManager, roomManager *RoomManager) *ChatService {
	return &ChatService{
		database:     database,
		tokenManager: tokenManager,
		roomManager:  roomManager,
	}
}

func (e *ChatService) AuthUser(ctx context.Context, req *pb.AuthUserRequest) (*pb.AuthUserResponse, error) {
	foundUser, err := e.database.UserDao.FindUserByLoginPassword(req.Login, req.Password)

	if err != nil || foundUser == nil {
		result := &pb.AuthUserResponse{
			Error:       pb.AuthErrorType_AuthErrorTypeUserWrongCreds,
			AccessToken: nil,
		}
		return result, nil
	}

	tokenString, err := e.tokenManager.CreateToken(foundUser.Id)
	if err != nil {
		result := &pb.AuthUserResponse{
			Error:       pb.AuthErrorType_AuthErrorTypeUserWrongCreds,
			AccessToken: nil,
		}
		return result, nil
	}

	result := &pb.AuthUserResponse{
		Error:       pb.AuthErrorType_AuthErrorTypeNone,
		AccessToken: tokenString,
		Detail: &pb.UserDetail{
			UserId:   foundUser.Id,
			FullName: foundUser.FullName,
			Avatar:   foundUser.Avatar,
		},
	}
	log.Printf(">>> User=%s AuthToken=%s\n", req.Login, *tokenString)
	return result, nil
}

func (e *ChatService) ListRooms(ctx context.Context, req *pb.ListRoomsRequest) (*pb.ListRoomsResponse, error) {
	rooms := e.roomManager.GetRooms(req.RoomIds)
	roomsCount := len(rooms)
	var wg sync.WaitGroup
	roomDetails := make([]*pb.RoomDetail, roomsCount)
	wg.Add(roomsCount)
	utils.MapForEach[string, *ChatRoom](
		rooms,
		func(k string, v *ChatRoom, index int) {
			inChan := v.InChan
			outChan := make(chan any)
			go func(idx int) {
				defer wg.Done()
				inChan <- &MessageToRoom{
					Message: &GetRoomDetailInternal{},
					OutChan: outChan,
				}
				msg := <-outChan
				if result, ok := msg.(*RoomDetailReplyInternal); ok {
					roomDetails[idx] = result.Reply
				}
			}(index)
		})
	wg.Wait()
	resp := &pb.ListRoomsResponse{
		Detail: roomDetails,
	}
	return resp, nil
}

func (e *ChatService) SyncRooms(ctx context.Context, req *pb.SyncRoomsRequest) (*pb.SyncRoomsResponse, error) {
	userId, err := e.getUserIdFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("internal error\n%v", err)
	}

	roomsByUser, err := e.database.RoomDao.GetRoomsByUserId(*userId)
	if err != nil {
		return nil, fmt.Errorf("internal error\n%v", err)
	}

	syncRoomIds := make([]string, 0)

	if len(req.RoomFilter) > 0 {
		for _, roomFilter := range req.RoomFilter {
			if room, ok := roomsByUser[roomFilter.RoomId]; ok {
				syncRoomIds = append(syncRoomIds, room.RoomId)
			}
		}
	} else {
		for roomId := range roomsByUser {
			syncRoomIds = append(syncRoomIds, roomId)
		}
	}

	result := &pb.SyncRoomsResponse{}

	var mutex sync.Mutex

	if len(syncRoomIds) > 0 {
		var wg sync.WaitGroup
		wg.Add(len(syncRoomIds))
		lo.ForEach[string](
			syncRoomIds,
			func(roomId string, idx int) {
				if foundRoom, ok := e.roomManager.GetRoom(roomId).Get(); ok {
					inChan := foundRoom.InChan
					outChan := make(chan any)
					go func(inChannel chan *MessageToRoom, outChannel chan any, roomId string, idx int, mutex *sync.Mutex) {
						defer wg.Done()
						filter := lo.FindOrElse[*pb.SyncRoomFilter](
							req.RoomFilter,
							&pb.SyncRoomFilter{
								RoomId:      roomId,
								SinceFilter: nil,
								EventFilter: pb.RoomEventType_roomEventTypeAll,
							},
							func(it *pb.SyncRoomFilter) bool {
								return it.RoomId == roomId
							})
						inChannel <- &MessageToRoom{
							Message: &SyncRoomEventsInternal{
								UserId: *userId,
								Filter: filter,
							},
							OutChan: outChannel,
						}
						msg := <-outChannel
						if inMsg, ok := msg.(*SyncRoomEventsReplyInternal); ok {
							if len(inMsg.MessageEvents) > 0 || len(inMsg.SystemEvents) > 0 {
								mutex.Lock()
								result.MessageEvents = append(result.MessageEvents, inMsg.MessageEvents...)
								result.SystemEvents = append(result.SystemEvents, inMsg.SystemEvents...)
								mutex.Unlock()
							}
						}
					}(inChan, outChan, roomId, idx, &mutex)
				} else {
					wg.Done()
				}
			})
		wg.Wait()
	}

	return result, nil
}

func (e *ChatService) ListUsers(ctx context.Context, req *pb.ListUsersRequest) (*pb.ListUsersResponse, error) {
	users, err := e.database.UserDao.GetAllUsers()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	userDetails := lo.Map[*entities.UserEntity, *pb.UserDetail](
		users,
		func(item *entities.UserEntity, idx int) *pb.UserDetail {
			return &pb.UserDetail{
				UserId:   item.Id,
				FullName: item.FullName,
				Avatar:   item.Avatar,
			}
		})

	result := &pb.ListUsersResponse{
		Users: userDetails,
	}

	return result, nil
}

func (e *ChatService) SetRoomReadMarker(ctx context.Context, req *pb.RoomReadMarkerRequest) (*pb.RoomStateChangedResponse, error) {
	foundRoom, ok := e.roomManager.GetRoom(req.RoomId).Get()
	if !ok {
		return nil, status.Error(codes.NotFound, "room not found")
	}
	userId, err := e.getUserIdFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	outChan := make(chan any)
	foundRoom.InChan <- &MessageToRoom{
		Message: &SetRoomReadMarkerInternal{
			UserId:     *userId,
			ReadMarker: time.UnixMilli(req.ReadMarkerTimestamp),
		},
	}
	msg := <-outChan
	if reply, ok := msg.(*RoomDetailReplyInternal); ok {
		result := &pb.RoomStateChangedResponse{
			Detail: reply.Reply,
		}
		return result, nil
	}
	return nil, status.Error(codes.Internal, "can't set read marker")
}

func (e *ChatService) CreateRoom(ctx context.Context, req *pb.CreateRoomRequest) (*pb.CreateRoomResponse, error) {
	userId, err := e.getUserIdFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	memberIds := lo.Uniq(req.MemberIds)
	if len(memberIds) < 2 {
		return nil, status.Error(codes.Internal, "can't create room because min unique users 2")
	}

	newRoom, err := e.database.RoomDao.AddRoom(req.Title, req.Avatar, *userId, memberIds)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = e.roomManager.StartRoom(newRoom.Room)
	if err != nil {
		return nil, err
	}

	roomMembers := lo.Map(newRoom.Members,
		func(item *entities.RoomMemberEntity, index int) *pb.RoomMemberDetail {
			return &pb.RoomMemberDetail{
				UserId:         item.UserId,
				FullName:       item.FullName,
				IsOnline:       false,
				MemberStatus:   pb.RoomMemberStatusType(item.MemberStatus),
				Avatar:         item.Avatar,
				LastReadMarker: item.LastReadMarker.UnixMilli(),
			}
		})

	roomDetail := &pb.RoomDetail{
		RoomId:                  newRoom.Room.RoomId,
		Title:                   newRoom.Room.Title,
		Avatar:                  newRoom.Room.Avatar,
		Members:                 roomMembers,
		EventMessageUnreadCount: 0,
		EventSystemUnreadCount:  0,
	}

	result := &pb.CreateRoomResponse{
		Detail: roomDetail,
	}

	return result, nil
}

func (e *ChatService) InviteRoomMember(ctx context.Context, req *pb.EmptyRequest) (*pb.RoomStateChangedResponse, error) {
	return nil, status.Error(codes.NotFound, "method not implemented")
}

func (e *ChatService) RemoveRoomMember(ctx context.Context, req *pb.EmptyRequest) (*pb.RoomStateChangedResponse, error) {
	return nil, status.Error(codes.NotFound, "method not implemented")
}

func (e *ChatService) AddRoomMessage(ctx context.Context, req *pb.RoomEventMessageRequest) (*pb.RoomEventMessageResponse, error) {
	userId, err := e.getUserIdFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	foundRoom, ok := e.roomManager.GetRoom(req.RoomId).Get()
	if !ok {
		return nil, status.Error(codes.NotFound, "room not found")
	}

	outChan := make(chan any)
	foundRoom.InChan <- &MessageToRoom{
		Message: &AddMessageInternal{
			UserId:        *userId,
			ClientEventId: req.ClientEventId,
			Attachment:    req.Attachment,
			Content:       req.Content,
			Version:       req.Version,
		},
		OutChan: outChan,
	}
	msg := <-outChan
	if reply, ok := msg.(*AddMessageReplyInternal); ok {
		result := &pb.RoomEventMessageResponse{
			Detail: reply.Reply,
		}
		return result, nil
	}

	return nil, status.Error(codes.NotFound, "method not implemented")
}

func (e *ChatService) getUserIdFromContext(ctx context.Context) (*string, error) {
	accessToken, err := utils.GetAccessTokenFromContext(ctx)
	if err != nil {
		return nil, err
	}

	userClaims, err := e.tokenManager.VerifyToken(*accessToken)
	if err != nil {
		return nil, err
	}

	return &userClaims.UserId, nil
}
