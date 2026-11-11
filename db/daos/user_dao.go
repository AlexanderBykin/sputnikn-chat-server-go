package daos

import (
	utils "chatserver/utils"
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	entities "chatserver/db/entities"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"
)

type UserDao struct {
	dbPool *pgxpool.Pool
}

func NewUserDao(dbPool *pgxpool.Pool) *UserDao {
	return &UserDao{
		dbPool: dbPool,
	}
}

func (e *UserDao) FindUserByLoginPassword(login string, password string) (*entities.UserEntity, error) {
	query := `SELECT id, login, full_name, avatar, date_create, date_update 
	FROM "user"
	WHERE login = $1 AND password = $2`

	row := e.dbPool.QueryRow(context.Background(), query, login, password)

	var userUuid pgxuuid.UUID
	var userLogin string
	var fullName string
	var avatar *string
	var dateCreate time.Time
	var dateUpdate *time.Time
	err := row.Scan(&userUuid, &userLogin, &fullName, &avatar, &dateCreate, &dateUpdate)

	if err != nil {
		return nil, err
	}

	userUuidStr, err := utils.UuidToString(userUuid)
	if err != nil {
		return nil, err
	}

	result := &entities.UserEntity{
		Id:         *userUuidStr,
		Login:      userLogin,
		FullName:   fullName,
		Avatar:     avatar,
		DateCreate: dateCreate,
		DateUpdate: dateUpdate,
	}

	return result, nil
}

func (e *UserDao) GetAllUsers() ([]*entities.UserEntity, error) {
	query := `SELECT id, login, full_name, avatar, date_create, date_update
	FROM "user"`

	rows, err := e.dbPool.Query(context.Background(), query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*entities.UserEntity, 0)
	for rows.Next() {
		var userUuid pgxuuid.UUID
		var userLogin string
		var fullName string
		var avatar *string
		var dateCreate time.Time
		var dateUpdate *time.Time
		err := rows.Scan(&userUuid, &userLogin, &fullName, &avatar, &dateCreate, &dateUpdate)

		if err != nil {
			return nil, err
		}

		userUuidStr, err := utils.UuidToString(userUuid)
		if err != nil {
			return nil, err
		}

		user := &entities.UserEntity{
			Id:         *userUuidStr,
			Login:      userLogin,
			FullName:   fullName,
			Avatar:     avatar,
			DateCreate: dateCreate,
			DateUpdate: dateUpdate,
		}
		result = append(result, user)
	}

	return result, nil
}
