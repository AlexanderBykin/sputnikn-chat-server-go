package db

import (
	"chatserver/db/daos"
	"context"
	"fmt"
	"log"
	"os"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SputnikDB struct {
	dbPool  *pgxpool.Pool
	RoomDao *daos.RoomDao
	UserDao *daos.UserDao
}

func SetupDatabase(dbUrl string) *SputnikDB {
	dbconfig, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		log.Printf("Unable to parse pool config: %v\n", err)
		os.Exit(1)
	}
	dbconfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxuuid.Register(conn.TypeMap())
		return nil
	}
	dbPool, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	return &SputnikDB{
		dbPool:  dbPool,
		RoomDao: daos.NewRoomDao(dbPool),
		UserDao: daos.NewUserDao(dbPool),
	}
}

func (e *SputnikDB) Close() {
	e.dbPool.Close()
}
