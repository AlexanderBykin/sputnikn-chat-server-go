package utils

import (
	"fmt"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"
)

func UuidToString(uuid pgxuuid.UUID) (*string, error) {
	var _uuid, err = uuid.UUIDValue()
	if err != nil {
		return nil, err
	}
	var result = fmt.Sprintf("%x-%x-%x-%x-%x",
		_uuid.Bytes[0:4],
		_uuid.Bytes[4:6],
		_uuid.Bytes[6:8],
		_uuid.Bytes[8:10],
		_uuid.Bytes[10:16])
	return &result, nil
}
