DO $$
DECLARE
    roomId UUID;
BEGIN

    /*
    INSERT INTO "user"(login, password, full_name)
    SELECT 'testuser'||i::varchar, i::varchar, 'test user '||i::varchar
    FROM generate_series(1, 30) i;

    INSERT INTO room(title) VALUES('test room') RETURNING id INTO roomId;

    INSERT INTO room_member(room_id, user_id, member_status, permission)
    SELECT roomId, id, 'MEMBER_STATUS_INVITED', 0
    FROM "user" WHERE login in ('testuser1', 'testuser2');
    */

END $$;