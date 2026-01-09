/*
DROP TABLE IF EXISTS public.user_push;
DROP TABLE IF EXISTS public.room_member;
DROP TABLE IF EXISTS public.room_event_system;
DROP TABLE IF EXISTS public.room_event_message_reaction;
DROP TABLE IF EXISTS public.room_event_message_attachment;
DROP TABLE IF EXISTS public.room_event_message;
DROP TABLE IF EXISTS public.chat_attachment;
DROP TABLE IF EXISTS public."user";
DROP TABLE IF EXISTS public.room;
DROP TYPE IF EXISTS public.member_status;
*/

CREATE TABLE IF NOT EXISTS public."user"
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    login varchar(255) NOT NULL,
    password varchar(255) NOT NULL,
    full_name varchar(255) NOT NULL,
    avatar varchar(255) ,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    date_update timestamp without time zone,
    PRIMARY KEY (id),
    CONSTRAINT ux_user_login UNIQUE (login)
);

CREATE TABLE IF NOT EXISTS public.user_push
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    device_name varchar(255) NOT NULL,
    token varchar(4096) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT ux_user_push UNIQUE (user_id, device_name),
    CONSTRAINT fk_user_push_user FOREIGN KEY (user_id)
        REFERENCES public."user" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

CREATE TABLE IF NOT EXISTS public.chat_attachment
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL,
    mime_type varchar(255) NOT NULL,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT fk_chat_attachment_user FOREIGN KEY (user_id)
        REFERENCES public."user" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

CREATE TABLE IF NOT EXISTS public.room
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    title varchar(255) NOT NULL,
    avatar varchar(255),
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    date_update timestamp without time zone,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.room_event_message
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    room_id uuid NOT NULL,
    user_id uuid NOT NULL,
    version smallint NOT NULL,
    content text NOT NULL,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    date_edit timestamp without time zone,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.room_event_message_attachment
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    room_event_message_id uuid NOT NULL,
    chat_attachment_id uuid NOT NULL,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT ux_room_event_message_attachment UNIQUE (room_event_message_id, chat_attachment_id),
    CONSTRAINT fk_room_event_message_attachment_event FOREIGN KEY (room_event_message_id)
            REFERENCES public.room_event_message (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID,
    CONSTRAINT fk_room_event_message_attachment_attachment FOREIGN KEY (chat_attachment_id)
            REFERENCES public.chat_attachment (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID
);

CREATE TABLE IF NOT EXISTS public.room_event_message_reaction
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    room_event_message_id uuid NOT NULL,
    user_id uuid NOT NULL,
    content varchar(30) NOT NULL,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT ux_room_event_message_reaction UNIQUE (room_event_message_id, user_id),
    CONSTRAINT fk_room_event_message_reaction_event FOREIGN KEY (room_event_message_id)
                REFERENCES public.room_event_message (id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
                NOT VALID,
        CONSTRAINT fk_room_event_message_reaction_user FOREIGN KEY (user_id)
                REFERENCES public."user" (id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
                NOT VALID
);

CREATE TABLE IF NOT EXISTS public.room_event_system
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    room_id uuid NOT NULL,
    version smallint NOT NULL,
    content text NOT NULL,
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TYPE public.member_status AS ENUM (
    'MEMBER_STATUS_INVITED',
    'MEMBER_STATUS_JOINED',
    'MEMBER_STATUS_LEFT',
    'MEMBER_STATUS_KICKED',
    'MEMBER_STATUS_BANNED'
    );

CREATE TABLE IF NOT EXISTS public.room_member
(
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    room_id uuid NOT NULL,
    user_id uuid NOT NULL,
    member_status member_status NOT NULL,
    permission smallint NOT NULL,
    last_read_marker timestamp without time zone NOT NULL DEFAULT now(),
    date_create timestamp without time zone NOT NULL DEFAULT now(),
    date_update timestamp without time zone,
    PRIMARY KEY (id),
    CONSTRAINT iu_room_member UNIQUE (room_id, user_id),
    CONSTRAINT fk_room_member_room FOREIGN KEY (room_id)
        REFERENCES public.room (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT fk_room_member_user FOREIGN KEY (user_id)
        REFERENCES public."user" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);
