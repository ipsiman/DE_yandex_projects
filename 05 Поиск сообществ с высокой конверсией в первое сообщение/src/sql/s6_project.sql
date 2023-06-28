-- СОЗДАДИМ ТАБЛИЦУ group_log в Vertica
-- group_log.csv  group_id, user_id, user_id_from, event, datetime

-- TRUNCATE TABLE DEZIXYANDEXRU__STAGING.group_log;
-- DROP TABLE IF EXISTS DEZIXYANDEXRU__STAGING.group_log;
CREATE TABLE IF NOT EXISTS DEZIXYANDEXRU__STAGING.group_log
(
	group_id int REFERENCES DEZIXYANDEXRU__STAGING.groups(id) NOT NULL,
	user_id int REFERENCES DEZIXYANDEXRU__STAGING.users(id) NOT NULL,
	user_id_from int,
	event varchar(20),
	date_ts datetime
)
ORDER BY group_id;


-- СОЗДАДИМ ТАБЛИЦУ СВЯЗИ l_user_group_activity

-- TRUNCATE TABLE DEZIXYANDEXRU__DWH.l_user_group_activity;
-- DROP TABLE IF EXISTS DEZIXYANDEXRU__DWH.l_user_group_activity;
CREATE TABLE DEZIXYANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity bigint PRIMARY KEY,
hk_user_id bigint NOT NULL
  CONSTRAINT fk_l_user_group_activity_h_users REFERENCES DEZIXYANDEXRU__DWH.h_users (hk_user_id),
hk_group_id bigint NOT NULL
  CONSTRAINT fk_l_groups_dialogs_h_groups REFERENCES DEZIXYANDEXRU__DWH.h_groups (hk_group_id),
load_dt datetime,
load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- НАПОЛНИМ ТАБЛИЦУ СВЯЗИ l_user_group_activity
INSERT INTO DEZIXYANDEXRU__DWH.l_user_group_activity
(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT hash(u.hk_user_id, g.hk_group_id),
	   u.hk_user_id,
	   g.hk_group_id,
	   now() AS load_dt,
	   's3' AS load_src
FROM DEZIXYANDEXRU__STAGING.group_log AS gl
LEFT JOIN DEZIXYANDEXRU__DWH.h_users AS u ON gl.user_id = u.user_id
LEFT JOIN DEZIXYANDEXRU__DWH.h_groups AS g ON gl.group_id = g.group_id
WHERE hash(u.hk_user_id, g.hk_group_id) NOT IN
	(SELECT hk_l_user_group_activity FROM DEZIXYANDEXRU__DWH.l_user_group_activity);


-- СОЗДАДИМ И НАПОЛНИМ САТЕЛЛИТ s_auth_history
-- TRUNCATE TABLE DEZIXYANDEXRU__DWH.s_auth_history;
-- DROP TABLE IF EXISTS DEZIXYANDEXRU__DWH.s_auth_history;
CREATE TABLE DEZIXYANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint NOT NULL
  CONSTRAINT fk_s_auth_history_l_user_group_activity
  REFERENCES DEZIXYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(20),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO DEZIXYANDEXRU__DWH.s_auth_history
    (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT luga.hk_l_user_group_activity,
	   gl.user_id_from,
	   gl.event,
	   gl.datetime AS event_dt,
	   now() AS load_dt,
	   's3' AS load_src
FROM DEZIXYANDEXRU__STAGING.group_log AS gl
LEFT JOIN DEZIXYANDEXRU__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN DEZIXYANDEXRU__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN DEZIXYANDEXRU__DWH.l_user_group_activity AS luga ON
	hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id;


-- РАССЧИТАЕМ КОНВЕРСИОННЫЕ ПОКАЗАТЕЛИ ДЛЯ ДЕСЯТИ САМЫХ СТАРЫХ ГРУПП

WITH user_group_messages AS (
	SELECT lgd.hk_group_id,
		   count(DISTINCT lum.hk_user_id) cnt_users_in_group_with_messages
	FROM DEZIXYANDEXRU__DWH.l_groups_dialogs lgd
	LEFT JOIN l_user_message lum ON lgd.hk_message_id = lum.hk_message_id 
	GROUP BY 1
),
user_group_log AS (
	SELECT luga.hk_group_id,
		   COUNT(DISTINCT luga.hk_user_id) cnt_added_users
	FROM l_user_group_activity luga
	LEFT JOIN s_auth_history sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
	LEFT JOIN h_groups hg ON luga.hk_group_id=hg.hk_group_id
	WHERE sah.event = 'add'
	GROUP BY luga.hk_group_id, hg.registration_dt
	ORDER BY hg.registration_dt
	LIMIT 10
)
SELECT ugl.hk_group_id,
	   ugl.cnt_added_users,
	   ugm.cnt_users_in_group_with_messages,
	   (ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users)*100 AS group_conversion
FROM user_group_log ugl
JOIN user_group_messages ugm ON ugl.hk_group_id=ugm.hk_group_id
ORDER BY group_conversion DESC;
