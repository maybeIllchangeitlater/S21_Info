Drop table if exists  friends CASCADE;
Drop table if exists  time_tracking CASCADE;
Drop table if exists  recommendations CASCADE;
Drop table if exists  transferred_points CASCADE;
Drop table if exists  p2p CASCADE;
Drop table if exists  xp CASCADE;
Drop table if exists  verter CASCADE;
Drop table if exists  checks CASCADE;
Drop table if exists check_status CASCADE;
Drop table if exists  peers CASCADE;
Drop table if exists  tasks CASCADE;
Drop table if exists global_settings CASCADE;

CREATE TABLE global_settings (
    name text,
    value text
);

INSERT INTO global_settings (name, value)
VALUES ('export', '/Users/susannel/part1/src/export/'), ('import', '/Users/susannel/part1/src/import/');


CREATE TABLE peers (
    nickname varchar(40) PRIMARY KEY NOT NULL,
    birthday date NOT NULL
);

CREATE TABLE tasks (
    title varchar(40) PRIMARY KEY NOT NULL,
    parent_task varchar(40),
    max_xp integer NOT NULL,
    CONSTRAINT fk_tasks_parent FOREIGN KEY (parent_task) REFERENCES tasks(title)
-- To access the task, you must complete the task that is its entry condition.
-- For simplicity, assume that each task has only one entry condition.
-- There must be one task in the table that has no entry condition (i.e., the ParentTask field is null).
);
CREATE INDEX index_parent_taks ON tasks ((1)) WHERE parent_task is NULL; -- 4 1st task

CREATE TYPE check_status AS ENUM('Start', 'Success', 'Failure');

CREATE TABLE checks (
    id bigint primary key NOT NULL,
    peer varchar(40),
    title varchar(40),
    date date,
    CONSTRAINT fk_check_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_check_task FOREIGN KEY (title) REFERENCES tasks(title)
-- Describes the check of the task as a whole. The check necessarily includes a one P2P step and possibly a Verter step.
-- For simplicity, assume that peer to peer and autotests related to the same check always happen on the same day.
-- The check is considered successful if the corresponding P2P step is successful and the Verter step is successful, or if there is no Verter step.
-- The check is considered a failure if at least one of the steps is unsuccessful. This means that checks in which the P2P step has not yet been completed, or it is successful but the Verter step has not yet been completed, are neither successful nor failed.
);
CREATE SEQUENCE seq_check AS bigint START WITH 1 INCREMENT BY 1;
ALTER TABLE checks ALTER COLUMN id SET DEFAULT nextval('seq_check');

CREATE TABLE p2p(
    id bigint primary key NOT NULL,
    check_id bigint,
    checking_peer varchar(40),
    p2p_check_status check_status,
    time time,
    CONSTRAINT fk_p2p_check FOREIGN KEY (check_id) REFERENCES checks(id),
    CONSTRAINT fk_p2p_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers(nickname),
    CONSTRAINT ch_p2p_status CHECK (p2p_check_status IN ('Start', 'Success', 'Failure'))
-- Each P2P check consists of 2 table records: the first has a start status, the second has a success or failure status. 
-- The table cannot contain more than one incomplete P2P check related to a specific task, a peer and a checking peer. 
-- Each P2P check (i.e. both records of which it consists) refers to the check in the Checks table to which it belongs. 
);
CREATE SEQUENCE seq_p2p AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE p2p ALTER COLUMN id SET DEFAULT nextval('seq_p2p');

CREATE TABLE verter (
    id bigint primary key NOT NULL,
    check_id bigint,
    verter_check_status check_status,
    time time,
    CONSTRAINT fk_verter_check FOREIGN KEY (check_id) REFERENCES checks(id),
    CONSTRAINT ch_verter_status CHECK (verter_check_status IN ('Start', 'Success', 'Failure'))
-- Check status
-- Create an enumeration type for the check status that contains the following values:
-- Start - the check starts
-- Success - successful completion of the check
-- Failure - unsuccessful completion of the check
-- Each check by Verter consists of 2 table records: the first has a start status, the second has a success or failure status. 
-- Each check by Verter (i.e. both records of which it consists) refers to the check in the Checks table to which it belongs. 
-- Сheck by Verter can only refer to those checks in the Checks table that already include a successful P2P check.
);
CREATE SEQUENCE seq_verter AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE verter ALTER COLUMN id SET DEFAULT nextval('seq_verter');


CREATE TABLE xp(
    id bigint primary key NOT NULL,
    check_id bigint,
    xp_amount integer,
    CONSTRAINT fk_xp_check FOREIGN KEY (check_id) REFERENCES checks(id)
-- For each successful check, the peer who completes the task receives some amount of XP displayed in this table.
-- The amount of XP cannot exceed the maximum available number for the task being checked.
-- The first field of this table can only refer to successful checks.
);
CREATE SEQUENCE seq_xp AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE xp ALTER COLUMN id SET DEFAULT nextval('seq_xp');

CREATE TABLE transferred_points(
    id bigint primary key NOT NULL,
    checking_peer varchar(40),
    checked_peer varchar(40),
    points_amount integer,
    CONSTRAINT fk_transferred_points_checking_peer FOREIGN KEY (checking_peer) REFERENCES peers(nickname),
    CONSTRAINT fk_transferred_points_checked_peer FOREIGN KEY (checked_peer) REFERENCES peers(nickname),
    CONSTRAINT ch_self_check CHECK (checking_peer NOT LIKE checked_peer)
-- At each P2P check, the peer being checked passes one peer point to the checker.
-- This table contains all pairs of the peer being checked-the checker and the number of transferred peer points, that is the number of P2P checks of the specified peer by the specified checker.
);
CREATE SEQUENCE seq_transferred_points AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE transferred_points ALTER COLUMN id SET DEFAULT nextval('seq_transferred_points');
 

CREATE TABLE friends (
    id bigint primary key NOT NULL,
    peer1 varchar(40),
    peer2 varchar(40),
    CONSTRAINT fk_friends_peer1 FOREIGN KEY (peer1) REFERENCES peers(nickname),
    CONSTRAINT fk_friends_peer2 FOREIGN KEY (peer2) REFERENCES peers(nickname),
    CONSTRAINT ch_self_friends CHECK (peer1 NOT LIKE peer2)
-- Friendship is mutual, i.e. the first peer is a friend of the second one, and vice versa.
-- need to check if p2 == p1 and p1 == p2 before add
);
CREATE SEQUENCE seq_friends AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE friends ALTER COLUMN id SET DEFAULT nextval('seq_friends');

CREATE TABLE recommendations(
    id bigint primary key NOT NULL,
    peer varchar(40),
    recommended_peer varchar(40),
    CONSTRAINT fk_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_recommended_peer FOREIGN KEY (recommended_peer) REFERENCES peers(nickname),
    CONSTRAINT ch_self_recommend CHECK (peer NOT LIKE recommended_peer)
-- Everyone can like how the P2P check was performed by a particular peer. The peer specified in the Peer field recommends passing the P2P check from the peer in the RecommendedPeer field. 
-- Each peer can recommend either one or several checkers at a time.
);
CREATE SEQUENCE seq_recommendations AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE recommendations ALTER COLUMN id SET DEFAULT nextval('seq_recommendations');

CREATE TABLE time_tracking(
    id bigint primary key NOT NULL,
    peer varchar(40),
    date date,
    time time,
    state integer, --(1 - in, 2 - out)
    CONSTRAINT fk_time_tracking_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT ch_time_tracking_status CHECK (state IN(1, 2))
-- This table contains information about peers' visits to campus.
-- When a peer enters campus, a record is added to the table with state 1, when leaving it adds a record with state 2. 
-- In tasks related to this table, the "out" action refers to all but the last Campus departure of the day.
-- There must be the same number of records with state 1 and state 2 for each peer during one day.
);
CREATE SEQUENCE seq_time_tracking AS bigint START WITH 1 INCREMENT by 1;
ALTER TABLE time_tracking ALTER COLUMN id SET DEFAULT nextval('seq_time_tracking');

CREATE OR REPLACE PROCEDURE import_database(delim varchar) AS $$
DECLARE
    path varchar := get_global_setting('import');
    files varchar[] := ARRAY['peers', 'tasks', 'checks', 'p2p', 'verter', 'xp', 'transferred_points', 'friends', 'recommendations', 'time_tracking'];
	file varchar;
BEGIN
    FOREACH file IN ARRAY files
    LOOP
        EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER;', file, path || file || '.csv', $1); -- %I identifier %L litaral path || file || '.csv' - concatenates into full path to csv
        --example: COPY peers FROM 'something/SQL2_Info21_v1.0/src/csv/peers.csv' DELIMITER ',' CSV HEADER; --header means that first row contains header information. nickname, birthday in example
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_database(delim varchar) AS $$
DECLARE
    path varchar := get_global_setting('export');
    file varchar; 
BEGIN -- INFORMATION_SCHEMA provides access to database metadata, information about the MySQL server such as the name of a database or table, the data type of a column, or access privileges. Other terms that are sometimes used for this information are data dictionary and system catalog.
    FOR file IN SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema LIKE 'public'
    LOOP
        EXECUTE format('COPY (SELECT * FROM %I) TO %L DELIMITER %L CSV HEADER;', file, path || file || '.csv', $1);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_global_setting(name text)
RETURNS text LANGUAGE SQL AS $$
    SELECT value FROM global_settings WHERE name = $1;
$$;

TRUNCATE peers, tasks, checks, p2p, verter, xp, transferred_points, friends, recommendations, time_tracking; --before importing

CALL import_database(',');

CALL export_database(',');




























