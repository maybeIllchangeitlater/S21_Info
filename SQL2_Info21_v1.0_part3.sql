/**
 * 1) Написать функцию, возвращающую таблицу TransferredPoints
 * в более человекочитаемом виде
 * Ник пира 1, ник пира 2, количество переданных пир поинтов. 
 * Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.
 */
CREATE OR REPLACE FUNCTION transfered_points_saldo()
RETURNS TABLE (peer1 varchar(40), peer2 varchar(40), transferred_points integer) AS $$
WITH cte1 AS (
	SELECT checking_peer AS peer1,			-- В прямом порядке берётся часть таблицы,
			checked_peer AS peer2,
			points_amount					
	FROM transferred_points
		WHERE checking_peer < checked_peer), -- где проверяющий идёт раньше по алфавиту
cte2 AS (
	SELECT checking_peer AS peer2,				-- В обратном порядке берётся часть таблицы,
			checked_peer AS peer1,
			points_amount * -1 AS points_amount
	FROM transferred_points
		WHERE checking_peer >= checked_peer), -- где проверяющий идёт позже по алфавиту
cte3 AS (
	SELECT  peer1, peer2, points_amount FROM cte1
	UNION ALL								-- объединяются две части
	SELECT  peer1, peer2, points_amount FROM cte2)
SELECT peer1, peer2, sum(points_amount) AS points_amount -- суммируются две части
FROM cte3
GROUP BY peer1, peer2 ORDER BY peer1, peer2;
$$ LANGUAGE SQL;

-- SELECT * FROM transfered_points_saldo();

/**
 * 2) Написать функцию, которая возвращает таблицу вида: ник пользователя,
 * название проверенного задания, кол-во полученного XP
 */
CREATE OR REPLACE FUNCTION peer_task_xp()
RETURNS TABLE (peer varchar(40), task varchar(40), xp integer) AS $$
WITH successful_checks AS (
	SELECT checks.id, checks.peer, checks.title  FROM checks 
	JOIN p2p ON checks.id = p2p.check_id
	LEFT JOIN verter ON checks.id = verter.check_id
	WHERE p2p.p2p_check_status = 'Success'
		AND (verter.verter_check_status = 'Success' OR verter.verter_check_status IS NULL))
SELECT peer, title AS task, xp_amount AS xp FROM successful_checks
JOIN xp ON successful_checks.id = xp.check_id
ORDER BY 1, 2;
$$ LANGUAGE SQL;

--SELECT * FROM peer_task_xp();

/**
 * 3) Написать функцию, определяющую пиров,
 * которые не выходили из кампуса в течение всего дня
 */
CREATE OR REPLACE FUNCTION peers_not_left_campus("day" date)
RETURNS TABLE (peer varchar(40)) AS $$
WITH num_of_leavings AS (			-- сколько раз за день отметка о выходе
	SELECT peer, count(state) AS num FROM time_tracking
	WHERE date = "day" AND state = 2
	GROUP BY peer)
SELECT peer FROM num_of_leavings
WHERE num = 1;	-- если всего 1 отметка, значит пришёл и ушёл, а в промежутке не выходил
$$ LANGUAGE SQL;

--SELECT * FROM peers_not_left_campus('2023-03-22');

/**
 * 4) Посчитать изменение в количестве пир поинтов каждого пира
 * по таблице TransferredPoints
 */
--DROP PROCEDURE IF EXISTS show_change_of_peerpoints(rc refcursor);

CREATE OR REPLACE PROCEDURE show_change_of_peerpoints (rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	WITH cte AS (
		SELECT checking_peer AS peer, points_amount		-- поинты проверяющих
		FROM transferred_points
		UNION ALL							-- объединяются с поинтами проверяемых
		SELECT checked_peer AS peer, points_amount * -1 -- с обратным знаком
		FROM transferred_points)
	SELECT peer, sum(points_amount) AS PointsChange -- сумма
	FROM cte
	GROUP BY peer ORDER BY 2;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL show_change_of_peerpoints('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице,
 * возвращаемой первой функцией из Part 3
 */
--DROP PROCEDURE IF EXISTS show_change_of_peerpoints_2(rc refcursor);

CREATE OR REPLACE PROCEDURE show_change_of_peerpoints_2 (rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	WITH cte AS (
		SELECT peer1 AS peer, transferred_points -- поинты пиров раньше по алфавиту
		FROM transfered_points_saldo() AS tp1
		UNION ALL								-- объединяются с
		SELECT peer2 AS peer, transferred_points * -1 -- поинтами пиров позже по алфавиту
		FROM transfered_points_saldo() AS tp2
		)
	SELECT peer, sum(transferred_points) AS PointsChange -- сумма
	FROM cte
	GROUP BY peer ORDER BY 2;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL show_change_of_peerpoints_2('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 6) Определить самое часто проверяемое задание за каждый день
 */
--DROP PROCEDURE IF EXISTS show_most_frequently_checked(rc refcursor);

CREATE OR REPLACE PROCEDURE show_most_frequently_checked (rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	WITH cte1 AS (
		SELECT "date", title, count(title) AS num
		FROM checks
		GROUP BY "date", title),
	cte2 AS (
		SELECT "date", max(num) AS num
		FROM cte1
		GROUP BY "date")
	SELECT cte1."date" AS "Day", title AS Task FROM cte1
	JOIN cte2 ON cte1."date" = cte2."date" AND cte1.num = cte2.num;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL show_most_frequently_checked('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 7) Найти всех пиров, выполнивших весь заданный блок задач
 * и дату завершения последнего задания
 */
--DROP PROCEDURE IF EXISTS show_peer_completed_block(rc refcursor, block varchar(10));

CREATE OR REPLACE PROCEDURE show_peer_completed_block(rc refcursor, block varchar(10)) AS $$
BEGIN
	OPEN rc FOR
	WITH last_in_block AS (			-- Определяется последнее задание в блоке
		SELECT max(title) AS title
		FROM tasks
		WHERE title ~* (block || '[0-9]')),
	successful_checks AS (			-- Создаётся список успешных проверок
		SELECT checks.peer, checks.title, checks."date" FROM checks 
		JOIN p2p ON checks.id = p2p.check_id
		LEFT JOIN verter ON checks.id = verter.check_id
		WHERE p2p.p2p_check_status = 'Success'
			AND (verter.verter_check_status = 'Success'
				OR verter.verter_check_status IS NULL)),
	successful_last_checks AS ( -- Из успешных проверок выбираются проверки последних заданий в блоке
		SELECT * FROM successful_checks
		JOIN last_in_block ON last_in_block.title = successful_checks.title)
	SELECT DISTINCT peer, min("date") AS "day"
	FROM successful_last_checks
	GROUP BY peer ORDER BY 2 DESC;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL show_peer_completed_block('rc', 'C');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 8) Определить исходя из рекомендаций друзей пира,
 * к какому пиру стоит идти на проверку каждому обучающемуся
 */
--DROP PROCEDURE IF EXISTS choose_peer_to_be_checked(rc refcursor);

CREATE OR REPLACE PROCEDURE choose_peer_to_be_checked (rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	WITH recomended_count AS (
		SELECT double_friends.peer,
				recommended_peer, -- подставляет вместо друга рекомендованного пира
				count(recommended_peer) AS num FROM -- считает рекомендации
			(SELECT peer1 AS peer, peer2 AS friend -- задваивает таблицу друзья (первый - пир, второй - его друг)
			FROM friends
			UNION
			SELECT peer2, peer1
			FROM friends) AS double_friends
		JOIN recommendations ON recommendations.peer = double_friends.friend
		WHERE double_friends.peer != recommended_peer
		GROUP BY double_friends.peer, recommended_peer)
	SELECT DISTINCT recomended_count.peer, recomended_count.recommended_peer FROM
		(SELECT peer, max(num) AS max_num FROM recomended_count -- выбирает, кого чаще рекомендуют
		GROUP BY peer) AS rc1
		JOIN recomended_count ON rc1.max_num = recomended_count.num
	ORDER BY 1,2;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL choose_peer_to_be_checked('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 9) Определить процент пиров, которые:
 * Приступили только к блоку 1
 * Приступили только к блоку 2
 * Приступили к обоим
 * Не приступили ни к одному
 */
--DROP PROCEDURE IF EXISTS find_peers_started_blocks(rc refcursor, block1 varchar(10), block2 varchar(10));

CREATE OR REPLACE PROCEDURE find_peers_started_blocks(rc refcursor, block1 varchar(10), block2 varchar(10)) AS $$
BEGIN
	OPEN rc FOR
SELECT ((started_first.num::real - started_both.num::real)
			/ num_of_peers.num * 100)::integer AS StartedBlock1,
		((started_second.num::real - started_both.num::real)
			/ num_of_peers.num * 100)::integer AS StartedBlock2,
		(started_both.num::real 
			/ num_of_peers.num * 100)::integer AS StartedBothBlocks,
		(started_none.num::real 
			/ num_of_peers.num * 100)::integer AS DidntStartAnyBlock			
FROM (
		SELECT count(nickname) AS num
		FROM peers
	) num_of_peers,
	(
		SELECT count(peer) AS num FROM (
			SELECT DISTINCT peer FROM checks
			WHERE title ~* (block1 || '[0-9]')) AS nickname
	) started_first,
	(
		SELECT count(peer) AS num FROM (
			SELECT DISTINCT peer FROM checks
			WHERE title ~* (block2 || '[0-9]')) AS nickname
	) started_second,
	(
		SELECT count(peer) AS num FROM (
			SELECT DISTINCT peer FROM checks
			WHERE title ~* (block1 || '[0-9]')
			INTERSECT
			SELECT DISTINCT peer FROM checks
			WHERE title ~* (block2 || '[0-9]')) AS nickname
	) started_both,
	(
		SELECT count(peer) AS num FROM (
			SELECT peers.nickname AS peer FROM
				(SELECT DISTINCT peer FROM checks
				WHERE title ~* (block1 || '[0-9]')
				UNION
				SELECT DISTINCT peer FROM checks
				WHERE title ~* (block2 || '[0-9]')) AS started_any
			RIGHT JOIN peers ON peers.nickname = started_any.peer
			WHERE started_any.peer IS NULL) AS nickname
	) started_none;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL find_peers_started_blocks('rc', 'CPP', 'SQl');
FETCH ALL IN "rc";
COMMIT;

BEGIN;
CALL find_peers_started_blocks('rc', 'C', 'SQl');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
 * Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения.
 */
--DROP PROCEDURE IF EXISTS find_birthday_checks_peers(rc refcursor);

CREATE OR REPLACE PROCEDURE find_birthday_checks_peers(rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	SELECT (num_of_successful_birthday_peers.num::real / num_of_peers.num * 100
				)::integer AS SuccessfulChecks,
		(num_of_failed_birthday_peers.num::real / num_of_peers.num * 100
				)::integer AS UnsuccessfulChecks
	FROM ( -- Всего пиров в базе
		SELECT count(nickname) AS num 
		FROM peers
	) num_of_peers,
	( -- Количество пиров с успешными проверками в ДР
		SELECT count(successful_birthday_peers.nickname) AS num 
		FROM ( -- Пиры с успешными проверками в ДР
			SELECT DISTINCT nickname FROM peers 
			JOIN checks ON peers.nickname = checks.peer
			JOIN p2p ON p2p.check_id = checks.id
			LEFT JOIN verter ON verter.check_id = checks.id 
			WHERE EXTRACT(
						MONTH
						FROM peers.birthday
					) = EXTRACT(
						MONTH
						from checks."date"
					)
				AND EXTRACT(
						DAY
						FROM peers.birthday
					) = EXTRACT(
						DAY
						FROM checks."date"
					)
				AND p2p.p2p_check_status = 'Success'
				AND (verter.verter_check_status = 'Success' 
					OR verter.verter_check_status IS NULL)
		) successful_birthday_peers
	) num_of_successful_birthday_peers,
	( --Количество пиров с неуспешными проверками в ДР
		SELECT count(failed_birthday_peers.nickname) AS num
		FROM ( -- Пиры с неуспешными проверками в ДР
			SELECT DISTINCT nickname FROM peers
			JOIN checks ON peers.nickname = checks.peer
			JOIN p2p ON p2p.check_id = checks.id
			LEFT JOIN verter ON verter.check_id = checks.id 
			WHERE EXTRACT(
						MONTH
						FROM peers.birthday
					) = EXTRACT(
						MONTH
						from checks."date"
					)
				AND EXTRACT(
						DAY
						FROM peers.birthday
					) = EXTRACT(
						DAY
						FROM checks."date"
					)
				AND (p2p.p2p_check_status = 'Failure'
					OR verter.verter_check_status = 'Failure')
		) failed_birthday_peers
	) num_of_failed_birthday_peers;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL find_birthday_checks_peers('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 11) Определить всех пиров, которые сдали заданные задания 1 и 2,
 * но не сдали задание 3
 */
--DROP PROCEDURE IF EXISTS show_peers_tasks12_not3(rc refcursor, task1 varchar(40), task2 varchar(40), task3 varchar (40));

CREATE OR REPLACE PROCEDURE show_peers_tasks12_not3(rc refcursor, task1 varchar(40), task2 varchar(40), task3 varchar (40)) AS $$
BEGIN
	OPEN rc FOR
	WITH successful_checks AS (						-- все успешные проверки
		SELECT checks.peer, checks.title FROM checks 
		JOIN p2p ON checks.id = p2p.check_id
		LEFT JOIN verter ON checks.id = verter.check_id
		WHERE p2p.p2p_check_status = 'Success'
			AND (verter.verter_check_status = 'Success'
				OR verter.verter_check_status IS NULL))
	SELECT DISTINCT peer FROM successful_checks	-- пиры, успешно сдавшие 1 задание
	WHERE successful_checks.title = task1
	INTERSECT									-- пересечение
	SELECT DISTINCT peer FROM successful_checks	-- с пирами, успешно сдавшими 2 задание
	WHERE successful_checks.title = task2
	EXCEPT										-- исключение
	SELECT DISTINCT peer FROM successful_checks	-- пиров, успешно сдавших 3 задание
	WHERE successful_checks.title = task3
	ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL show_peers_tasks12_not3('rc', 'C2_SimpleBashUtils', 'C3_String+', 'CPP1_s21_matrix+');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 12) Используя рекурсивное обобщенное табличное выражение,
 * для каждой задачи вывести кол-во предшествующих ей задач
 */
--DROP PROCEDURE IF EXISTS count_parent_tasks(rc refcursor);

CREATE OR REPLACE PROCEDURE count_parent_tasks(rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	WITH RECURSIVE r_tasks AS (
			SELECT
				title,
				parent_task,
				0 AS num
			FROM tasks
			WHERE parent_task IS NULL
		UNION ALL	
			SELECT
				tasks.title,
				tasks.parent_task,
				num + 1 AS num
			FROM tasks
				JOIN r_tasks ON r_tasks.title = tasks.parent_task
	)
	SELECT title AS Task, max(num) AS Prevcount
	FROM r_tasks
	GROUP BY title ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL count_parent_tasks('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 13) Найти "удачные" для проверок дни. День считается "удачным",
 * если в нем есть хотя бы N идущих подряд успешных проверки.
 * Параметры процедуры: количество идущих подряд успешных проверок N.
 * Временем проверки считать время начала P2P этапа.
 * Под идущими подряд успешными проверками подразумеваются успешные проверки,
 * между которыми нет неуспешных. При этом кол-во опыта за каждую из этих проверок
 * должно быть не меньше 80% от максимального.
 * Формат вывода: список дней
 */
--DROP PROCEDURE IF EXISTS lucky_check_days(rc refcursor, p_num_of_checks integer);

CREATE OR REPLACE PROCEDURE lucky_check_days(rc refcursor, p_num_of_checks integer) AS $$
BEGIN
	OPEN rc FOR
WITH RECURSIVE
	-- Выбирает дни, когда были какие-то проверки в принципе
	dates_of_checks AS (
		SELECT checks.id,
			checks."date",
			(CASE WHEN verter.verter_check_status IS NULL THEN p2p.p2p_check_status
				ELSE verter.verter_check_status
			END) AS status,
			max_xp
		FROM checks
		JOIN p2p ON p2p.check_id = checks.id
		JOIN tasks ON checks.title = tasks.title
		LEFT JOIN verter ON verter.check_id = checks.id
		WHERE (verter.verter_check_status != 'Start'
				OR verter.verter_check_status IS NULL)
			AND p2p.p2p_check_status != 'Start'),
	-- Подменяет статус на 'Failure', если ХР < 80% от максимального
	status_based_on_xp AS (
		SELECT dates_of_checks.id,
			"date",
			(CASE WHEN xp_amount::real / max_xp < 0.8
				OR xp_amount IS NULL THEN 'Failure'
				ELSE 'Success'
			END) AS status
		FROM dates_of_checks
		LEFT JOIN xp ON dates_of_checks.id = xp.check_id
	),
	-- Определяет время проверок
	time_of_checks AS (SELECT checks.id, p2p."time"
		FROM checks
		JOIN p2p ON p2p.check_id = checks.id
		WHERE p2p_check_status = 'Start'),
	-- Упорядочивает проверки по дате и времени, создаёт хронологически верную сквозную нумерацию
	checks_with_rns AS (SELECT ROW_NUMBER() OVER (ORDER BY "date", "time") AS rn,
							time_of_checks.id,
							"time",
							"date",
							"status"
						FROM time_of_checks
						JOIN dates_of_checks
						ON time_of_checks.id = dates_of_checks.id),
	-- Добавляет информацию о предшествующей по времени проверке
	previous_info AS (SELECT rns1.rn,
						rns1."date",
						rns1.status,
						rns2."date" AS date_prev,
						rns2."status" AS status_prev
					FROM checks_with_rns AS rns1
					LEFT JOIN checks_with_rns AS rns2
					ON rns2.rn = rns1.rn - 1),
	-- Расставляет числовые флаги в зависимости от особенностей предыдущей проверки
	previous_flag AS (SELECT rn,
						"date",
						(CASE WHEN status = 'Failure' THEN 0
							WHEN date_prev IS NULL
								OR date_prev != "date"
								OR status_prev = 'Failure' THEN 1
							ELSE 2
						END) AS flag
						FROM previous_info),
	-- Рекурсивная часть. Увеличивает значения флагов для последующих успешных проверок
	r_flag AS (SELECT 	0 rec_len,
						rn,
						"date",
						flag
				FROM previous_flag
				UNION ALL				
				SELECT	rec_len + 1,
						previous_flag.rn,
						previous_flag."date",
						(CASE WHEN previous_flag.flag > 1  
							AND previous_flag.flag = r_flag.flag
							THEN (previous_flag.flag + 1)
							ELSE previous_flag.flag
						END) AS flag
				FROM  previous_flag
				JOIN r_flag ON previous_flag.rn = r_flag.rn + 1
				WHERE rec_len < (SELECT max(num)
								FROM (SELECT "date", count(*) AS num
									FROM checks
									GROUP BY date) checks_a_day)
				),
	-- Создаёт список дней с максимальным значением успешных проверок подряд
	max_in_a_row AS (SELECT "date", max(flag) AS max_num
				FROM r_flag
				GROUP BY "date")
	-- Выбирает успешные дни в зависимости от параметра процедуры
	SELECT "date" AS LuckyDays
	FROM max_in_a_row
	WHERE p_num_of_checks <= max_in_a_row.max_num
	ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL lucky_check_days('rc', 2);
FETCH ALL IN "rc";
COMMIT;

BEGIN;
CALL lucky_check_days('rc', 0);
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 14) Определить пира с наибольшим количеством XP
 */
--DROP PROCEDURE IF EXISTS max_xp_peer(rc refcursor);

CREATE OR REPLACE PROCEDURE max_xp_peer(rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	-- Список всех проверок с нужными атрибутами
	WITH all_checks AS (
		SELECT checks.id, checks.peer, checks.title, "date", p2p."time", xp_amount
		FROM xp
		RIGHT JOIN checks ON xp.check_id = checks.id
		JOIN p2p ON p2p.check_id = checks.id
		LEFT JOIN verter ON verter.check_id = checks.id
		WHERE p2p.p2p_check_status != 'Start'
			AND (verter.verter_check_status != 'Start'
				OR verter.verter_check_status IS NULL)),
	-- Список "устаревших проверок", после которых были новые проверки той же задачи
	old_checks AS (
		SELECT DISTINCT all1.id
		FROM all_checks AS all1
		JOIN all_checks AS all2
		ON all1.peer = all2.peer AND all1.title = all2.title
		WHERE all1."date" < all2."date" OR
			(all1."date" = all2."date" AND all1."time" < all2."time"))
	-- Складывает сумму всех проверок, исключая "устаревшие"
	SELECT peer, sum(xp_amount) AS XP
	FROM all_checks
	LEFT JOIN old_checks ON all_checks.id = old_checks.id
	WHERE old_checks.id IS NULL
	GROUP BY peer
	ORDER BY XP DESC LIMIT 1; -- Выбирает одно максимальное значение для вывода
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL max_xp_peer('rc');
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 15) Определить пиров, приходивших раньше заданного времени
 * не менее N раз за всё время
 */
--DROP PROCEDURE IF EXISTS num_of_entrance_before_time(rc refcursor);

CREATE OR REPLACE PROCEDURE num_of_entrance_before_time(rc refcursor, p_entrance time, p_num integer) AS $$
BEGIN
	OPEN rc FOR
	-- Спиок записей в time_tracking, не относящихся к первому за день приходу в кампус
	WITH later_tracking AS (
		SELECT tt1.id 
		FROM time_tracking AS tt1
		JOIN time_tracking AS tt2
		ON tt1.peer = tt2.peer AND tt1."date" = tt2."date"
		WHERE tt1."time" > tt2."time"),
	-- Список записей о первом приходе в кампус
	entrance_time AS (
		SELECT peer, "time"
		FROM time_tracking
		LEFT JOIN later_tracking ON later_tracking.id = time_tracking.id
		WHERE later_tracking.id IS NULL),
	-- Подсчёт количества приходов ранее заданного времени для каждого пира
	entrance_by_peer AS (
		SELECT peer, count(peer) AS num_of_entrance
		FROM entrance_time
		WHERE "time" < p_entrance
		GROUP BY peer)
	-- Вывод списка пиров
	SELECT peer AS Peer
	FROM entrance_by_peer
	WHERE num_of_entrance >= p_num;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL num_of_entrance_before_time('rc', '12:00:00', 1);
FETCH ALL IN "rc";
COMMIT;

BEGIN;
CALL num_of_entrance_before_time('rc', '15:00:00', 2);
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 16) Определить пиров, выходивших за последние N дней
 * из кампуса больше M раз
 */
--DROP PROCEDURE IF EXISTS num_of_entrance_before_time(rc refcursor, p_num_days integer, p_num_times integer);

CREATE OR REPLACE PROCEDURE find_peers_leaving_x_times(rc refcursor, p_num_days integer, p_num_times integer) AS $$
BEGIN
	OPEN rc FOR
	WITH leavings AS ( -- Список выходов
		SELECT DISTINCT tt1.id, tt1.peer, tt1."date"
		FROM time_tracking AS tt1
		JOIN time_tracking AS tt2
		ON tt1.peer = tt2.peer AND tt1."date" = tt2."date" AND tt1.state = tt2.state
		WHERE tt1."time" > tt2."time" AND tt1.state = 1),
	num_of_leavings AS ( -- Подсчёт количества выходов за последние дни
		SELECT peer, count(peer) AS num
		FROM leavings
		WHERE "date" >= current_date - p_num_days * interval '1 day'
		GROUP BY peer)
	SELECT peer AS Peer -- Выбор имени пиров в зависимости от параметра "число выходов"
	FROM num_of_leavings
	WHERE num_of_leavings.num > p_num_times;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL find_peers_leaving_x_times('rc', 300, 1);
FETCH ALL IN "rc";
COMMIT;

BEGIN;
CALL find_peers_leaving_x_times('rc', 100, 1);
FETCH ALL IN "rc";
COMMIT;

BEGIN;
CALL find_peers_leaving_x_times('rc', 700, 2);
FETCH ALL IN "rc";
COMMIT;
*/

/**
 * 17) Определить для каждого месяца процент ранних входов
 * Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц,
 * приходили в кампус за всё время (будем называть это общим числом входов). 
 * Для каждого месяца посчитать, сколько раз люди, родившиеся в этот месяц, 
 * приходили в кампус раньше 12:00 за всё время (будем называть это числом ранних входов). 
 * Для каждого месяца посчитать процент ранних входов в кампус
 * относительно общего числа входов.
 */
--DROP PROCEDURE IF EXISTS find_early_entries_by_birth_month(rc refcursor);

CREATE OR REPLACE PROCEDURE find_early_entries_by_birth_month(rc refcursor) AS $$
BEGIN
	OPEN rc FOR
	-- Список пиров по месяцу рождения
	WITH peer_birth_month AS (
		SELECT nickname, EXTRACT(MONTH FROM peers.birthday) AS "month"
		FROM peers),
	-- Спиок записей в time_tracking, не относящихся к первому за день приходу в кампус
	later_tracking AS (
		SELECT tt1.id 
		FROM time_tracking AS tt1
		JOIN time_tracking AS tt2
		ON tt1.peer = tt2.peer AND tt1."date" = tt2."date"
		WHERE tt1."time" > tt2."time"),
	-- Список записей о первом за день приходе в кампус
	entrance_cases AS (
		SELECT peer, "time"
		FROM time_tracking
		LEFT JOIN later_tracking ON later_tracking.id = time_tracking.id
		WHERE later_tracking.id IS NULL),
	-- Список времени прихода в кампус в зависимости от месяца рождения пира
	entrance_time_by_birth_month AS (
		SELECT "time", "month"
		FROM entrance_cases
		JOIN peer_birth_month
			ON peer_birth_month.nickname = entrance_cases.peer),
	-- Общее количество приходов в кампус в зависимости от месяца рождения пира
	total_entrance AS (
		SELECT "month", count("month") AS num
		FROM entrance_time_by_birth_month
		GROUP BY "month"),
	-- Количество ранних приходов в кампус в зависимости от месяца рождения пира
	early_entrance AS (
		SELECT "month", count("month") AS num
		FROM entrance_time_by_birth_month
		WHERE "time" < '12:00:00'
		GROUP BY "month")
	-- Вывод итоговой таблицы. Процент ранних приходов в зависимости от месяца рождения
	SELECT (CASE WHEN total_entrance."month" = 1 THEN 'January'
			WHEN total_entrance."month" = 2 THEN 'February'
			WHEN total_entrance."month" = 3 THEN 'March'
			WHEN total_entrance."month" = 4 THEN 'April'
			WHEN total_entrance."month" = 5 THEN 'May'
			WHEN total_entrance."month" = 6 THEN 'June'
			WHEN total_entrance."month" = 7 THEN 'July'
			WHEN total_entrance."month" = 8 THEN 'August'
			WHEN total_entrance."month" = 9 THEN 'September'
			WHEN total_entrance."month" = 10 THEN 'October'
			WHEN total_entrance."month" = 11 THEN 'November'
			WHEN total_entrance."month" = 12 THEN 'December'
			END) AS "Month",
		(CASE WHEN early_entrance.num IS NULL THEN 0
			ELSE (early_entrance.num::real / total_entrance.num * 100
				)::integer
		END) AS "EarlyEntries"
	FROM total_entrance
	LEFT JOIN early_entrance
		ON total_entrance."month" = early_entrance."month"
	ORDER BY total_entrance."month";	
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
CALL find_early_entries_by_birth_month('rc');
FETCH ALL IN "rc";
COMMIT;
*/