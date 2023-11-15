/**
 * 1) Процедура добавления P2P проверки
 * ! Нет проверки, что предыдущая проверка со статусом 'Start' была закрыта
 * Если добавить подряд две проверки со статусом 'Start' и одинаковыми параметрами,
 * то система это даст сделать, но проверка, которая будет стоять раньше по времени
 * не сможет быть завершена с помощью этой процедуры
 */
CREATE OR REPLACE PROCEDURE add_p2p
(
	p_checked_peer varchar(40),
	p_checking_peer varchar(40),
	p_check_title varchar(40),
	p_check_state check_status,
	p_check_time time
) LANGUAGE plpgsql AS $$
DECLARE last_check_id bigint;
		last_check_status check_status;
BEGIN
	IF p_check_state = 'Start' THEN
		INSERT INTO checks (id, peer, title, date)		-- Запись в таблицу 'checks' для случая, если параметр 'Start'
		VALUES (
			(SELECT COALESCE(max(id), 0) + 1 FROM checks),
			p_checked_peer,
			p_check_title,
			(SELECT CURRENT_DATE)
		);
		INSERT INTO p2p (id, check_id, checking_peer, p2p_check_status, time) -- Запись в таблицу 'p2p' для случая, если параметр 'Start'
		VALUES (
			(SELECT COALESCE(max(id), 0) + 1 FROM p2p),
			(SELECT max(checks.id) FROM checks),
			p_checking_peer,
			p_check_state,
			p_check_time
		);
	ELSE												-- Случай, когда статус параметра не 'Start'
		last_check_id = (SELECT max(checks.id) FROM checks -- Поиск последнего id по параметрам (пир, задача) для таблицы 'checks'
							INNER JOIN p2p
								ON p2p.check_id = checks.id 
							WHERE p_checked_peer = checks.peer
								AND p_check_title = checks.title
								AND p_checking_peer = p2p.checking_peer);
		last_check_status = (SELECT p2p_check_status FROM -- Доп. проверка статуса в найденной строке. Т.к. в параметре не 'Start', то здесь должен быть найден 'Start'
								(SELECT * FROM p2p
									WHERE last_check_id = check_id
									ORDER BY "time" DESC
									LIMIT 1) X);
		IF last_check_status = 'Start' THEN	-- Обрабатывается, если статус предыдущей записи 'Start'. Если не 'Start' - не делает ничего
			INSERT INTO p2p (id, check_id, checking_peer, p2p_check_status, time) -- Запись в таблицу 'p2p' для случая, если параметр не 'Start' 
			VALUES (
				(SELECT COALESCE(max(id), 0) + 1 FROM p2p),
				last_check_id,
				p_checking_peer,
				p_check_state,
				p_check_time
			);	
		END IF;
	END IF;
END
$$;


/**
 * 2) Процедура добавления проверки Verter-ом
 * Не делается специальной проверки, что был Start для случаев,
 * когда статус не Start
 */
CREATE OR REPLACE PROCEDURE add_verter
(
	p_checked_peer varchar(40),
	p_check_title varchar(40),
	p_check_state check_status,
	p_check_time time
) LANGUAGE plpgsql AS $$
DECLARE last_check_id bigint := 0;
BEGIN
	last_check_id = (SELECT X.id FROM -- Выбирает последнюю успешную p2p проверку задания пиром по дате и времени 
						(SELECT checks.id, p2p."time", checks."date" 
						FROM checks
						JOIN p2p ON p2p.check_id = checks.id
						WHERE p2p.p2p_check_status = 'Success'
							AND p_check_title = checks.title
							AND p_checked_peer = checks.peer
						ORDER BY "date" DESC, "time" DESC LIMIT 1
						) AS X
					);
	IF last_check_id > 0 THEN
		INSERT INTO verter (id, check_id, verter_check_status, time) -- Делает запись в Вертер
		VALUES (
			(SELECT COALESCE(max(id), 0) + 1 FROM verter),
			last_check_id,
			p_check_state,
			p_check_time
		);
	END IF;
END
$$;


/**
 * 3) Триггер: после добавления записи со статутом "начало" в таблицу P2P,
 *  изменяетяется соответствующую запись в таблице TransferredPoints
 */
CREATE OR REPLACE FUNCTION fnc_trg_transferred_points_change() RETURNS TRIGGER AS $$
DECLARE
	l_checked_peer varchar(40) := (SELECT checks.peer
								FROM checks
								WHERE id = NEW.check_id);
BEGIN
	IF NEW.p2p_check_status = 'Start' THEN
		IF EXISTS -- Проверка, что в 'transferred_points' есть сочетание проверяющего и проверяемого пиров
		(
			SELECT tp.id FROM transferred_points tp
			WHERE tp.checking_peer = NEW.checking_peer
				AND tp.checked_peer = l_checked_peer
		)
		THEN -- Сочетание есть - тогда обновляем
			UPDATE transferred_points tp 
			SET points_amount = points_amount + 1
			WHERE tp.checking_peer = NEW.checking_peer
				AND tp.checked_peer = l_checked_peer;
		ELSE -- Сочетания нет - тогда создаём
			INSERT INTO transferred_points (id, checking_peer, checked_peer, points_amount)
			VALUES (
				(SELECT COALESCE(max(id), 0) + 1 FROM transferred_points),
				NEW.checking_peer,
				l_checked_peer,
				1
			);
		END IF;
	END IF;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transferred_points_change
AFTER INSERT
ON p2p
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_transferred_points_change();

/**
 *  4) Триггер: перед добавлением записи в таблицу XP
 * 	проверяется корректность добавляемой записи
 */
CREATE OR REPLACE FUNCTION fnc_trg_xp_check() RETURNS TRIGGER AS $$
DECLARE
BEGIN
	IF
	(
		NEW.xp_amount > (SELECT max_xp FROM tasks -- Проверяется на превышение max_xp
						JOIN checks ON checks.title = tasks.title
						WHERE checks.id = NEW.check_id)
		OR
		NEW.check_id NOT IN (SELECT checks.id FROM checks -- Проверяется, что проверка успешаня
							JOIN p2p ON checks.id = p2p.check_id
							LEFT JOIN verter ON checks.id = verter.check_id
							WHERE NEW.check_id = checks.id
							AND (verter.verter_check_status = 'Success' 	-- Успешная проверка Вертером
								OR (verter.verter_check_status IS NULL		-- или Вертером нет проверки,
									AND p2p.p2p_check_status = 'Success'))) -- но успешная p2p
	)
	THEN RETURN NULL;
	ELSE RETURN NEW;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_xp_check
BEFORE INSERT
ON xp
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_xp_check();

/**
 * Тесты
 */ 
-- Тест, что не будет добавлена запись об окончании проверки, если не было начала
CALL add_p2p (
	'Sus',
	'Boba',
	'C2_SimpleBashUtils',
	'Success',
	'09:00:00'
);

-- Тест, что запись о начале проверки будет успешно добавлена
-- Срабатывает триггер trg_transferred_points_change
CALL add_p2p (
	'Sus',
	'Boba',
	'C2_SimpleBashUtils',
	'Start',
	'10:00:00'
);

-- Тест, что запись об окончании проверки будет успешно добавлена
CALL add_p2p (
	'Sus',
	'Boba',
	'C2_SimpleBashUtils',
	'Success',
	'10:30:00'
);

-- Тест, что не будет добавлена запись об окончании проверки, если проверка
-- с такими параметрами уже была, но незаконченной проверки нет
CALL add_p2p (
	'Sus',
	'Boba',
	'C2_SimpleBashUtils',
	'Success',
	'10:35:00'
);

-- Добавление старта проверки Verter
CALL add_verter (
	'Sus',
	'C2_SimpleBashUtils',
	'Start',
	'11:00:00'
);
-- Добавление окончания проверки Verter
CALL add_verter (
	'Sus',
	'C2_SimpleBashUtils',
	'Success',
	'11:01:00'
);

-- Тест, что в отношение хр не вносится запись, если xp_amount больше максимального
INSERT INTO xp(id, check_id, xp_amount)
VALUES(
	(SELECT COALESCE(max(id), 0) + 1 FROM xp),
	29,
	350
);

-- Тест, что в отношение хр вносится запись после успешной проверки в Verter
INSERT INTO xp(id, check_id, xp_amount)
VALUES(
	(SELECT COALESCE(max(id), 0) + 1 FROM xp),
	29,
	300
);
-- Тест, что в отношение хр вносится запись после успешной проверки в p2p,
-- при отсутствии проверки в Verter
INSERT INTO xp(id, check_id, xp_amount)
VALUES(
	(SELECT COALESCE(max(id), 0) + 1 FROM xp),
	12,
	740
);

-- Тест, что не вносится запись в отношение хр после неуспешной проверки в Verter 
INSERT INTO xp(id, check_id, xp_amount)
VALUES(
	(SELECT COALESCE(max(id), 0) + 1 FROM xp),
	10,
	300
);

-- Тест, что не вносится запись в отношение хр после неуспешной проверки в p2p 
INSERT INTO xp(id, check_id, xp_amount)
VALUES(
	(SELECT COALESCE(max(id), 0) + 1 FROM xp),
	8,
	300
);