/**
 * 1) Создать хранимую процедуру, которая, не уничтожая базу данных,
 * уничтожает все те таблицы текущей базы данных,
 * имена которых начинаются с фразы 'TableName'.
 */
--DROP PROCEDURE IF EXISTS drop_tables_by_name_begining(name_of_table varchar);

CREATE OR REPLACE PROCEDURE drop_tables_by_name_begining(name_of_table varchar) AS $$
BEGIN	
	FOR name_of_table IN
	    SELECT quote_ident(table_name) AS tn
	    FROM information_schema.TABLES
	    WHERE table_name LIKE (name_of_table || '%')
	    	AND table_schema = 'public'
	LOOP
	EXECUTE 'DROP TABLE IF EXISTS' || name_of_table || ' CASCADE';
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS test(i integer);
CREATE TABLE IF NOT EXISTS test2(i integer);
CREATE TABLE IF NOT EXISTS "TableName1"(i integer);

BEGIN;
CALL drop_tables_by_name_begining('test');
COMMIT;
END;

BEGIN;
CALL drop_tables_by_name_begining('TableName');
COMMIT;
END;

/**
 * 2) Создать хранимую процедуру с выходным параметром, которая выводит список
 * имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
 * Имена функций без параметров не выводить. Имена и список параметров должны
 * выводиться в одну строку. Выходной параметр возвращает количество найденных функций.
 */
--DROP PROCEDURE IF EXISTS count_scalar_functions(OUT "Number_of_functions" integer);

CREATE OR REPLACE PROCEDURE count_scalar_functions(OUT "Number_of_functions" integer) AS $$
DECLARE func_record record;
list_of_functions varchar := '';
BEGIN
	"Number_of_functions" := 0;
FOR func_record IN
	SELECT rout.routine_name AS func_name,
		string_agg(param.parameter_name || ' ' || param.data_type, ', ') AS parameters 
	FROM information_schema."routines" rout
	JOIN information_schema.parameters param 
	ON rout.specific_name = param.specific_name
	WHERE rout.specific_schema = 'public'
		AND rout.routine_type = 'FUNCTION'
		AND param.data_type IS NOT NULL 
		AND rout.data_type IN ('bit', 'tinyint', 'smallint', 'int', 'integer',
		'bigint', 'int4', 'int8', 'decimal', 'numeric', 'float', 'real', 'date',
		'time', 'datetime', 'timestamp', 'year', 'char', 'varchar', 'text',
		 'nchar', 'nvarchar', 'ntext', 'boolean', 'uuid', 'character varying')
	GROUP BY func_name
LOOP list_of_functions := list_of_functions || func_record.func_name ||
		'(' || func_record.parameters || ')' || chr(10);
	"Number_of_functions" := "Number_of_functions" + 1;
END LOOP;
RAISE NOTICE '%', list_of_functions;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION empty_params() RETURNS integer AS $$
BEGIN
RETURN 1::integer;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION return_i(i integer) RETURNS integer AS $$
BEGIN
RETURN i;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION return_sum(x integer, y integer) RETURNS integer AS $$
BEGIN
RETURN x + y;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION add_hello(t varchar) RETURNS varchar AS $$
BEGIN
RETURN t || ' Hello there!';
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL count_scalar_functions(0);
COMMIT;
END;

/**
 * 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все
 * SQL DML триггеры в текущей базе данных. Выходной параметр возвращает
 * количество уничтоженных триггеров.
 */
--DROP PROCEDURE IF EXISTS destroy_dml_triggers(OUT "Number_of_destroyed_triggers" integer);

CREATE OR REPLACE PROCEDURE destroy_dml_triggers(OUT "Number_of_destroyed_triggers" integer) AS $$
DECLARE tr record;
BEGIN
	"Number_of_destroyed_triggers" := 0;
	FOR tr IN
		SELECT trigger_name,
		event_object_table
		FROM information_schema.triggers
		WHERE trigger_schema = 'public'
			AND (event_manipulation = 'INSERT'
				OR event_manipulation = 'UPDATE'
				OR event_manipulation = 'DELETE')
	LOOP
		EXECUTE 'DROP TRIGGER IF EXISTS ' || tr.trigger_name || ' ON '
			|| tr.event_object_table;
		"Number_of_destroyed_triggers" := "Number_of_destroyed_triggers" + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS trigger_test(i integer);

CREATE OR REPLACE FUNCTION fnc_trg_test() RETURNS TRIGGER AS $$
BEGIN
	RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_test
AFTER INSERT
ON trigger_test
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_test();

CREATE OR REPLACE FUNCTION fnc_trg_test1() RETURNS TRIGGER AS $$
BEGIN
	RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_test1
AFTER DELETE
ON trigger_test
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_test1();

CREATE OR REPLACE FUNCTION fnc_trg_test2() RETURNS TRIGGER AS $$
BEGIN
	RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_test2
AFTER UPDATE
ON trigger_test
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_test2();


BEGIN;
CALL destroy_dml_triggers(0);
COMMIT;
END;

/**
 * 4) Создать хранимую процедуру с входным параметром, которая выводит имена
 * и описания типа объектов (только хранимых процедур и скалярных функций),
 * в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.
 */
--DROP PROCEDURE IF EXISTS show_routines_names(string varchar);

CREATE OR REPLACE PROCEDURE show_routines_names(string varchar) AS $$
DECLARE r record;
list_of_routines varchar := '';
BEGIN
	FOR r IN
		SELECT routine_name AS "name", 'function' AS object_type
		FROM information_schema.routines
		WHERE routine_schema = 'public'
			AND routine_type = 'FUNCTION'
			AND (routine_name LIKE '%'|| string || '%'
				OR routine_definition LIKE '%'|| string || '%')
			AND data_type IN ('bit', 'tinyint', 'smallint', 'int', 'integer',
			'bigint', 'int4', 'int8', 'decimal', 'numeric', 'float', 'real', 'date',
			'time', 'datetime', 'timestamp', 'year', 'char', 'varchar', 'text',
			 'nchar', 'nvarchar', 'ntext', 'boolean', 'uuid', 'character varying')
		UNION
		SELECT routine_name, 'procedure'
		FROM information_schema.routines
		WHERE routine_schema = 'public'
			AND routine_type = 'PROCEDURE'
			AND (routine_name LIKE '%'|| string || '%'
				OR routine_definition LIKE '%'|| string || '%')
	LOOP 
		list_of_routines := list_of_routines || r."name" || ' ' || r.object_type
							|| chr(10);
	END LOOP;
	RAISE NOTICE '%', list_of_routines;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL show_routines_names('SELECT');
COMMIT;
END;

BEGIN;
CALL show_routines_names('show');
COMMIT;
END;

BEGIN;
CALL show_routines_names('peer');
COMMIT;
END;