BEGIN;

CREATE SCHEMA test_pg_array_fk;
SET search_path TO test_pg_array_fk;
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP on

-- Load the library.

\ir ./pg-array-fk-up.sql

-- Initial fixture.

CREATE TABLE test (
    id text,
    parents text[],
    ancestors text[]
);

-- Add triggers.
SELECT array_fk_create('test', 'parents', 'test', 'text');
SELECT array_fk_create('test', 'ancestors', 'test', 'text');

-- Add stuff
INSERT INTO test VALUES ('root', '{null}', '{null}');
INSERT INTO test VALUES ('root.foo', '{root}', '{}');
INSERT INTO test VALUES ('root.bar', '{root}', '{}');
INSERT INTO test VALUES ('root.foo.child', '{root,root.foo}', '{}');
INSERT INTO test VALUES ('orphan', '{}', '{}');

UPDATE test SET ancestors = '{root.foo,root.bar,root.foo.child}' WHERE id = 'root';
UPDATE test SET ancestors = '{root.foo.child}' WHERE id = 'root.foo';
UPDATE test SET ancestors = '{root.foo.child}' WHERE id = 'orphan';
UPDATE test SET parents = '{root.foo,root}' WHERE id = 'orphan';
UPDATE test SET ancestors = ancestors || '{orphan}' WHERE id IN ('root', 'root.foo');

DO $$
  BEGIN
    DELETE FROM test WHERE id = 'root';
    ASSERT NOT EXISTS (
      SELECT id FROM test WHERE 'root' = ANY(ancestors) OR 'root' = ANY(parents)
    ), 'root still present';

    DELETE FROM test WHERE id = 'orphan';
    ASSERT NOT EXISTS (
      SELECT id FROM test WHERE 'orphan' = ANY(ancestors) OR 'orphan' = ANY(parents)
    ), 'orphan still present';

    BEGIN
      UPDATE test SET parents = '{not-found}' WHERE id = 'root.foo';
      SELECT 1/0;
    EXCEPTION
      WHEN division_by_zero THEN RAISE EXCEPTION 'Should not reach here';
      WHEN OTHERS THEN RAISE NOTICE 'ok!';
    END;

    DELETE FROM test WHERE id = 'root.foo.child';

    PERFORM test FROM test
      WHERE parents IS DISTINCT FROM '{}' OR ancestors IS DISTINCT FROM '{}';
    IF FOUND THEN
      RAISE 'All rows must have empty parents/ancestors after DELETE.';
    END IF;
  END;
$$ LANGUAGE plpgsql;

-- Test that cleanup works.

SELECT array_fk_drop('test', 'parents', 'test');
SELECT array_fk_create('test', 'ancestors', 'test', 'text');

\ir ./pg-array-fk-down.sql

ROLLBACK;
