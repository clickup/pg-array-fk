CREATE OR REPLACE FUNCTION array_fk_create(
  child_table text,
  col text,
  parent_table text,
  id_type text = 'bigint'
)
RETURNS text LANGUAGE plpgsql AS $body$
DECLARE
  template text;
BEGIN
  template := $t$
    CREATE OR REPLACE FUNCTION {child_table}_{col}_fk_after_modify_trigger() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    AS $$
    DECLARE
      new_parent_ids {id_type}[];
      new_parent_ids_existing {id_type}[];
      new_parent_ids_missing {id_type}[];
    BEGIN
      IF TG_OP = 'UPDATE' THEN
        -- Only take modified array-fk columns into accounts. This allows
        -- after_delete_trigger to update multiple array-fk columns (if the
        -- table has more than one) on deletion without temporary conflicts.
        new_parent_ids := ARRAY(
            SELECT UNNEST(new_rows.{col})
            FROM new_rows JOIN old_rows ON old_rows.id = new_rows.id
            WHERE new_rows.{col} IS DISTINCT FROM old_rows.{col} AND NOT (
              /* Skip rows if new ids are subset of old ids */
              new_rows.{col} <@ old_rows.{col}
            )
            GROUP BY 1
        );
      ELSE
        -- Runs on INSERT.
        new_parent_ids := ARRAY(SELECT unnest({col}) FROM new_rows GROUP BY 1);
      END IF;

      IF new_parent_ids = '{}' THEN
        RETURN NULL;
      END IF;

      new_parent_ids_existing := ARRAY(
        SELECT id FROM {parent_table}
        WHERE id = ANY(new_parent_ids)
        FOR KEY SHARE
      );
      new_parent_ids_missing := ARRAY(
        SELECT unnest(new_parent_ids)
        EXCEPT SELECT unnest(new_parent_ids_existing)
        EXCEPT SELECT NULL
      );

      IF new_parent_ids_missing <> '{}' THEN
        RAISE '%',
          format(
            'insert or update on table %I violates foreign key constraint for column %I',
            '{child_table}', '{col}'
          )
          USING
            DETAIL = format(
              'Key(s) (%I) = ANY(%L) are not present in table %I',
              'id', new_parent_ids_missing, '{parent_table}'
            ),
            COLUMN = '{col}',
            CONSTRAINT = '{col}',
            TABLE = '{child_table}',
            SCHEMA = current_schema,
            ERRCODE = '23000';
      END IF;

      RETURN NULL;
    END;
    $$;

    CREATE OR REPLACE FUNCTION {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    AS $$
    DECLARE
      parent_ids {id_type}[];
    BEGIN
      parent_ids := ARRAY(SELECT id FROM old_rows);

      IF parent_ids <> '{}' THEN
        UPDATE {child_table}
          SET {col} = ARRAY(SELECT unnest({col}) EXCEPT SELECT unnest(parent_ids))
          WHERE {col} && parent_ids;
      END IF;

      RETURN NULL;
    END;
    $$;

    DROP TRIGGER IF EXISTS {child_table}_{col}_fk_after_insert_trigger ON {child_table};
    CREATE TRIGGER {child_table}_{col}_fk_after_insert_trigger
      AFTER INSERT ON {child_table}
      REFERENCING NEW TABLE AS new_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {child_table}_{col}_fk_after_modify_trigger();

    DROP TRIGGER IF EXISTS {child_table}_{col}_fk_after_update_trigger ON {child_table};
    CREATE TRIGGER {child_table}_{col}_fk_after_update_trigger
      AFTER UPDATE ON {child_table}
      REFERENCING NEW TABLE AS new_rows OLD TABLE AS old_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {child_table}_{col}_fk_after_modify_trigger();

    DROP TRIGGER IF EXISTS {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger ON {parent_table};
    CREATE TRIGGER {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger
      AFTER DELETE ON {parent_table}
      REFERENCING OLD TABLE AS old_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger();
  $t$;

  template := replace(template, '{child_table}', child_table);
  template := replace(template, '{col}', col);
  template := replace(template, '{parent_table}', parent_table);
  template := replace(template, '{id_type}', id_type);
  EXECUTE template;

  RETURN child_table || '.' || col || ' now refers to ' || parent_table;
END;
$body$;


CREATE OR REPLACE FUNCTION array_fk_drop(
  child_table text,
  col text,
  parent_table text
)
RETURNS text LANGUAGE plpgsql AS $body$
DECLARE
  template text;
BEGIN
  template := $$
    DROP TRIGGER {child_table}_{col}_fk_after_insert_trigger ON {child_table};
    DROP TRIGGER {child_table}_{col}_fk_after_update_trigger ON {child_table};
    DROP TRIGGER {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger ON {parent_table};
    DROP FUNCTION {child_table}_{col}_fk_after_modify_trigger();
    DROP FUNCTION {parent_table}_id_to_{child_table}_{col}_fk_after_delete_trigger();
  $$;

  template := replace(template, '{child_table}', child_table);
  template := replace(template, '{col}', col);
  template := replace(template, '{parent_table}', parent_table);
  EXECUTE template;

  RETURN child_table || '.' || col || ' no more refers to ' || parent_table;
END;
$body$;
