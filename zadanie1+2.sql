
CREATE TABLE FACT_HISTORY 
(
    ID_FACT NUMBER,
    ID_PACK NUMBER,
    SAMPLE_FIELD VARCHAR2(20),
    SAMPLE_DATE DATE,
    CONSTRAINT PK_FACT_HISTORY PRIMARY KEY (ID_PACK, ID_FACT)
);
/


INSERT INTO FACT_HISTORY (id_fact, id_pack, SAMPLE_FIELD,SAMPLE_DATE) VALUES (1, 1 , 'test', TO_DATE('2024-04-01', 'YYYY-MM-DD')) ;
INSERT INTO FACT_HISTORY (id_fact, id_pack, SAMPLE_FIELD,SAMPLE_DATE)  VALUES (1, 2, 'test1', TO_DATE('2025-04-01', 'YYYY-MM-DD'));
INSERT INTO FACT_HISTORY (id_fact, id_pack, SAMPLE_FIELD,SAMPLE_DATE)  VALUES (1, 123, NULL, TO_DATE('2024-04-01', 'YYYY-MM-DD'));
INSERT INTO FACT_HISTORY (id_fact, id_pack, SAMPLE_FIELD,SAMPLE_DATE)  VALUES (1, 150, 'test', TO_DATE('2023-04-01', 'YYYY-MM-DD'));
INSERT INTO FACT_HISTORY (id_fact, id_pack, SAMPLE_FIELD,SAMPLE_DATE)  VALUES (1, 156, NULL, NULL);
/

CREATE GLOBAL TEMPORARY TABLE TMP_FACT_HISTORY_COMPARE_STATUS (
                            id_fact NUMBER,
                            status NUMBER
                            ) ON COMMIT PRESERVE ROWS;
/
CREATE GLOBAL TEMPORARY TABLE TMP_FACT_HISTORY_COMPARE_DIFF (
    ID_FACT NUMBER,
    COL_NAME VARCHAR2(100),
    COL_TYPE VARCHAR2(100),
    COL_OLD_VALUE VARCHAR2(100),
    COL_NEW_VALUE VARCHAR2(100)
) ON COMMIT PRESERVE ROWS;
/
CREATE OR REPLACE PACKAGE comparator AS
    PROCEDURE compare_fact(id_pack_old IN NUMBER, id_pack_new IN NUMBER);
END comparator;
/
CREATE OR REPLACE PACKAGE BODY comparator AS
   
   PROCEDURE compare_fact(id_pack_old IN NUMBER, id_pack_new IN NUMBER) IS

    v_column_name VARCHAR2(100);
    v_sql VARCHAR2(1000); 
    v_old_value VARCHAR2(100); 
    v_new_value VARCHAR2(100); 
    v_compare_result NUMBER;
    v_id_fact NUMBER;
    v_last_index NUMBER; 
    v_current_index NUMBER := 0;


    CURSOR c_columns IS
      SELECT column_name
      FROM user_tab_columns
      WHERE table_name = 'FACT_HISTORY' AND column_name NOT IN ('ID_FACT', 'ID_PACK');

BEGIN

    SELECT ID_FACT INTO v_id_fact FROM FACT_HISTORY FETCH FIRST 1 ROW ONLY;

    SELECT COUNT(*) INTO v_last_index FROM user_tab_columns WHERE table_name = 'FACT_HISTORY' AND column_name NOT IN ('ID_FACT', 'ID_PACK');
    
    FOR column_rec IN c_columns LOOP
      v_column_name := column_rec.column_name;
      
      v_current_index := v_current_index + 1;
     
      v_sql := 'SELECT ' || v_column_name || ' FROM fact_history WHERE ID_PACK = :id';
      
      BEGIN
        
        EXECUTE IMMEDIATE v_sql INTO v_old_value USING id_pack_old;
        
        EXECUTE IMMEDIATE v_sql INTO v_new_value USING id_pack_new;
        
      --  (1 - MODIFIED, 2 - NEW, 3 - DELETED)
        IF v_old_value <> v_new_value THEN
            v_compare_result := 1; 
            INSERT INTO TMP_FACT_HISTORY_COMPARE_DIFF VALUES(v_id_fact,v_column_name,
                                                                    (SELECT DATA_TYPE
                                                                        FROM USER_TAB_COLUMNS
                                                                        WHERE TABLE_NAME = 'FACT_HISTORY'
                                                                        AND COLUMN_NAME = v_column_name) , v_old_value , v_new_value
                                                                        );
            IF v_current_index = v_last_index THEN
            INSERT INTO TMP_FACT_HISTORY_COMPARE_STATUS VALUES(v_id_fact,v_compare_result);
            END IF;
        ELSIF v_old_value IS NULL THEN 
            v_compare_result := 2; 
            INSERT INTO TMP_FACT_HISTORY_COMPARE_STATUS VALUES(v_id_fact,v_compare_result);
            EXIT;
        ELSIF v_new_value IS NULL THEN 
            v_compare_result := 3; 
            INSERT INTO TMP_FACT_HISTORY_COMPARE_STATUS VALUES(v_id_fact,v_compare_result);
            EXIT;
        END IF;
      
      EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('An error occurred while executing dynamic SQL: ' || SQLERRM);
          ROLLBACK;
      END;
      
    END LOOP;
    COMMIT;
  END compare_fact;

END comparator;
/

set SERVEROUTPUT ON
EXECUTE COMPARATOR.COMPARE_FACT(1,2);

END;




   