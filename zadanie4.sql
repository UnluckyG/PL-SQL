CREATE TABLE ORDERS (
ORDER_ID NUMBER,
CUSTOMER_ID NUMBER,
ORDER_DATE DATE,
AMOUNT NUMBER(10,2)
);
BEGIN
    FOR i IN 2..5 LOOP
        INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, AMOUNT)
        VALUES (i, 1000 + i, SYSDATE - i, i * 25.00);
    END LOOP;
    COMMIT;
END;

Select * from orders;

CREATE OR REPLACE TYPE client_order_sum AS OBJECT (
    client_id NUMBER,
    sum_amount NUMBER
);

CREATE OR REPLACE TYPE client_order_sum_table AS TABLE OF client_order_sum;

CREATE OR REPLACE FUNCTION sum_of_sales_between_dates(StartDate DATE, EndDate DATE) 
RETURN client_order_sum_table
IS
    sales_list client_order_sum_table := client_order_sum_table();
BEGIN
    SELECT client_order_sum(CUSTOMER_ID, SUM(AMOUNT))
    BULK COLLECT INTO sales_list
    FROM ORDERS
    WHERE ORDER_DATE BETWEEN StartDate AND EndDate
    GROUP BY CUSTOMER_ID
    ORDER BY SUM(AMOUNT) DESC;
    
    RETURN sales_list;
END sum_of_sales_between_dates;
/

DECLARE
    sales_list client_order_sum_table;
BEGIN
    sales_list := sum_of_sales_between_dates(TO_DATE('2024-04-01', 'YYYY-MM-DD'), TO_DATE('2024-04-04', 'YYYY-MM-DD'));
    
    FOR i IN 1..sales_list.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Total Sales for Customer '|| sales_list(i).client_id || ': ' || sales_list(i).sum_amount);
    END LOOP;
END;
/