
CREATE TABLE tickets (
    id_ticket INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 1) PRIMARY KEY,
    ticket_reserved NUMBER(1) CHECK (ticket_reserved IN (0, 1)),
    reserved_by VARCHAR2(20),
    IDR VARCHAR2(30)
);
/

CREATE TABLE Ticket_Reservation_Log (
    reservation_id NUMBER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
    pesel VARCHAR2(20),
    reservation_date TIMESTAMP,
    successful_reservation NUMBER(1) CHECK (successful_reservation IN (0, 1)),
    id_ticket INTEGER,
    IDR VARCHAR2(30),
    error_message VARCHAR2(100),
    CONSTRAINT fk_ticket FOREIGN KEY (id_ticket) REFERENCES tickets(id_ticket)
);
/
DECLARE
BEGIN
    FOR i IN 1..50000 LOOP
        INSERT INTO tickets (ticket_reserved, reserved_by, IDR)
        VALUES (0, NULL, NULL);
    END LOOP;
    COMMIT;
END;
/
CREATE OR REPLACE PROCEDURE p_ticket_reservation (
    pesel IN VARCHAR2,
    IDR OUT VARCHAR2,
    status OUT NUMBER
) AS

    v_ticket INTEGER;
    v_reserved_count INTEGER;
    max_attempts INTEGER := 5;
    attempts INTEGER := 0; 
    v_error_message VARCHAR2(100);

BEGIN

      IF pesel IS NULL THEN

        status := 0;
        IDR := NULL;

        v_error_message := 'Missing PESEL number';

        INSERT INTO Ticket_Reservation_Log(
            pesel,
            reservation_date,
            successful_reservation,
            id_ticket,
            IDR,
            error_message
        ) VALUES( 
            pesel,
            SYSDATE,
            0,
            NULL,
            NULL,
            v_error_message
        );

        RETURN; 
        
    END IF;


    SELECT COUNT(*) INTO v_reserved_count
    FROM tickets
    WHERE ticket_reserved = 1
    AND reserved_by = pesel;

    IF v_reserved_count >= 5 THEN

        status := 0;
        IDR := NULL;

        INSERT INTO Ticket_Reservation_Log(
                pesel,
                reservation_date,
                successful_reservation,
                id_ticket,
                IDR,
                error_message
                ) 
        VALUES( 
                pesel,
                SYSDATE,
                0,
                NULL,
                NULL,
                'Only 5 tickets can be reserved'
            );

        RETURN; 
    END IF;

    
    WHILE attempts < max_attempts LOOP
        BEGIN
            SELECT id_ticket INTO v_ticket 
            FROM tickets 
            WHERE ticket_reserved = 0
            AND ROWNUM = 1
            FOR UPDATE;

            EXIT;
        EXCEPTION

            WHEN NO_DATA_FOUND THEN
                DBMS_LOCK.SLEEP(1); 
                attempts := attempts + 1;
            IF attempts >= max_attempts THEN
                RETURN;
            END IF;
        END;
    END LOOP;


    IF v_ticket IS NOT NULL THEN

        status := 1;
        IDR := v_ticket || pesel || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');

        UPDATE tickets
        SET ticket_reserved = 1,
            reserved_by = pesel,
            IDR = v_ticket || pesel || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS')
        WHERE id_ticket = v_ticket;
        
        

        INSERT INTO Ticket_Reservation_Log( 
                pesel,
                reservation_date,
                successful_reservation,
                id_ticket,
                IDR
            ) 
        VALUES(
                pesel,
                SYSDATE,
                1,
                v_ticket,
                IDR
            );
        
    ELSE
        status := 0;
        IDR := NULL;

        INSERT INTO Ticket_Reservation_Log(
                pesel,
                reservation_date,
                successful_reservation,
                id_ticket,
                IDR
            ) 
        VALUES( 
                pesel,
                SYSDATE,
                1,
                NULL,
                IDR
            );

    END IF;
    COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE p_ticket_cancel_reservation (
    p_IDR IN VARCHAR2,
    cancellation_result OUT NUMBER
) AS
    v_cancelled_tickets INTEGER;
BEGIN

    UPDATE tickets
        SET ticket_reserved = 0,
            reserved_by = NULL,
            IDR = NULL
        WHERE IDR = p_IDR;

    IF SQL%FOUND THEN
        cancellation_result := 1;
        
    ELSE 
        cancellation_result := 0;
    END IF;

    COMMIT;
     

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
        RAISE;

END p_ticket_cancel_reservation;
/




set serveroutput on
DECLARE
    v_pesel VARCHAR2(20) := '112233445566'; 
    v_IDR VARCHAR2(30); 
    v_status NUMBER; 
BEGIN
    p_ticket_reservation(v_pesel, v_IDR, v_status); 
    
    DBMS_OUTPUT.PUT_LINE('IDR: ' || v_IDR);
    DBMS_OUTPUT.PUT_LINE('Status: ' || v_status);
END;

DECLARE
    v_IDR VARCHAR2(100);
    v_status NUMBER;
BEGIN
    p_ticket_reservation(NULL, v_IDR, v_status);
    DBMS_OUTPUT.PUT_LINE('IDR: ' || v_IDR);
    DBMS_OUTPUT.PUT_LINE('Status: ' || v_status);
END;
/

DECLARE
    v_IDR VARCHAR2(100);
    v_status NUMBER;
BEGIN
    
    FOR i IN 1..10 LOOP 
        p_ticket_reservation('12345678901', v_IDR, v_status);
        DBMS_OUTPUT.PUT_LINE('Attempt ' || i || ': IDR: ' || v_IDR || ', Status: ' || v_status);
    END LOOP;
END;
/

DECLARE
    v_IDR VARCHAR2(100) := ''; 
    v_cancellation_result NUMBER;
BEGIN
    p_ticket_cancel_reservation(v_IDR, v_cancellation_result); 
    DBMS_OUTPUT.PUT_LINE('Cancellation Result: ' || v_cancellation_result);
END;
/