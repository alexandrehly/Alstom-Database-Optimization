-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS CreateIntermediateTableInterval//
CREATE PROCEDURE CreateIntermediateTableInterval( interm_table_name text, field_name text, field_type text, input text, length int) 
BEGIN	
    
    DECLARE n int unsigned default 1;

    SET @drop_temp = CONCAT('DROP TABLE IF EXISTS ',interm_table_name);
	PREPARE drop_temp from @drop_temp;
	EXECUTE drop_temp;
    
	SET @create_temp = CONCAT( 'CREATE TABLE ',interm_table_name,' (id int PRIMARY KEY , ',field_name,' ', field_type,')');
	PREPARE create_temp from @create_temp;
	EXECUTE create_temp;
    
	WHILE n<=length
    DO
		SET @given_val = SPLIT_STR(input,'),(',n);
		IF n = 1 THEN 
			SET @add_val =  SUBSTR(@given_val, 2 );
			IF length = 1 THEN
            SET @add_val =  SUBSTR(@add_val, 1, LENGTH(@add_val) -1 );
				SET @insert_temp = CONCAT("INSERT INTO ", interm_table_name," (id,",field_name,") VALUES (",n,',\'',@add_val ,"\');");
				PREPARE insert_temp from @insert_temp;
				EXECUTE insert_temp;
            ELSE
				SET @insert_temp = CONCAT("INSERT INTO ", interm_table_name," (id,",field_name,") VALUES (",n,',\'',@add_val ,"\');");
				PREPARE insert_temp from @insert_temp;
				EXECUTE insert_temp;
            END IF;
 		ELSEIF  n = length THEN
            SET @add_val = SUBSTR(@given_val, 1, LENGTH(@given_val) -1); 
			SET @insert_temp = CONCAT("INSERT INTO ", interm_table_name," (id,",field_name,") VALUES (",n,',\'',@add_val ,"\');");
            PREPARE insert_temp from @insert_temp;
			EXECUTE insert_temp;
		ELSE
        	SET @insert_temp = CONCAT("INSERT INTO ", interm_table_name," (id,",field_name,") VALUES (",n,',\'',@given_val ,"\');");
			PREPARE insert_temp from @insert_temp;
			EXECUTE insert_temp;
		END IF;
		SET n = n+1;
    END WHILE;
    
	SET @remove_parenthesis = CONCAT('UPDATE ',interm_table_name,'
    SET ',field_name,' = REPLACE(',field_name,', ")", "")
    WHERE ',field_name,' LIKE "%)"');
    PREPARE remove_parenthesis FROM @remove_parenthesis;
    EXECUTE remove_parenthesis;
    
END //

delimiter ;