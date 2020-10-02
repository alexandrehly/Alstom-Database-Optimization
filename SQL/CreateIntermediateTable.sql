-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS CreateIntermediateTable//
CREATE PROCEDURE CreateIntermediateTable(first_table_name text, interm_table_name text, field_name text, field_type text, input text) 
BEGIN	
    
    DROP TABLE IF EXISTS first_table_name;
	CREATE TABLE first_table_name( field_name text );
	INSERT INTO first_table_name values(input);
    
    SET @drop_temp = CONCAT('DROP TABLE IF EXISTS ',interm_table_name);
	PREPARE drop_temp from @drop_temp;
	EXECUTE drop_temp;
    
	SET @create_temp = CONCAT( 'CREATE TABLE ',interm_table_name,' (id int PRIMARY KEY AUTO_INCREMENT, ',field_name,' ', field_type,')');
	PREPARE create_temp from @create_temp;
	EXECUTE create_temp;
    
    SET @get_values = CONCAT('SELECT REPLACE(( SELECT group_concat(DISTINCT field_name) AS DATA FROM first_table_name) , ",", "),(") INTO @vals');
	PREPARE get_values from @get_values;
	EXECUTE get_values;
    
	SET @insert_temp = CONCAT("INSERT INTO ", interm_table_name," (",field_name,") VALUES (", @vals ,");");
    PREPARE insert_temp from @insert_temp;
	EXECUTE insert_temp;
    
	DROP TABLE IF EXISTS first_table_name;
    
END //

delimiter ;