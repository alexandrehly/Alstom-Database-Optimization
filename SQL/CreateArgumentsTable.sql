-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS CreateArgumentsTable//
CREATE PROCEDURE CreateArgumentsTable(uevol_field_id_list text, src_id_list text, dst_id_list text, filter_list text, temp_nb bigint) 
BEGIN
    
    -- Setting up an argument table
    DECLARE n int unsigned default 0;
	DECLARE temp1 text default CONCAT('data_center.temp1_',temp_nb);
	DECLARE temp2 text default CONCAT('data_center.temp2_',temp_nb);
	DECLARE temp3 text default CONCAT('data_center.temp3_',temp_nb);
	DECLARE temp4 text default CONCAT('data_center.temp4_',temp_nb);
    DECLARE arguments text default CONCAT('data_center.arguments_',temp_nb);
	DECLARE arguments_inter text default CONCAT('data_center.arguments_inter_',temp_nb);
 
 
 -- Calling procedures to generate individual argument tables
    CALL CreateIntermediateTable('t1', temp1 , 'uevol_field_id', 'int' , uevol_field_id_list) ;
	SET @get_count = CONCAT('SELECT COUNT(*) INTO @l FROM ',temp1);
    PREPARE get_count FROM @get_count;
    EXECUTE get_count;
	CALL CreateIntermediateTableInterval(temp2 , 'src_id', 'text' , src_id_list, @l) ;
	CALL CreateIntermediateTableInterval( temp3 , 'dst_id', 'text' , dst_id_list, @l) ;
	CALL CreateIntermediateTable('t4' , temp4 , 'filter', 'text' , filter_list) ;
	
    
-- Joining the individual tables into a main argument table
	SET @drop_table_inter = CONCAT('DROP TABLE IF EXISTS ', arguments_inter);
    PREPARE drop_table_inter from @drop_table_inter;
    EXECUTE drop_table_inter;
    
	SET @get_arguments_inter = CONCAT('SELECT uevol_field_id, src_id, dst_id, filter FROM ', temp1,' t1, ',temp2,' t2, ',temp3,' t3, ',temp4,' t4 
	WHERE t1.id=t2.id AND t1.id = t3.id AND t1.id = t4.id');
    
	SET @create_arguments_table_inter = CONCAT('CREATE TABLE ' ,arguments_inter,' AS (',@get_arguments_inter,')');
	PREPARE create_arguments_table_inter from @create_arguments_table_inter;
	EXECUTE create_arguments_table_inter;
    
	SET @drop_table = CONCAT('DROP TABLE IF EXISTS ', arguments);
    PREPARE drop_table from @drop_table;
    EXECUTE drop_table;
    
	SET @create_arguments_table = CONCAT('CREATE TABLE ', arguments,' (
	  uevol_field_id int(11) ,
	  src_id int,
	  dst_id int,
	  filter text) ');
	PREPARE create_arguments_table from @create_arguments_table;
	EXECUTE create_arguments_table;
    
    SELECT count(name)-1 INTO @last_snap FROM snapshots ;
    
	SET @get_snap_name = CONCAT('SELECT name INTO @snap_name FROM snapshots LIMIT ',@last_snap,',1'); 
	PREPARE get_snap_name FROM @get_snap_name;
	EXECUTE get_snap_name;
    
    -- Loop to fill the argument table layer by layer
    
    WHILE n<@l
    DO

		SET @arg_n = CONCAT('SELECT * FROM ', arguments_inter,' LIMIT ',n,', 1');
		-- PREPARE arg_n FROM @arg_n;
		-- EXECUTE arg_n;
        
		SET @get_arg_lim = CONCAT('SELECT uevol_field_id, src_id,dst_id INTO @uf_id, @src_id, @dst_id FROM (',@arg_n,') arg_n');
        PREPARE get_arg_lim FROM @get_arg_lim;
        EXECUTE get_arg_lim;
        
        IF @last_snap >0 THEN
			SET @arg_line = CONCAT('
			SELECT snap.uevol_field_id, snap.src_id, snap.dst_id, arg_n.filter
			FROM ',@snap_name,' snap
			INNER JOIN (', @arg_n,') arg_n
			ON snap.uevol_field_id = arg_n.uevol_field_id
			WHERE snap.uevol_field_id = ',@uf_id,' AND snap.src_id IN (',@src_id,') AND snap.dst_id IN (',@dst_id,')');
			-- PREPARE arg_line FROM @arg_line;
			-- EXECUTE arg_line;
		ELSE
			SET @arg_line = CONCAT('
			SELECT snap.uevol_field_id, snap.src_id, snap.dst_id, arg_n.filter
			FROM ',@instance_field,' snap
			INNER JOIN (', @arg_n,') arg_n
			ON snap.uevol_field_id = arg_n.uevol_field_id
			WHERE snap.uevol_field_id = ',@uf_id,' AND snap.src_id IN (',@src_id,') AND snap.dst_id IN (',@dst_id,')');
			-- PREPARE arg_line FROM @arg_line;
			-- EXECUTE arg_line;
        END IF;
        
		SET @insert_arg = CONCAT('INSERT INTO ',arguments,' (',@arg_line,')');
        PREPARE insert_arg FROM @insert_arg;
        EXECUTE insert_arg;

		SET n = n+1;
    END WHILE;
    
    SET @arguments = CONCAT('SELECT * FROM ', arguments, ' GROUP BY uevol_field_id, src_id,dst_id');
	-- PREPARE arguments FROM @arguments;
	-- EXECUTE arguments;
    
    -- Dropping intermediate tables
	SET @drop1 = CONCAT(' DROP TABLE IF EXISTS ', temp1);
    PREPARE drop1 FROM @drop1;
    EXECUTE drop1;
	SET @drop2 = CONCAT(' DROP TABLE IF EXISTS ', temp2);
	PREPARE drop2 FROM @drop2;
    EXECUTE drop2;
	SET @drop3 = CONCAT(' DROP TABLE IF EXISTS ', temp3);
    PREPARE drop3 FROM @drop3;
    EXECUTE drop3;
	SET @drop4 = CONCAT(' DROP TABLE IF EXISTS ', temp4);
    PREPARE drop4 FROM @drop4;
    EXECUTE drop4;
	SET @drop_arg_inter = CONCAT(' DROP TABLE IF EXISTS ', arguments_inter);
    PREPARE drop_arg_inter FROM @drop_arg_inter;
    EXECUTE drop_table_inter;
	SET @drop_arg = CONCAT(' DROP TABLE IF EXISTS ', arguments);
    PREPARE drop_arg FROM @drop_arg;
END //

delimiter ;