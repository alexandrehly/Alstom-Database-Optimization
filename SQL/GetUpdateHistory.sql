-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS GetUpdateHistory//
CREATE PROCEDURE GetUpdateHistory(database_name text, instance_message_id_start bigint, instance_message_id_end bigint, uevol_field_id_list text, src_id_list text, dst_id_list text, filter_list text, replay boolean, temp_nb bigint) 
BEGIN

-- Defining variables for teh following procedure
	DECLARE n int unsigned default 0;
    DECLARE m int unsigned default 1;
	DECLARE l int unsigned default 0;
    DECLARE k int unsigned default 0;
	SET @results_name = CONCAT('data_center.results_',temp_nb);
	SET @results_filtered_name = CONCAT('data_center.results_filtered_',temp_nb);
	SET @snapshots = CONCAT(database_name,'.snapshots');
    
	IF replay = 1 THEN
		SET @instance_message = 'instance_message_replay';
        SET @instance_field = 'instance_field_replay';
	ELSE 
		SET @instance_message = 'instance_message';
        SET @instance_field = 'instance_field';
    END IF;

-- Calling the procedure to get the initial values of the wanted fields -----------------------------------------------------------------------------------------------------------------------------------------------------------
	CALL GetMultipleFieldsHistory(database_name, instance_message_id_start,  uevol_field_id_list, src_id_list, dst_id_list, filter_list , replay , temp_nb);

	SET @get_nb_fields = CONCAT('SELECT COUNT(*) INTO @Nb_fields FROM (', @arguments,') arg');
	 PREPARE get_nb_fields FROM @get_nb_fields;
	 EXECUTE get_nb_fields;

-- Creating the results table and inititalizing it ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	SET @make_cols = CONCAT("SELECT CONCAT('id',uevol_field_id, '_src', src_id, '_dst', dst_id, ' BigInt(20)') as mc FROM (", @arguments,') arg');
	-- PREPARE make_cols from @make_cols;
	-- EXECUTE make_cols;
	
    SET @str_table = CONCAT('data_center.str_table_', temp_nb);
    
	SET @drop_str_table = CONCAT('DROP TABLE IF EXISTS ', @str_table);
	PREPARE drop_str_table FROM @drop_str_table;
	EXECUTE drop_str_table;
    
	SET @create_str_table = CONCAT('CREATE TABLE ',@str_table,'  (',@make_cols,')');
	PREPARE create_str_table from @create_str_table;
	EXECUTE create_str_table;


    SET @results_prem = CONCAT('data_center.results_prem_', temp_nb);
    
	SET @drop_results_prem = CONCAT('DROP TABLE IF EXISTS ', @results_prem);
	PREPARE drop_results_prem FROM @drop_results_prem;
	EXECUTE drop_results_prem;
    
	SET @create_results_prem = CONCAT( "CREATE TABLE ",@results_prem," SELECT GROUP_CONCAT(distinct mc) as data FROM ",@str_table);
    PREPARE create_results_prem from @create_results_prem;
	EXECUTE create_results_prem;
    
    
	SET @update_results_prem = CONCAT( "UPDATE ",@results_prem," SET data = REPLACE(data, \',\', \', \')" );
	PREPARE update_results_prem from @update_results_prem;
	EXECUTE update_results_prem;
    
	SET @drop_results = CONCAT('DROP TABLE IF EXISTS ', @results_name);
	PREPARE drop_results FROM @drop_results;
	EXECUTE drop_results;
    
    
    SET @get_data= CONCAT(' SELECT data INTO @data FROM ',@results_prem);
	PREPARE get_data from @get_data;
	EXECUTE get_data;
    
    
	SET @create_results = CONCAT( "CREATE TABLE ",@results_name," (instance_message_id BigInt(20) PRIMARY KEY ,", @data ,")");
    PREPARE create_results from @create_results;
	EXECUTE create_results;
    
	SET @init_results = CONCAT( "INSERT INTO ",@results_name," (instance_message_id ) VALUES (", instance_message_id_start,")");
	PREPARE init_results from @init_results;
	EXECUTE init_results;
    
	SET @start_conc = CONCAT(" SELECT CONCAT(\'id\',uevol_field_id, \'_src\', src_id, \'_dst\', dst_id) AS cols, value FROM ",@start_table);
    
	WHILE n<@Nb_fields
    DO
		 SET @get_val = CONCAT('SELECT cols, value INTO @c,@v FROM (',@start_conc,') sc LIMIT ',n,', 1');
         PREPARE get_val FROM @get_val;
         EXECUTE get_val;
         IF @v IS NOT NUll THEN
				SET @upd_results = CONCAT( "UPDATE ",@results_name," SET ",@c,"=",@v," WHERE instance_message_id = ", instance_message_id_start);
				PREPARE upd_results from @upd_results;
				EXECUTE upd_results;
		END IF;
        SET n=n+1;
	END WHILE;
    
    SET @prev_id = instance_message_id_start;

-- Finding out the number of updates over the time window------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	    
	SET @get_update_times = CONCAT('
		SELECT if_r.uevol_field_id, if_r.src_id, if_r.dst_id, if_r.instance_message_id, if_r.relative_path, if_r.value
		FROM ', database_name,'.',@instance_field,' if_r
        INNER JOIN (', @arguments,') arg
		ON if_r.src_id = arg.src_id AND if_r.dst_id = arg.dst_id AND if_r.uevol_field_id = arg.uevol_field_id
		WHERE instance_message_id>=', instance_message_id_start,' 
			AND instance_message_id<=', instance_message_id_end,'
        ORDER BY if_r.instance_message_id');
	-- PREPARE get_update_times FROM @get_update_times;
    -- EXECUTE get_update_times;
    
    SET @update_times = CONCAT('data_center.update_times_', temp_nb);
    
    SET @drop_update_times = CONCAT('DROP TABLE IF EXISTS ', @update_times);
    PREPARE drop_update_times FROM @drop_update_times;
    EXECUTE drop_update_times;
    
    SET @create_update_times = CONCAT('CREATE TABLE ',@update_times,' ',@get_update_times);
	PREPARE create_update_times FROM @create_update_times;
    EXECUTE create_update_times;
    
	SET @COUNT = CONCAT('SELECT COUNT( DISTINCT instance_message_id) INTO @X FROM ',@update_times,' up_t');
    PREPARE COUNT FROM @COUNT;
	EXECUTE COUNT;
	-- SELECT @X;
    
    SET @update_times_conc = CONCAT('SELECT CONCAT(\'id\',uevol_field_id, \'_src\', src_id, \'_dst\', dst_id) AS cols, instance_message_id, value FROM ',@update_times,' up_t');
    
	SET @get_prev_row_cols = CONCAT('SELECT GROUP_CONCAT(SUBSTRING(mc, 1, LENGTH(mc) - 11)) INTO @prev_row_cols FROM ',@str_table);
	PREPARE get_prev_row_cols FROM @get_prev_row_cols;
	EXECUTE get_prev_row_cols;
    -- SELECT @prev_row_cols;
 -- Double loop to copy the previous row on the results table and only update the required fields------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	WHILE m<@X
    DO
		SET @get_curr_id = CONCAT('SELECT DISTINCT instance_message_id INTO @curr_id FROM ',@update_times,' up_t LIMIT ', m,', 1');
		PREPARE get_curr_id from @get_curr_id;
		EXECUTE get_curr_id;
        -- SELECT @curr_id;

		SET @prev_row = CONCAT('data_center.prev_row_',temp_nb);
        
		SET @drop_prev_row = CONCAT('DROP TABLE IF EXISTS ', @prev_row);
		PREPARE drop_prev_row FROM @drop_prev_row;
		EXECUTE drop_prev_row;
        
		SET @create_prev_row = CONCAT('CREATE TABLE ',@prev_row,' AS SELECT ',@curr_id, ' as instance_message_id,',@prev_row_cols,' FROM ', @results_name,' WHERE instance_message_id = ',@prev_id);
        PREPARE create_prev_row FROM @create_prev_row;
		EXECUTE create_prev_row;

		SET @set_curr_row = CONCAT( 'INSERT INTO ',@results_name,' (instance_message_id,',@prev_row_cols,') SELECT instance_message_id,',@prev_row_cols,' FROM ',@prev_row,' WHERE instance_message_id = ', @curr_id);
        PREPARE set_curr_row from @set_curr_row;
		EXECUTE set_curr_row;
        
        SET @curr_updates = CONCAT('SELECT * FROM (',@update_times_conc,') up_t_c WHERE instance_message_id = ', @curr_id);
        SET @get_curr_updates_count = CONCAT('SELECT COUNT(*) INTO @curr_updates_count FROM (',@curr_updates,') cu');
        PREPARE get_curr_updates_count FROM @get_curr_updates_count;
        EXECUTE get_curr_updates_count;
        
			WHILE l<@curr_updates_count
			DO
				SET @get_curr_val = CONCAT('SELECT cols, value INTO @c,@v FROM (',@curr_updates,') sc LIMIT ',l,', 1');
				PREPARE get_curr_val FROM @get_curr_val;
				EXECUTE get_curr_val;
                
				SET @set_curr_value = CONCAT( "UPDATE ",@results_name," SET ",@c,"=",@v," WHERE instance_message_id = ", @curr_id);
				PREPARE set_curr_value from @set_curr_value;
				EXECUTE set_curr_value;
				SET l=l+1;
			END WHILE;
            
		SET @prev_id = @curr_id;
        SET m=m+1;
		SET l = 0;
	END WHILE;
    
-- Creating the filtering query progressively ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	SET @filters = CONCAT('data_center.filters_',temp_nb);
        
	SET @drop_filters = CONCAT('DROP TABLE IF EXISTS ', @filters);
	PREPARE drop_filters FROM @drop_filters;
	EXECUTE drop_filters;

    SET @create_filters = CONCAT( "CREATE TABLE ",@filters,"  SELECT * FROM (", @arguments,") arg WHERE filter !=\'\' ");
	PREPARE create_filters from @create_filters;
	EXECUTE create_filters;

    SET @get_nb_filters = CONCAT('SELECT COUNT(*) INTO @nb_filters FROM ', @filters);
	PREPARE get_nb_filters from @get_nb_filters;
	EXECUTE get_nb_filters;
    -- SELECT @nb_filters;
    
	SET @drop_results_filtered = CONCAT('DROP TABLE IF EXISTS ', @results_filtered_name);
	PREPARE drop_results_filtered FROM @drop_results_filtered;
	EXECUTE drop_results_filtered;
    
	SET @filtering = CONCAT('SELECT * FROM ',@results_name);
    
    -- Testing if there is at least one filter
    IF @nb_filters > 0 THEN 
		SET @filtering = CONCAT(@filtering, ' WHERE ');

		WHILE k<@nb_filters
		DO
        
			SET @get_filter_arg = CONCAT('SELECT uevol_field_id,src_id,dst_id INTO @current_id, @current_src, @current_dst FROM ',@filters,' LIMIT ',k,', 1');
			PREPARE get_filter_arg FROM @get_filter_arg;
			EXECUTE get_filter_arg;
            
			SET @update_filters = CONCAT( "UPDATE ",@filters,"  SET filter = REPLACE(filter, 'value', 'id",@current_id,"_src",@current_src,"_dst",@current_dst,"')  
            WHERE uevol_field_id = ",@current_id," AND src_id =",@current_src," AND dst_id =",@current_dst );
			PREPARE update_filters from @update_filters;
			EXECUTE update_filters;
    
			SET @get_filter = CONCAT('SELECT filter INTO @current_filter FROM ',@filters,' LIMIT ',k,', 1');
			PREPARE get_filter FROM @get_filter;
			EXECUTE get_filter;
    
			SET @filtering = CONCAT(@filtering, @current_filter);
			SET k=k+1;
            IF  k<@nb_filters THEN
				SET @filtering = CONCAT(@filtering, ' AND ');
            END IF;
		END WHILE;
		
		
    END IF;
	-- SELECT @filtering;
	SET @create_results_filtered = CONCAT( "CREATE TABLE ",@results_filtered_name, " ", @filtering );
    PREPARE create_results_filtered from @create_results_filtered;
	EXECUTE create_results_filtered;
    
-- Dropping all the intermediate tables------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   
   -- Dropping the arguments table
	 EXECUTE drop_arg;
	
    -- Dropping the start table
	 EXECUTE drop_start_table;
    
	-- Dropping the results table
    EXECUTE drop_results_prem;
	EXECUTE drop_results;
    
	EXECUTE drop_str_table;
    EXECUTE drop_prev_row;
	EXECUTE drop_update_times;
    EXECUTE drop_filters;

END //

delimiter ;