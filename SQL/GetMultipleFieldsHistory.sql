-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS GetMultipleFieldsHistory//
CREATE PROCEDURE GetMultipleFieldsHistory(database_name text, instance_message_id bigint, uevol_field_id_list text, src_id_list text, dst_id_list text, filter_list text, replay boolean, temp_nb bigint) 
BEGIN
    
	IF replay = 1 THEN
		SET @instance_message = 'instance_message_replay';
        SET @instance_field = 'instance_field_replay';
	ELSE 
		SET @instance_message = 'instance_message';
        SET @instance_field = 'instance_field';
    END IF;

    -- Calling the arguments procedure to create the table containing the arguments
    CALL CreateArgumentsTable(uevol_field_id_list , src_id_list, dst_id_list,filter_list, temp_nb );
    
-- Getting the fields name for latter joins
	SET @get_wanted_fields = CONCAT( 'SELECT id as uevol_field_id, type, name FROM ', database_name, '.uevol_field WHERE id IN (SELECT uevol_field_id FROM (' , @arguments, ') arg)');
	-- PREPARE get_wanted_fields FROM @get_wanted_fields;
	-- EXECUTE get_wanted_fields;
    
    SET @wanted_fields = CONCAT('data_center.wanted_fields_', temp_nb);
    
	SET @drop_wanted_fields = CONCAT('DROP TABLE IF EXISTS ',@wanted_fields); 
	PREPARE drop_wanted_fields FROM @drop_wanted_fields;
	EXECUTE drop_wanted_fields;
    
    SET @create_wanted_fields = CONCAT('CREATE TABLE ',@wanted_fields,' SELECT * FROM (',@get_wanted_fields,') wf');
    PREPARE create_wanted_fields FROM @create_wanted_fields;
	EXECUTE create_wanted_fields;

-- Filtering the nearest snapshots for wanted fields and concatenating them------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     
     SET @snapshots = CONCAT(database_name,'.snapshots');

		-- Getting nearest snapshot names
		SET @get_snap_min_name = CONCAT(
		'SELECT name, start_instance_message_id INTO @snap_min_name, @snap_min_start FROM ',@snapshots, ' 
			WHERE start_instance_message_id = (
			SELECT max(start_instance_message_id)
			FROM ', @snapshots, ' 
			WHERE start_instance_message_id <= ', instance_message_id, ')' );
		PREPARE get_snap_min_name FROM @get_snap_min_name;
		EXECUTE get_snap_min_name;

        SET @snap_max_name = @snap_min_name;
        
        IF instance_message_id = 0 THEN
				SET @get_snap_max_name = CONCAT('SELECT name INTO @snap_max_name FROM ', @snapshots, ' 
				WHERE start_instance_message_id = (
				SELECT min(start_instance_message_id)
				FROM ', @snapshots, ' 
				WHERE start_instance_message_id > ',instance_message_id,')');
        ELSE
			SET @get_snap_max_name = CONCAT('SELECT name INTO @snap_max_name FROM ', @snapshots, ' 
				WHERE start_instance_message_id = (
				SELECT min(start_instance_message_id)
				FROM ', @snapshots, ' 
				WHERE start_instance_message_id >= ',instance_message_id,')');
        END IF;    
			PREPARE get_snap_max_name FROM @get_snap_max_name;
			EXECUTE get_snap_max_name;

		IF @snap_min_name != @snap_max_name THEN
        
			-- Getting the nearest napshots
			SET @snap_min = CONCAT(database_name,'.',@snap_min_name);
			SET @snap_max = CONCAT(database_name,'.',@snap_max_name);

	   -- Getting the nearest napshots and filtering by arguments
			SET @get_min = CONCAT('
				SELECT min.uevol_field_id, min.src_id, min.dst_id, min.instance_message_id, min.json_value 
				FROM ',@snap_min,' min 
				INNER JOIN (', @arguments,') arg
				ON min.src_id = arg.src_id AND min.dst_id = arg.dst_id AND min.uevol_field_id = arg.uevol_field_id');
			  -- PREPARE get_min FROM @get_min;
			  -- EXECUTE get_min;

			SET @snap_min = CONCAT('data_center.snap_min_',temp_nb);
			
			SET @drop_snap_min = CONCAT('DROP TABLE IF EXISTS ', @snap_min ); 
			PREPARE drop_snap_min FROM @drop_snap_min;
			EXECUTE drop_snap_min;
			
			SET @create_snap_min = CONCAT('CREATE TABLE ',@snap_min,' SELECT * FROM(',@get_min,') ga');
			PREPARE create_snap_min FROM @create_snap_min;
			EXECUTE create_snap_min;
				
			SET @get_max = CONCAT('	
				SELECT max.uevol_field_id, max.src_id, max.dst_id, max.instance_message_id, max.json_value 
				FROM ',@snap_max,' max
				INNER JOIN (', @arguments,') arg
				ON max.src_id = arg.src_id AND max.dst_id = arg.dst_id AND max.uevol_field_id = arg.uevol_field_id');
			-- PREPARE get_max FROM @get_max;
			-- EXECUTE get_max;
		
			-- Getting the concatenation of both snapshots
			SET @get_all = CONCAT('SELECT * FROM ',@snap_min,' UNION ALL ',@get_max);
			-- PREPARE get_all FROM @get_all;
			-- EXECUTE get_all;
			
			-- Creating a table to store it 
			SET @conc_snap = CONCAT('data_center.conc_snap_',temp_nb);
			
			SET @drop_conc_snap = CONCAT('DROP TABLE IF EXISTS ', @conc_snap ); 
			PREPARE drop_conc_snap FROM @drop_conc_snap;
			EXECUTE drop_conc_snap;
			
			SET @create_conc_snap = CONCAT('CREATE TABLE ',@conc_snap,' SELECT * FROM(',@get_all,') ga');
			PREPARE create_conc_snap FROM @create_conc_snap;
			EXECUTE create_conc_snap;
		 
	-- Getting the values of the fields not changing in between the snapshots ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

		-- Getting the value of fields which didn't change between snapshots by counting
		-- if in the concatenated table the field value/instance__message_id appears twice it means no update
			SET @compare_same = CONCAT(
			'SELECT *
			FROM ', @conc_snap,'  
			GROUP BY uevol_field_id, src_id, dst_id, instance_message_id
			HAVING COUNT(*) > 1 '); 
			-- PREPARE compare_same FROM @compare_same;
			-- EXECUTE compare_same;
			
			SET @same = CONCAT('data_center.same_',temp_nb);
			
			SET @drop_same = CONCAT('DROP TABLE IF EXISTS ', @same ); 
			PREPARE drop_same FROM @drop_same;
			EXECUTE drop_same;
			
			SET @create_same = CONCAT('CREATE TABLE ',@same,' SELECT * FROM(',@compare_same,') ga');
			PREPARE create_same FROM @create_same;
			EXECUTE create_same;

	-- Getting the values of fields updated between the snapshots------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

		-- Getting the value of fields which changed between snapshots by counting
		-- if in the concatenated table the field value/instance__message_id appears only once it means there was an update
		SET @compare_diff = CONCAT('
		SELECT *
		FROM ',@conc_snap,'
		GROUP BY uevol_field_id, src_id, dst_id, instance_message_id
		HAVING COUNT(*) =1 
		ORDER BY uevol_field_id'); 
		-- PREPARE compare_diff FROM @compare_diff;
		-- EXECUTE compare_diff;

		SET @instance_field_reduced = CONCAT('
			SELECT * FROM ',@instance_field,'  
			WHERE instance_message_id<=', instance_message_id, ' AND instance_message_id>',@snap_min_start );

		-- Accessing instance_field_replay to get the updates for the given fields
		SET @get_diff_update_prem = CONCAT(
		'SELECT in_fr.uevol_field_id,in_fr.src_id, in_fr.dst_id, in_fr.instance_message_id, in_fr.relative_path, in_fr.value
		FROM  (',@instance_field_reduced,') in_fr 
		INNER JOIN (',@compare_diff,') cd 
		ON in_fr.uevol_field_id = cd.uevol_field_id AND in_fr.src_id = cd.src_id AND in_fr.dst_id = cd.dst_id');

		SET @diff_update_prem = CONCAT('data_center.diff_update_prem_',temp_nb);
		
		SET @drop_diff_update_prem = CONCAT('DROP TABLE IF EXISTS ', @diff_update_prem ); 
		PREPARE drop_diff_update_prem FROM @drop_diff_update_prem;
		EXECUTE drop_diff_update_prem;
		
		SET @create_diff_update_prem = CONCAT('CREATE TABLE ',@diff_update_prem,' SELECT * FROM(',@get_diff_update_prem,') gdup');
		PREPARE create_diff_update_prem FROM @create_diff_update_prem;
		EXECUTE create_diff_update_prem;
		
		SET @get_diff_recent_update = CONCAT('SELECT prem1.uevol_field_id,prem1.src_id, prem1.dst_id, prem1.instance_message_id, prem1.relative_path, prem2.value
		FROM (
			SELECT uevol_field_id,src_id, dst_id, max(instance_message_id) as instance_message_id, relative_path
			FROM ',@diff_update_prem,' 
			GROUP BY uevol_field_id, src_id, dst_id, relative_path) prem1
		RIGHT OUTER JOIN  ',@diff_update_prem,' prem2
		ON prem1.uevol_field_id=prem2.uevol_field_id 
			AND prem1.src_id = prem2.src_id 
			AND prem1.dst_id = prem2.dst_id 
			AND prem1.instance_message_id = prem2.instance_message_id 
			AND prem1.relative_path = prem2.relative_path
		WHERE prem1.uevol_field_id IS NOT NULL');
		-- PREPARE get_diff_recent_update FROM @get_diff_recent_update;
		-- EXECUTE get_diff_recent_update;
		
		SET @diff_update_rec = CONCAT('data_center.diff_update_rec_',temp_nb);
		
		SET @drop_diff_update_rec = CONCAT('DROP TABLE IF EXISTS ', @diff_update_rec ); 
		PREPARE drop_diff_update_rec FROM @drop_diff_update_rec;
		EXECUTE drop_diff_update_rec;
		
		SET @create_diff_update_rec = CONCAT('CREATE TABLE ',@diff_update_rec,' SELECT * FROM(',@get_diff_recent_update,') ga');
		PREPARE create_diff_update_rec FROM @create_diff_update_rec;
		EXECUTE create_diff_update_rec;

		-- Joining with Uevol_field to get field names and keep the last update
		SET @get_diff = CONCAT( 'SELECT upd.uevol_field_id, upd.src_id, upd.dst_id , wf.name, upd.instance_message_id, upd.relative_path, wf.type, upd.value
		FROM ',@diff_update_rec,' upd 
		INNER JOIN ',@wanted_fields,' wf
		ON upd.uevol_field_id = wf.uevol_field_id 
		ORDER BY uevol_field_id, relative_path');
		PREPARE get_diff FROM @get_diff;
		-- EXECUTE get_diff;
		
		SET @diff = CONCAT('data_center.diff_',temp_nb);
		
		SET @drop_diff = CONCAT('DROP TABLE IF EXISTS ', @diff ); 
		PREPARE drop_diff FROM @drop_diff;
		EXECUTE drop_diff;
		
		SET @create_diff = CONCAT('CREATE TABLE ',@diff,' SELECT * FROM(',@get_diff,') ga');
		PREPARE create_diff FROM @create_diff;
		EXECUTE create_diff;
		
	-- Getting the fields which change between the snapshots but have yet to be updated at the given moment------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

		-- Get all other fields
		Set @known_fields = CONCAT('
		SELECT uevol_field_id, src_id, dst_id, instance_message_id FROM ',@same,' 
		UNION ALL
		SELECT uevol_field_id, src_id, dst_id, instance_message_id FROM ',@diff);
		-- PREPARE known_fields FROM @known_fields;
		-- EXECUTE known_fields;
		
		-- Getting the fields which changed between snapshots but have yet to be updated at the time
		-- We join the result of finding the updates and the fields which changed and see which were not found 
		SET @get_no_update_yet = CONCAT('
		SELECT arg.uevol_field_id, arg.src_id, arg.dst_id
		FROM (',@arguments,') arg
		LEFT OUTER JOIN (',@known_fields,') kf
		ON arg.uevol_field_id=kf.uevol_field_id AND arg.src_id= kf.src_id AND arg.dst_id = kf.dst_id
		WHERE kf.instance_message_id IS NULL');
		-- PREPARE no_update_yet FROM @no_update_yet;
		-- EXECUTE no_update_yet;
		
		SET @compare_no_update_yet = CONCAT(
		'SELECT nuy.uevol_field_id, nuy.src_id, nuy.dst_id, gm.instance_message_id, gm.json_value 
		FROM (',@get_no_update_yet , ') nuy 
		LEFT OUTER JOIN ',@conc_snap,' gm
		ON nuy.uevol_field_id=gm.uevol_field_id AND nuy.src_id= gm.src_id AND nuy.dst_id = gm.dst_id'); 
		-- PREPARE compare_no_update_yet FROM @compare_no_update_yet;
		-- EXECUTE compare_no_update_yet;

		SET @no_update_yet = CONCAT('data_center.no_update_yet_',temp_nb);
		
		SET @drop_no_update_yet = CONCAT('DROP TABLE IF EXISTS ', @no_update_yet ); 
		PREPARE drop_no_update_yet FROM @drop_no_update_yet;
		EXECUTE drop_no_update_yet;
		
		SET @create_no_update_yet = CONCAT('CREATE TABLE ',@no_update_yet,' SELECT * FROM (',@compare_no_update_yet,') ga');
		PREPARE create_no_update_yet FROM @create_no_update_yet;
		EXECUTE create_no_update_yet;
			  
	-- Concatenating the unchanged fields and fetching all fields------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

		-- We concatenate the fields which stayed the same and those which have yet to be updated
		SET @get_unchanged_prem = CONCAT( 'SELECT * FROM ',@same,' UNION ALL SELECT * FROM ',@no_update_yet);
		-- PREPARE unchanged FROM @unchanged;
	  
		SET @unchanged_prem = CONCAT('data_center.unchanged_prem_',temp_nb);

		SET @drop_unchanged_prem = CONCAT('DROP TABLE IF EXISTS ', @unchanged_prem );
		PREPARE drop_unchanged_prem FROM @drop_unchanged_prem;
		EXECUTE drop_unchanged_prem;

		SET @create_unchanged_prem = CONCAT('CREATE TABLE ',@unchanged_prem,' SELECT * FROM (',@get_unchanged_prem,') gu');
		PREPARE create_unchanged_prem FROM @create_unchanged_prem;
		EXECUTE create_unchanged_prem;
	  
		-- Extract json values to concatenate
		SET @get_unchanged = CONCAT('
			SELECT wf.uevol_field_id, src_id, dst_id, name, instance_message_id, \'000\' as relative_path, wf.type, JSON_EXTRACT(json_value,\'$."0"\')+0 as value 
			FROM ',@unchanged_prem,' u
			JOIN ',@wanted_fields,' wf
			WHERE wf.uevol_field_id = u.uevol_field_id ');
		-- PREPARE get_unchanged FROM @get_unchanged;
		-- EXECUTE get_unchanged;
				 
		SET @unchanged = CONCAT('data_center.unchanged_',temp_nb);

		SET @drop_unchanged = CONCAT('DROP TABLE IF EXISTS ', @unchanged );
		PREPARE drop_unchanged FROM @drop_unchanged;
		EXECUTE drop_unchanged;

		SET @create_unchanged = CONCAT('CREATE TABLE ',@unchanged,' SELECT * FROM (',@get_unchanged,') gu');
		PREPARE create_unchanged FROM @create_unchanged;
		EXECUTE create_unchanged;
		
		-- We concatenate everything 
		SET @get_everything = CONCAT('SELECT * FROM ',@unchanged,' UNION ALL SELECT * FROM ',@diff);
		PREPARE get_everything FROM @get_everything;
		-- EXECUTE get_everything;
    
    ELSE
			SET @instance_field_reduced = CONCAT('
			SELECT uevol_field_id, src_id, dst_id, instance_message_id, relative_path, value 
			FROM ',@instance_field,'  
			WHERE instance_message_id >= ', @snap_min_start,' 
				AND instance_message_id<=', instance_message_id, ' 
				AND src_id=',@src_id,' 
				AND dst_id = ',@dst_id );
			-- PREPARE instance_field_reduced FROM @instance_field_reduced;
			-- EXECUTE instance_field_reduced;
		 
			SET @get_diff_update_prem = CONCAT('SELECT in_fr.uevol_field_id, in_fr.src_id, in_fr.dst_id, in_fr.instance_message_id, in_fr.relative_path, in_fr.value
				FROM  (',@instance_field_reduced,') in_fr 
				INNER JOIN ',@wanted_fields,' wf 
				ON in_fr.uevol_field_id = wf.uevol_field_id');
            -- SELECT @get_diff_update_prem;
            				
			SET @diff_update_prem = CONCAT('data_center.diff_update_prem_',temp_nb);
	
			SET @drop_diff_update_prem = CONCAT('DROP TABLE IF EXISTS ', @diff_update_prem ); 
			PREPARE drop_diff_update_prem FROM @drop_diff_update_prem;
			EXECUTE drop_diff_update_prem;

			SET @create_diff_update_prem = CONCAT('CREATE TABLE ',@diff_update_prem,' ',@get_diff_update_prem);
			PREPARE create_diff_update_prem FROM @create_diff_update_prem;
			EXECUTE create_diff_update_prem;
   
			-- Accessing instance_field_replay to get the updates for the given fields
			SET @get_diff_recent_update = CONCAT('SELECT prem1.uevol_field_id,prem1.src_id, prem1.dst_id, prem1.instance_message_id, prem1.relative_path, prem2.value
			FROM (
				SELECT uevol_field_id,src_id, dst_id, max(instance_message_id) as instance_message_id, relative_path
				FROM ',@diff_update_prem,' 
				GROUP BY uevol_field_id, relative_path) prem1
			RIGHT OUTER JOIN  ',@diff_update_prem,' prem2
			ON prem1.uevol_field_id=prem2.uevol_field_id 
				AND prem1.instance_message_id = prem2.instance_message_id 
				AND prem1.relative_path = prem2.relative_path
			WHERE prem1.uevol_field_id IS NOT NULL');
			-- PREPARE get_diff_recent_update FROM @get_diff_recent_update;
			-- EXECUTE get_diff_recent_update;
			
			SET @diff_update_rec = CONCAT('data_center.diff_update_rec_',temp_nb);
			
			SET @drop_diff_update_rec = CONCAT('DROP TABLE IF EXISTS ', @diff_update_rec ); 
			PREPARE drop_diff_update_rec FROM @drop_diff_update_rec;
			EXECUTE drop_diff_update_rec;
			
			SET @create_diff_update_rec = CONCAT('CREATE TABLE ',@diff_update_rec,' ',@get_diff_recent_update);
			PREPARE create_diff_update_rec FROM @create_diff_update_rec;
			EXECUTE create_diff_update_rec;
			
			-- Joining with Uevol_field to get field names and keep the last update
			SET @get_everything = CONCAT('SELECT DISTINCT upd.uevol_field_id, upd.src_id, upd.dst_id , wf.name, upd.instance_message_id, upd.relative_path, wf.type, upd.value
			FROM ',@diff_update_rec,' upd 
			INNER JOIN ',@wanted_fields,' wf
			ON upd.uevol_field_id = wf.uevol_field_id 
			ORDER BY uevol_field_id, relative_path');
			PREPARE get_everything FROM @get_everything;
			-- EXECUTE get_everything;
    END IF;

-- Initializing the start table for GetUpdateHistory------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

	-- Creating the start table
	SET @start_table = CONCAT('data_center.start_table_', temp_nb);
    
	SET @drop_start_table = CONCAT('DROP TABLE IF EXISTS ', @start_table);
	PREPARE drop_start_table FROM @drop_start_table;
	EXECUTE drop_start_table;
    
	SET @create_start_table = CONCAT( "CREATE TABLE ",@start_table," (SELECT uevol_field_id, src_id, dst_id, value FROM (", @get_everything,") ge ) ");
	PREPARE create_start_table from @create_start_table;
	EXECUTE create_start_table;
    
 -- Dropping intermediate tables------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   
		EXECUTE drop_wanted_fields; 
	EXECUTE drop_snap_min; 
    EXECUTE drop_conc_snap;
    EXECUTE drop_same;
	EXECUTE drop_diff_update_prem;
	EXECUTE drop_diff_update_rec;
    EXECUTE drop_diff;
    EXECUTE drop_no_update_yet;
    EXECUTE drop_unchanged_prem;
	EXECUTE drop_unchanged;


END //

delimiter ;