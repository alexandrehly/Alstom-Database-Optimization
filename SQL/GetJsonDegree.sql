-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

delimiter //

DROP PROCEDURE IF EXISTS GetJsonDegree//
CREATE PROCEDURE GetJsonDegree(json_data json) 
BEGIN
	SELECT JSON_EXTRACT(json_data, '$."000.001"') INTO @test0;
	IF (@test0 IS NULL) THEN
		SET @degree = 0;
	ELSE 
		SELECT JSON_EXTRACT(json_data, '$."000.001"."000.001.001"') INTO @test1;
		IF (@test1 IS NULL) THEN
			SET @degree = 1;
		ELSE
			SELECT JSON_EXTRACT(json_data, '$."000.001"."000.001.001"."000.001.001.001"') INTO @test_2;
			IF (@test_2 IS NULL) THEN
				SET @degree = 2;
			ELSE 
				SET @degre = 3;
			END IF;
		END IF;
	END IF;

END //

delimiter ;