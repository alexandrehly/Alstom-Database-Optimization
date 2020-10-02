-- Author : Alexandre Ly
-- Date : 28/09/2020
-- Version : 0.1

DROP FUNCTION IF EXISTS SPLIT_STR;
	CREATE FUNCTION SPLIT_STR(
	  x VARCHAR(255),
	  delim VARCHAR(12),
	  pos INT
	)
	RETURNS VARCHAR(255)
	RETURN REPLACE(SUBSTRING(SUBSTRING_INDEX(x, delim, pos),
		   CHAR_LENGTH(SUBSTRING_INDEX(x, delim, pos -1)) + 1),
		   delim, "");
           