-- drop the table if already present
DROP TABLE CUSTOMER_CLUSTERING;

-- this should really be a range partitioned table
CREATE TABLE CUSTOMER_CLUSTERING
(
	"CUSTOMER_ID"	NUMBER,
	"X"		NUMBER,
	"Y"		NUMBER,
	"Z"		NUMBER
)
PARTITION BY HASH(CUSTOMER_ID);
QUIT;