#! /bin/bash
hadoop fs -rm -r /user/oracle/temp_out_session
# make the database table
sqlplus marketbasket/welcome1 @transactions.sql

#run the OLH job
hadoop jar ${OLH_HOME}/jlib/oraloader.jar oracle.hadoop.loader.OraLoader -conf /mnt/shared/canopy_clustering/olh/kmeans_job.xml
