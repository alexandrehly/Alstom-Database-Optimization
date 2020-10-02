package org.alstom.jupiter.utilities.SparkCassandra;

import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.SparkSession;
import org.alstom.jupiter.utilities.SparkCassandra.DataLoader.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RetrieveField {
	public static void main (String[] args) {
		
//		@SuppressWarnings("unused")
//		SparkSession spark = SparkSession
//				  .builder()
//				  .appName("RetrieveField")
//		          .config("spark.cassandra.connection.host","10.22.14.174")
//		          .config("spark.cassandra.connection.port", "9042")
//		          .config("org.apache.cassandra.auth.user_name", "cassandra")
//		          .config("org.apache.cassandra.auth.password", "cassandra")
//				  .getOrCreate();
		
		//Loading the needed tables
		String keyspaceName = "fluence_lille_1311beta29_site_i_62c3";
		Dataset<InstanceField> instanceField = DataLoader.loadInstanceField(keyspaceName);
		Dataset<InstanceMessage> instanceMessage = DataLoader.loadInstanceMessage(keyspaceName);
		

		//Defining the arguments for field research
	    int uevol_message_id = 40;
	    int uevol_field_id = 7196;
	    int src_ty = 34;
	    int src_id = 13;
	    int dst_ty = 35;
	    int dst_id = 11;
	    int seq_nb = -1;
	    long start_log_time = 1581734409340L; //1581734409341L
	    long end_log_time = -1L;
	    long duration = 1000L;
	    int chosen_value = -1;
	    
	    long timeStart = System.currentTimeMillis();
	    
	    //Calling the field filtering function   
	    Dataset<InstanceField> instanceFieldSelected = fieldFilter(instanceField, instanceMessage, uevol_message_id ,uevol_field_id,  src_id,  dst_id, src_ty, dst_ty, seq_nb,  start_log_time,  end_log_time,  duration,  chosen_value) ;		
	    instanceFieldSelected.show();
	    System.out.println(("Temps de calcul: "+((System.currentTimeMillis()-timeStart)/1000)));
	}

	//Defining the Field filtering function
	static Dataset<InstanceField> fieldFilter(Dataset<InstanceField> instanceField, Dataset<InstanceMessage> instanceMessage,int uevol_message_id,int uevol_field_id, int src_id, int dst_id, int src_ty, int dst_ty,int seq_nb, Long start_log_time, long end_log_time, long duration, long chosen_value) {
		
		//Loading the tables
		Dataset<InstanceField> instanceFieldSelected = instanceField;
		Dataset<InstanceMessage> instanceMessageSelected = instanceMessage;
		
		//Filtering along the arguments
		if (uevol_field_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("uevol_field_id=="+uevol_field_id);}
		if (chosen_value!= -1){instanceFieldSelected=instanceFieldSelected.filter("new_value=="+chosen_value);}
        if (src_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("src_id=="+src_id);}
        if (dst_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("dst_id=="+dst_id);}
        if (uevol_message_id!= -1){instanceMessageSelected=instanceMessageSelected.filter("uevol_message_id=="+uevol_message_id);}
		if (src_ty!= -1){instanceMessageSelected=instanceMessageSelected.filter("src_ty=="+src_ty);}
		if (dst_ty!= -1){instanceMessageSelected=instanceMessageSelected.filter("dst_ty=="+dst_ty);}
        
		//if sequence number is given the we can find the exact message the field belongs to.
		if (seq_nb != -1){
			int instance_message_id = instanceMessage.filter("seq_nb=="+seq_nb).first().getId();
			instanceFieldSelected = instanceFieldSelected.filter("instance_message_id=="+instance_message_id);
        return instanceFieldSelected;
			} 
		//otherwise we can keep filtering along time to find several fields
		else {
			
	        if (start_log_time!= -1){
	        	instanceMessageSelected = instanceMessageSelected.filter("log_time>="+start_log_time);
	        	int instance_message_id_start = instanceMessageSelected.orderBy(instanceMessageSelected.col("id").asc()).first().getId();
	        	instanceFieldSelected=instanceFieldSelected.filter("instance_message_id>="+instance_message_id_start);
	        }
	        
	        if (end_log_time!= -1){
	        	instanceMessageSelected = instanceMessageSelected.filter("log_time<="+end_log_time);
	        	int instance_message_id_end = instanceMessageSelected.orderBy(instanceMessageSelected.col("id").desc()).first().getId();
	        	instanceFieldSelected=instanceFieldSelected.filter("instance_message_id<="+instance_message_id_end);
	        }
	        if (end_log_time== -1 && duration!= -1){
	        	end_log_time = start_log_time+duration;
	        	instanceMessageSelected = instanceMessageSelected.filter("log_time<="+end_log_time);
	        	int instance_message_id_end = instanceMessageSelected.orderBy(instanceMessageSelected.col("id").desc()).first().getId();
	        	instanceFieldSelected=instanceFieldSelected.filter("instance_message_id<="+instance_message_id_end);
	        }		    
	        return instanceFieldSelected;
	      }
	    }
}