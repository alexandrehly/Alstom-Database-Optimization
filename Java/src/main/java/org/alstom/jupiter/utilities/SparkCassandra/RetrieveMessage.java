package org.alstom.jupiter.utilities.SparkCassandra;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.alstom.jupiter.utilities.SparkCassandra.DataLoader.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RetrieveMessage {
	public static void main (String[] args) {
		
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Logger.getLogger("org").setLevel(Level.DEBUG);
		Logger.getLogger("com").setLevel(Level.DEBUG);
		
		@SuppressWarnings("unused")
		SparkSession spark = SparkSession
				  .builder()
				  .appName("RetrieveField")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		
		//Loading the needed tables
		String keyspaceName = "fluence_lille_1311beta29_site_i_62c3";
		Dataset<InstanceField> instanceField = DataLoader.loadInstanceField(keyspaceName);
		Dataset<InstanceMessage> instanceMessage = DataLoader.loadInstanceMessage(keyspaceName);
		Dataset<UevolField> uevolField= DataLoader.loadUevolField(keyspaceName);
		
		//Defining the arguments for field research
	    int src_ty = 34;
	    int src_id = 13;
	    int dst_ty = 35;
	    int dst_id = 11;
	    long log_time = 1581734409341L; 
	    long sync_time = -1L; 
	    
	    //Calling the message filtering function  
	    Dataset<MessageContent> instanceFieldSelected = messageFilter(instanceField, instanceMessage, uevolField,  src_id,  dst_id, src_ty, dst_ty, log_time, sync_time) ;		
	    
	    instanceFieldSelected.filter("id>=7196").show();
	    
	}
	
	static Dataset<MessageContent> messageFilter(Dataset<InstanceField> instanceField, Dataset<InstanceMessage> instanceMessage, Dataset<UevolField> uevolField, int src_id, int dst_id, int src_ty, int dst_ty, long log_time, long sync_time ) {
		
		//Initializing Spark Session
		SparkSession spark = SparkSession
				.builder()
				.appName("Retrieve Message")
				.getOrCreate();
		
		//Loading the tables
		Dataset<InstanceField> instanceFieldSelected = instanceField;
		Dataset<InstanceMessage> instanceMessageSelected = instanceMessage;
		Dataset<UevolField> uevolFieldSelected = uevolField;
		
		long timeStart = System.currentTimeMillis();
		
		//Filtering along the arguments to find the message
		if (log_time!= -1){instanceMessageSelected=instanceMessageSelected.filter("log_time=="+log_time);}
		if (sync_time!= -1){instanceMessageSelected=instanceMessageSelected.filter("sync_time=="+sync_time);}
		if (src_id!= -1){instanceMessageSelected=instanceMessageSelected.filter("src_id=="+src_id);}
        if (dst_id!= -1){instanceMessageSelected=instanceMessageSelected.filter("dst_id=="+dst_id);}
        if (src_ty!= -1){instanceMessageSelected=instanceMessageSelected.filter("src_ty=="+src_ty);}
		if (dst_ty!= -1){instanceMessageSelected=instanceMessageSelected.filter("dst_ty=="+dst_ty);}
		
		//Getting the uevol_message_id and instance_message_id
		int uevol_message_id = instanceMessageSelected.first().getUevol_message_id();
		int instance_message_id = instanceMessageSelected.first().getId();

		//Getting the list of fields
		uevolFieldSelected = uevolFieldSelected.filter("uevol_message_id="+uevol_message_id).orderBy(uevolFieldSelected.col("id").asc());
	    
		//Filtering along the arguments to find the fields value
		if (src_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("src_id=="+src_id);}
        if (dst_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("dst_id=="+dst_id);}
        if (uevol_message_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("uevol_message_id=="+uevol_message_id);}
        if (instance_message_id!= -1){instanceFieldSelected=instanceFieldSelected.filter("instance_message_id<="+instance_message_id);}
	    
        //Creating the udf
        final Dataset<InstanceField> instanceFieldSelectedUdf = instanceFieldSelected;
        UDF1<Integer, Long> getFieldsUdf = new UDF1<Integer, Long>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Long call(Integer id) throws Exception {
                return instanceFieldSelectedUdf.filter("uevol_field_id=="+id).orderBy(instanceFieldSelectedUdf.col("instance_message_id").desc()).first().getNew_value();

            }
        };

        /**Register udf*/
        spark.udf().register("getFieldsUdf", getFieldsUdf, DataTypes.LongType);

        /**call udf using functions.callUDF method*/
        Dataset<Row> messageContentDf = uevolFieldSelected.select(uevolFieldSelected.col("id"), uevolFieldSelected.col("name") , functions.callUDF("getFieldsUdf",uevolFieldSelected.col("id")).alias("value"));
        
        //Creating the dataset schema
        Encoder<MessageContent> encoderMessageContent = Encoders.bean(MessageContent.class);
        
        //Creating the output dataset
        Dataset<MessageContent> messageContent = messageContentDf.as(encoderMessageContent);
        
        System.out.println(("Temps de calcul: "+((System.currentTimeMillis()-timeStart)/1000)));
        return messageContent;           
	     }

	public static class MessageContent implements Serializable {
		public MessageContent(int id, String name, long value) {
			super();
			this.id = id;
			this.name = name;
			this.value = value;
		}
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private int id;
		private String name;
		private long value;
		@Override
		public String toString() {
			return "MessageContent [name=" + name + ", value=" + value + "]";
		}
		public int getId() {
			return id;
		}
		public void setName(int id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public long getValue() {
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		}
	    
}
