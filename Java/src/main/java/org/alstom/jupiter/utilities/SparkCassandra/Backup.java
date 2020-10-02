package org.alstom.jupiter.utilities.SparkCassandra;

import java.util.Arrays;
import java.util.List;

import org.alstom.jupiter.utilities.SparkCassandra.DataLoader.InstanceField;
import org.alstom.jupiter.utilities.SparkCassandra.DataLoader.InstanceMessage;
import org.alstom.jupiter.utilities.SparkCassandra.DataLoader.UevolField;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class Backup {
	public static void main (String[] args) {
		
	@SuppressWarnings("unused")
	SparkSession spark = SparkSession
			  .builder()
			  .appName("RetrieveField")
			  .master("local")
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
	
	//Setting the time where we remove everything older
	long log_time_limit = 1581731348541L;
	
	//Filtering the dynamic tables to keep data after set time
	Dataset<InstanceMessage> instanceMessageRecent = instanceMessage.filter("log_time>="+log_time_limit).orderBy(instanceMessage.col("id").asc());
	int message_id_limit = instanceMessageRecent.first().getId();
	Dataset<InstanceField> instanceFieldRecent = instanceField.filter("instance_message_id>="+message_id_limit);
	
	//Determining the list of the fields which have changed recently 
	List<Integer> uevolFieldIdUnchangedList = instanceFieldRecent.select("uevol_field_id").toJavaRDD().map(f -> f.getInt(0)).collect();
	
	//Getting the field names which didn't change since after the time limit to add them later
	Dataset<UevolField> uevolFieldUnchanged = uevolField.filter(functions.not(uevolField.col("id").isin(uevolFieldIdUnchangedList)));
	
	//Setting a given message id to the unchanged fields
	int instance_message_id = -1;
	
	//Getting the last change from before the time limit of the unchanged fields
	Dataset<InstanceField> instanceFieldOlder = instanceFieldOlder(instanceField, uevolFieldUnchanged, instance_message_id);
	
	//Concatenate the last changes to the data after the time limit
	Dataset<InstanceField> instanceFieldFinal = instanceFieldOlder.union(instanceFieldRecent);
	instanceFieldFinal.show();
	
	}
	
	static Dataset<InstanceField> instanceFieldOlder(Dataset<InstanceField> instanceField, Dataset<UevolField> uevolFieldUnchanged, int instance_message_id ) {

		//Initializing Spark Session
		SparkSession spark = SparkSession
				.builder()
				.appName("Retrieve Older Fields")
				.master("local[2]")
				.getOrCreate();
		
		//Creating the udf
	    final Dataset<InstanceField> instanceFieldUdf = instanceField;
	    UDF1<Integer, Row> getOlderFieldsUdf = new UDF1<Integer, Row>() {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Row call(Integer uevol_field_id) throws Exception {
				InstanceField instanceFieldChosen = instanceFieldUdf.filter("uevol_field_id=="+uevol_field_id).orderBy(instanceFieldUdf.col("instance_message_id").desc()).first();
				return RowFactory.create(instanceFieldChosen.getRelative_path(), instanceFieldChosen.getSrc_id(), instanceFieldChosen.getDst_id(), instanceFieldChosen.getInstance_message_id_previous(), instanceFieldChosen.getIteration(), instanceFieldChosen.getNew_value(), instanceFieldChosen.getPrevious_value());

	        }
	    };
		
	    //Register udf for a complex datatype output
	    spark.udf().register("getOlderFieldsUdf", getOlderFieldsUdf, DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("relative_path", DataTypes.StringType, true), DataTypes.createStructField("src_id", DataTypes.IntegerType, true), DataTypes.createStructField("dst_id", DataTypes.IntegerType, true), DataTypes.createStructField("instance_message_id_previous", DataTypes.IntegerType, true), DataTypes.createStructField("iteration", DataTypes.IntegerType, true), DataTypes.createStructField("new_value", DataTypes.LongType, true), DataTypes.createStructField("prev_value", DataTypes.LongType, true))));

	    /**call udf using functions.callUDF method*/
	    Dataset<Row> InstanceFieldOlderDf = uevolFieldUnchanged.select(uevolFieldUnchanged.col("id"), uevolFieldUnchanged.col("uevol_message_id"), functions.callUDF("getOlderFieldsUdf",uevolFieldUnchanged.col("id")).alias("Field"));
	    //InstanceFieldOlderDf = InstanceFieldOlderDf.withColumn("newCol", getOlderFieldsUdf(InstanceFieldOlderDf.col("id"))).select("relative_path","src_id" , "dst_id", "instance_message_id_previous", "iteration", "new_value", "previous_value" , "newCol.*");
	    
	    //Creating the dataset schema
	    Encoder<InstanceField> encoderInstanceField = Encoders.bean(InstanceField.class);
	    
	    //Creating the output dataset
	    Dataset<InstanceField> InstanceFieldOlder = InstanceFieldOlderDf.as(encoderInstanceField);
	    
	    return InstanceFieldOlder;
	
	}
}
