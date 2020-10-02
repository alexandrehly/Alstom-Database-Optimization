package org.alstom.jupiter.utilities.SparkCassandra;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DataLoader {
	public static void main (String[] args) {
		
		//Initializing Spark Session to connect to Cassandra Database on server address
		SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
	              .config("spark.cassandra.connection.host","10.22.14.174")
	              .config("spark.cassandra.connection.port", "9042")
	              .config("org.apache.cassandra.auth.user_name", "cassandra")
	              .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		
		//Defining the map scheme to load the tables
		String keyspaceName = "fluence_lille_1311beta29_site_i_62c3";
		
		//Load the tables
		Dataset<InstanceField> instanceField = loadInstanceField(keyspaceName);
		Dataset<InstanceMessage> instanceMessage = loadInstanceMessage(keyspaceName);
		Dataset<UevolEquipment> uevolEquipment = loadUevolEquipment(keyspaceName);
		Dataset<UevolField> uevolField = loadUevolField(keyspaceName);
		Dataset<UevolMessage> uevolMessage = loadUevolMessage(keyspaceName);
		Dataset<UevolProject> uevolProject = loadUevolProject(keyspaceName);
		Dataset<UevolSubsystem> uevolSubsystem = loadUevolSubsystem(keyspaceName);

		instanceField.show();
		instanceMessage.show();
		uevolEquipment.show();
		uevolField.show();
		uevolMessage.show();
		uevolProject.show();
		uevolSubsystem.show();
		
		//Ending the SparkSession
		spark.stop();
	}
	
	//Defining the classes corresponding to Cassandra Tables
	  public static class InstanceField implements Serializable {
		  	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
			public static final String tableName = "instance_field_replay";
		    private int uevol_field_id;
		    private int uevol_message_id;
		    private int instance_message_id;
		    private int instance_message_id_previous;
		    private int src_id;  
		    private int dst_id;  
		    private String relative_path;  
		    private int iteration;  
		    private long previous_value;
		    private long new_value;
			@Override
			public String toString() {
				return "InstanceField [uevol_field_id=" + uevol_field_id + ", uevol_message_id=" + uevol_message_id
						+ ", instance_message_id=" + instance_message_id + ", instance_message_id_previous="
						+ instance_message_id_previous + ", src_id=" + src_id + ", dst_id=" + dst_id
						+ ", relative_path=" + relative_path + ", iteration=" + iteration + ", previous_value="
						+ previous_value + ", new_value=" + new_value + "]";
			}
			public int getUevol_field_id() {
				return uevol_field_id;
			}
			public void setUevol_field_id(int uevol_field_id) {
				this.uevol_field_id = uevol_field_id;
			}
			public int getUevol_message_id() {
				return uevol_message_id;
			}
			public void setUevol_message_id(int uevol_message_id) {
				this.uevol_message_id = uevol_message_id;
			}
			public int getInstance_message_id() {
				return instance_message_id;
			}
			public void setInstance_message_id(int instance_message_id) {
				this.instance_message_id = instance_message_id;
			}
			public int getInstance_message_id_previous() {
				return instance_message_id_previous;
			}
			public void setInstance_message_id_previous(int instance_message_id_previous) {
				this.instance_message_id_previous = instance_message_id_previous;
			}
			public int getSrc_id() {
				return src_id;
			}
			public void setSrc_id(int src_id) {
				this.src_id = src_id;
			}
			public int getDst_id() {
				return dst_id;
			}
			public void setDst_id(int dst_id) {
				this.dst_id = dst_id;
			}
			public String getRelative_path() {
				return relative_path;
			}
			public void setRelative_path(String relative_path) {
				this.relative_path = relative_path;
			}
			public int getIteration() {
				return iteration;
			}
			public void setIteration(int iteration) {
				this.iteration = iteration;
			}
			public long getPrevious_value() {
				return previous_value;
			}
			public void setPrevious_value(long previous_value) {
				this.previous_value = previous_value;
			}
			public long getNew_value() {
				return new_value;
			}
			public void setNew_value(long new_value) {
				this.new_value = new_value;
			}
		  }
	  
	  public static class InstanceMessage implements Serializable {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "instance_message_replay";
			private int id;  
		    private int uevol_message_id;  
		    private int src_ty; 
		    private int src_id;  
		    private int dst_ty;  
		    private int dst_id;  
		    private int seq_nb;  
		    private long log_time;  
		    private long sync_time;
			@Override
			public String toString() {
				return "InstanceMessage [id=" + id + ", uevol_message_id=" + uevol_message_id + ", src_ty=" + src_ty
						+ ", src_id=" + src_id + ", dst_ty=" + dst_ty + ", dst_id=" + dst_id + ", seq_nb=" + seq_nb
						+ ", log_time=" + log_time + ", sync_time=" + sync_time + "]";
			}
			public int getId() {
				return id;
			}
			public void setId(int id) {
				this.id = id;
			}
			public int getUevol_message_id() {
				return uevol_message_id;
			}
			public void setUevol_message_id(int uevol_message_id) {
				this.uevol_message_id = uevol_message_id;
			}
			public int getSrc_ty() {
				return src_ty;
			}
			public void setSrc_ty(int src_ty) {
				this.src_ty = src_ty;
			}
			public int getSrc_id() {
				return src_id;
			}
			public void setSrc_id(int src_id) {
				this.src_id = src_id;
			}
			public int getDst_ty() {
				return dst_ty;
			}
			public void setDst_ty(int dst_ty) {
				this.dst_ty = dst_ty;
			}
			public int getDst_id() {
				return dst_id;
			}
			public void setDst_id(int dst_id) {
				this.dst_id = dst_id;
			}
			public int getSeq_nb() {
				return seq_nb;
			}
			public void setSeq_nb(int seq_nb) {
				this.seq_nb = seq_nb;
			}
			public long getLog_time() {
				return log_time;
			}
			public void setLog_time(long log_time) {
				this.log_time = log_time;
			}
			public long getSync_time() {
				return sync_time;
			}
			public void setSync_time(long sync_time) {
				this.sync_time = sync_time;
			}
		  }
	  
	  public static class UevolEquipment implements Serializable {  
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "uevol_equipment";
			private int ssty; 
		    private int ssid;  
		    private String name;  
		    private int sector_id;  
		    private boolean playback_activated;  
		    private boolean online_activated;
			@Override
			public String toString() {
				return "UevolEquipment [ssty=" + ssty + ", ssid=" + ssid + ", name=" + name + ", sector_id=" + sector_id
						+ ", playback_activated=" + playback_activated + ", online_activated=" + online_activated + "]";
			}
			public int getSsty() {
				return ssty;
			}
			public void setSsty(int ssty) {
				this.ssty = ssty;
			}
			public int getSsid() {
				return ssid;
			}
			public void setSsid(int ssid) {
				this.ssid = ssid;
			}
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public int getSector_id() {
				return sector_id;
			}
			public void setSector_id(int sector_id) {
				this.sector_id = sector_id;
			}
			public boolean isPlayback_activated() {
				return playback_activated;
			}
			public void setPlayback_activated(boolean playback_activated) {
				this.playback_activated = playback_activated;
			}
			public boolean isOnline_activated() {
				return online_activated;
			}
			public void setOnline_activated(boolean online_activated) {
				this.online_activated = online_activated;
			}  
		  }
	  
	  public static class UevolField implements Serializable {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "uevol_field";
			private int id;  
		    private int uevol_message_id;  
		    private String name; 
		    private String designation;  
		    private int indexation;  
		    private int type;  
		    private int size;  
		    private String unit; 
		    private boolean enumerated;
		    private boolean playback_activated;  
		    private boolean online_activated;
			@Override
			public String toString() {
				return "UevolField [id=" + id + ", uevol_message_id=" + uevol_message_id + ", name=" + name
						+ ", designation=" + designation + ", indexation=" + indexation + ", type=" + type + ", size="
						+ size + ", unit=" + unit + ", enumerated=" + enumerated + ", playback_activated="
						+ playback_activated + ", online_activated=" + online_activated + "]";
			}
			public int getId() {
				return id;
			}
			public void setId(int id) {
				this.id = id;
			}
			public int getUevol_message_id() {
				return uevol_message_id;
			}
			public void setUevol_message_id(int uevol_message_id) {
				this.uevol_message_id = uevol_message_id;
			}
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public String getDesignation() {
				return designation;
			}
			public void setDesignation(String designation) {
				this.designation = designation;
			}
			public int getIndexation() {
				return indexation;
			}
			public void setIndexation(int indexation) {
				this.indexation = indexation;
			}
			public int getType() {
				return type;
			}
			public void setType(int type) {
				this.type = type;
			}
			public int getSize() {
				return size;
			}
			public void setSize(int size) {
				this.size = size;
			}
			public String getUnit() {
				return unit;
			}
			public void setUnit(String unit) {
				this.unit = unit;
			}
			public boolean isEnumerated() {
				return enumerated;
			}
			public void setEnumerated(boolean enumerated) {
				this.enumerated = enumerated;
			}
			public boolean isPlayback_activated() {
				return playback_activated;
			}
			public void setPlayback_activated(boolean playback_activated) {
				this.playback_activated = playback_activated;
			}
			public boolean isOnline_activated() {
				return online_activated;
			}
			public void setOnline_activated(boolean online_activated) {
				this.online_activated = online_activated;
			}
		  }
	  
	  public static class UevolMessage implements Serializable {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "uevol_message";
			private int id;  
		    private int source;  
		    private int destination; 
		    private int message_id;  
		    private String name;  
		    private String protocol;  
		    private String size_in;  
		    private boolean playback_activated;  
		    private boolean online_activated;
			@Override
			public String toString() {
				return "UevolMessage [id=" + id + ", source=" + source + ", destination=" + destination
						+ ", message_id=" + message_id + ", name=" + name + ", protocol=" + protocol + ", size_in="
						+ size_in + ", playback_activated=" + playback_activated + ", online_activated="
						+ online_activated + "]";
			}
			public int getId() {
				return id;
			}
			public void setId(int id) {
				this.id = id;
			}
			public int getSource() {
				return source;
			}
			public void setSource(int source) {
				this.source = source;
			}
			public int getDestination() {
				return destination;
			}
			public void setDestination(int destination) {
				this.destination = destination;
			}
			public int getMessage_id() {
				return message_id;
			}
			public void setMessage_id(int message_id) {
				this.message_id = message_id;
			}
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}
			public String getProtocol() {
				return protocol;
			}
			public void setProtocol(String protocol) {
				this.protocol = protocol;
			}
			public String getSize_in() {
				return size_in;
			}
			public void setSize_in(String size_in) {
				this.size_in = size_in;
			}
			public boolean isPlayback_activated() {
				return playback_activated;
			}
			public void setPlayback_activated(boolean playback_activated) {
				this.playback_activated = playback_activated;
			}
			public boolean isOnline_activated() {
				return online_activated;
			}
			public void setOnline_activated(boolean online_activated) {
				this.online_activated = online_activated;
			}
		  }
	  
	  public static class UevolProject implements Serializable {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "uevol_project";
			private int key;  
		    private int key_description;  
		    private String value;
			@Override
			public String toString() {
				return "UevolProject [key=" + key + ", key_description=" + key_description + ", value=" + value + "]";
			}
			public int getKey() {
				return key;
			}
			public void setKey(int key) {
				this.key = key;
			}
			public int getKey_description() {
				return key_description;
			}
			public void setKey_desciption(int key_description) {
				this.key_description = key_description;
			}
			public String getValue() {
				return value;
			}
			public void setValue(String value) {
				this.value = value;
			} 
		  }
	  
	  public static class UevolSubsystem implements Serializable {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final String tableName = "uevol_subsystem";
			private int ssty;  
		    private String name;
			@Override
			public String toString() {
				return "UevolSubsystem [ssty=" + ssty + ", name=" + name + "]";
			}
			public int getSsty() {
				return ssty;
			}
			public void setSsty(int ssty) {
				this.ssty = ssty;
			}
			public String getName() {
				return name;
			}
			public void setName(String name) {
				this.name = name;
			}  
		  }
	  
	//Defining the mapping function
	  static Map<String,String> mapping(String table, String keyspace) {
			Map<String,String> map_request= new HashMap<String,String>();
			map_request.put("table",table);
			map_request.put("keyspace",keyspace);
			return map_request;
		  }
	  
	//Defining the load functions
	  static Dataset<InstanceField> loadInstanceField(String keyspaceName) {
			  Map<String,String> mapInstanceField = mapping(InstanceField.tableName, keyspaceName);
			  Encoder<InstanceField> encoderInstanceField = Encoders.bean(InstanceField.class);
			  
			  SparkSession spark = SparkSession
					  .builder()
					  .appName("DataLoader")
			          .config("spark.cassandra.connection.host","10.22.14.174")
			          .config("spark.cassandra.connection.port", "9042")
			          .config("org.apache.cassandra.auth.user_name", "cassandra")
			          .config("org.apache.cassandra.auth.password", "cassandra")
					  .getOrCreate();
			  
			  Dataset<Row> instanceFieldDF = spark
						.read()
						.format("org.apache.spark.sql.cassandra")
						.options(mapInstanceField)
						.load();
			  Dataset<InstanceField> instanceField = instanceFieldDF.as(encoderInstanceField);
			  
			  return instanceField;
		  }
	  
	  static Dataset<InstanceMessage> loadInstanceMessage(String keyspaceName) {
		  Map<String,String> mapInstanceMessage = mapping(InstanceMessage.tableName, keyspaceName);
		  Encoder<InstanceMessage> encoderInstanceMessage = Encoders.bean(InstanceMessage.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> instanceMessageDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapInstanceMessage)
					.load();
		  Dataset<InstanceMessage> instanceMessage = instanceMessageDF.as(encoderInstanceMessage);
		  
		  return instanceMessage;
	  }
	  
	  static Dataset<UevolEquipment> loadUevolEquipment(String keyspaceName) {
		  Map<String,String> mapUevolEquipment = mapping(UevolEquipment.tableName, keyspaceName);
		  Encoder<UevolEquipment> encoderUevolEquipment = Encoders.bean(UevolEquipment.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> uevolEquipmentDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapUevolEquipment)
					.load();
		  Dataset<UevolEquipment> uevolEquipment = uevolEquipmentDF.as(encoderUevolEquipment);
		  
		  return uevolEquipment;
	  }
	  
	  static Dataset<UevolField> loadUevolField(String keyspaceName) {
		  Map<String,String> mapUevolField = mapping(UevolField.tableName, keyspaceName);
		  Encoder<UevolField> encoderUevolField = Encoders.bean(UevolField.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> uevolFieldDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapUevolField)
					.load();
		  Dataset<UevolField> uevolField = uevolFieldDF.as(encoderUevolField);
		  
		  return uevolField;
	  }
	  
	  static Dataset<UevolMessage> loadUevolMessage(String keyspaceName) {
		  Map<String,String> mapUevolMessage = mapping(UevolMessage.tableName, keyspaceName);
		  Encoder<UevolMessage> encoderUevolMessage = Encoders.bean(UevolMessage.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> uevolMessageDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapUevolMessage)
					.load();
		  Dataset<UevolMessage> uevolMessage = uevolMessageDF.as(encoderUevolMessage);
		  
		  return uevolMessage;
	  }
	  
	  static Dataset<UevolProject> loadUevolProject(String keyspaceName) {
		  Map<String,String> mapUevolProject = mapping(UevolProject.tableName, keyspaceName);
		  Encoder<UevolProject> encoderUevolProject = Encoders.bean(UevolProject.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> uevolProjectDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapUevolProject)
					.load();
		  Dataset<UevolProject> uevolProject = uevolProjectDF.as(encoderUevolProject);
  
		  return uevolProject;
	  }
	  
	  static Dataset<UevolSubsystem> loadUevolSubsystem(String keyspaceName) {
		  Map<String,String> mapUevolSubsystem = mapping(UevolSubsystem.tableName, keyspaceName);
		  Encoder<UevolSubsystem> encoderUevolSubsystem = Encoders.bean(UevolSubsystem.class);
		  
		  SparkSession spark = SparkSession
				  .builder()
				  .appName("DataLoader")
		          .config("spark.cassandra.connection.host","10.22.14.174")
		          .config("spark.cassandra.connection.port", "9042")
		          .config("org.apache.cassandra.auth.user_name", "cassandra")
		          .config("org.apache.cassandra.auth.password", "cassandra")
				  .getOrCreate();
		  
		  Dataset<Row> uevolSubsystemDF = spark
					.read()
					.format("org.apache.spark.sql.cassandra")
					.options(mapUevolSubsystem)
					.load();
		  Dataset<UevolSubsystem> uevolSubsystem = uevolSubsystemDF.as(encoderUevolSubsystem);
		  
		  return uevolSubsystem;
	  }
}
