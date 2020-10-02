package org.alstom

import org.apache.spark.sql.{Encoders, SparkSession}

case class InstanceField(uevol_field_id: Int,  uevol_message_id: Int,  instance_message_id: Int,  instance_message_id_previous: Int,  src_id: Int,  dst_id: Int,  relative_path: String,  iteration: Int,  previous_value: Long,  new_value: Long)
case class InstanceMessage(id: Int,  uevol_message_id: Int,  src_ty: Int,  src_id: Int,  dst_ty: Int,  dst_id: Int,  seq_nb: Int,  log_time: Long,  sync_time: Long)
case class UevolEquipment(ssty: Int,  ssid: Int,  name: Int,  sector_id: Int,  playback_activated: Boolean,  online_activated: Boolean)
case class UevolField(id : Int,  uevol_message_id: Int,  name: String,  designation: String,  indexation: Int,  a_type: Int,  size: Int,  unit: Int,  enumerated: Boolean,  playback_activated: Boolean)
case class UevolMessage(id: Int,  source: Int,  destination: Int,  message_id: Int,  name: String,  protocol: String,  size_in: String,  playback_activated: Boolean,  online_activated: Boolean)
case class UevolProject(key: Int,  key_description: String,  value: String)
case class UevolSubsystem(ssty: Int,  name: String)

object DatabaseLoad {
	def main(args: Array[String]) {
		val schema_InstanceField = Encoders.product[InstanceField].schema
		val schema_InstanceMessage = Encoders.product[InstanceMessage].schema
		val schema_UevolEquipment = Encoders.product[UevolEquipment].schema
		val schema_UevolField = Encoders.product[UevolField].schema
		val schema_UevolMessage = Encoders.product[UevolMessage].schema
		val schema_UevolProject = Encoders.product[UevolProject].schema
		val schema_UevolSubsystem = Encoders.product[UevolSubsystem].schema

		//Creating Spark Session
		val spark = SparkSession
		  .builder()
		  .appName("Cassandra_Connection")
		  .master("local[2]")
		  .config("spark.cassandra.connection.host","10.22.14.174")
		  .config("org.apache.cassandra.auth.user_name", "cassandra")
		  .config("org.apache.cassandra.auth.password", "cassandra")
		  .getOrCreate()

		//Accessing Jupiter database
		val instance_field = spark
		  .read
		  .format(source = "org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "instance_field_replay", "keyspace" -> "fluence_lille_1311beta29_site_i_62C3" ))
		  .schema(schema_InstanceField)
		  .load()

		val instance_message = spark
		  .read
		  .format("org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "instance_message_replay", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		  .schema(schema_InstanceMessage)
		  .load()

		val uevol_equipment = spark
		  .read
		  .format("org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "uevol_equipment", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		  .schema(schema_UevolEquipment)
		  .load()

		val uevol_field = spark
		.read
		.format("org.apache.spark.sql.cassandra")
		.options(Map( "table" -> "uevol_field", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		.schema(schema_UevolField)
		.load()

		val uevol_message = spark
		  .read
		  .format("org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "uevol_message", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		  .schema(schema_UevolMessage)
		  .load()
		
		val uevol_project = spark
		  .read
		  .format("org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "uevol_project", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		  .schema(schema_UevolProject)
		  .load()

		val uevol_subsystem = spark
		  .read
		  .format("org.apache.spark.sql.cassandra")
		  .options(Map( "table" -> "uevol_subsystem", "keyspace" -> "fluence_lille_1311beta29_site_i_62c3" ))
		  .schema(schema_UevolSubsystem)
		  .load()
		  
		instance_message.show
		
		spark.stop

	}
  }