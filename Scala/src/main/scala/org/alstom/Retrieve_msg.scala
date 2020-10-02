package org.alstom

//All imports
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.StdIn.{readInt, readLine}

object RetrieveMsg {
	def main(args: Array[String]) {
//Creating Spark Session
val spark = SparkSession.builder().appName("CassTest").master("local[2]").config("spark.cassandra.connection.host","10.22.14.184").config("org.apache.cassandra.auth.user_name", "cassandra").config("org.apache.cassandra.auth.password", "cassandra").getOrCreate()


//Accessing Jupiter database
val field_update = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "field_update", "keyspace" -> "jupiter2" )).load()
val msg_log = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "msg_log", "keyspace" -> "jupiter2" )).load()
val fields = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "fields", "keyspace" -> "jupiter2" )).load()

//Asking for the information input
val msg_name = readLine("Message name: ")

var src_ty = -1
//println("Source Type: ")
//src_ty = readInt()

var src_id = -1
//println("Source ID: ")
//src_id = readInt()

var dest_ty = -1
//println("Destination Type: ")
//dest_ty = readInt()

var dest_id = -1
//println("Destination ID: ")
//dest_id = readInt()

var seq_nb = -1
println("Sequence Number: ")
seq_nb = readInt()

var log_time = -1
//println("Logging Time :")
//log_time = readInt()

//Filter function
def filter(msg_log:DataFrame, msg_name:String, src_ty:Int, src_id:Int, dest_ty:Int,dest_id:Int,seq_nb:Int, log_time:Int) : DataFrame={
	var filtered_msg_log = msg_log
	if (msg_name!=""){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("src_ty")===msg_name)}
	if (src_ty!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("src_ty")===src_ty)}
	if (src_id!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("src_id")===src_id)}
	if (dest_ty!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("dest_ty")===dest_ty)}
	if (dest_id!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("dest_id")===dest_id)}
	if (seq_nb!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("seq_nb")===seq_nb)}
	if (log_time!= -1){filtered_msg_log=filtered_msg_log.filter(filtered_msg_log("log_time")===log_time)}
	return filtered_msg_log
}

//Function to get field names
def get_fields(filtered_msg_log : DataFrame, fields:DataFrame, len:Int) : DataFrame ={
	var field_names = new ListBuffer[(Int,List[String])]()
	var i:Int = 1
	val w2 = Window.orderBy(asc("seq_nb"))
	var filtered_msg_log_order = filtered_msg_log.withColumn("row",row_number.over(w2))
	while (i<=len){
		val chosen_msg_type = filtered_msg_log.where($"row" === i).select("type_id").head.getInt(0)
		val chosen_fields = fields.filter(fields("msg_type")===chosen_msg_type).orderBy(asc("field_list_index")).select("field_name").rdd.map(r => r(0)).asInstanceOf[String]
		field_names ++= ListBuffer(List(i,chosen_fields))
		i+=1
	}
	val field_names_df = field_names.toList.map(x =>(x(0), x(1))).toDF("index", "field_names")
	filtered_msg_log_order = filtered_msg_log_order.join(field_names_df, filtered_msg_log_order("row")==field_names_df("index"))
	filtered_msg_log_order.drop("index")
	return filtered_msg_log_order
}

//Function to get a single message content
def get_single_msg_content = (time : Int, names:List[String], field_update:DataFrame) : List[(Int, Int)] = {
	var field_content = new ListBuffer[Int]()
	val n = names.size
	var i:Int = 0
	while (i<n){
		val field_rec=field_update.filter(field_update("log_time")>time).head.getInt(0)).filter(field_update("field_name")===names(i)).head.getInt(6)
		field_content++= ListBuffer((i,field_rec))
		i+=1
	}
	return field_content.toList
}

//Function to get all messages content
def get_fields_content = (filtered_msg_with_fields : DataFrame, field_update:DataFrame, len:Int) : DataFrame = {
	var chosen_content = new ListBuffer[List[Int]]()
	
	var i:Int = 0
	while (i<len){
		var given_row = filtered_msg_with_fields.where($"row" === i)
		val time = given_row.select("log_time").head.getInt(0)
		val names = given_row.select("field_names").head.getInt(0)
		val field_rec = get_single_msg_content(time , names, field_update)
		chosen_content++= ListBuffer(field_rec)
		i+=1
	}
	var chosen_content_df = chosen_content.toList.map(x =>(x(0), x(1))).toDF("index", "msg_content")
	final_df = filtered_msg_with_fields.join(chosen_content_df, filtered_msg_with_fields("row")==chosen_content_df("index"))
	final_df.drop("index").drop("row")
	return final_df
}

//Function for retrieving fields value from update
def get_msg_content (msg_log:DataFrame, field_update:DataFrame, msg_name:String, src_ty:Int, src_id:Int, dest_ty:Int,dest_id:Int,seq_nb:Int, log_time:Int):  DataFrame = {
	
	val filtered_msg_log = filter(msg_log, msg_name, src_ty, src_id, dest_ty,dest_id,seq_nb, log_time)
	val len = filtered_msg_log.count
	
	val filtered_msg_with_fields = get_fields(filtered_msg_log , fields, len)
	
	return get_fields_content (filtered_msg_with_fields , field_update, len)

}


//Finding the field values for given message
val chosen_msg_content = get_msg_content(msg_log, field_update, msg_name, src_ty , src_id, dest_ty,dest_id, seq_nb, log_time)
chosen_msg_content.show

}
}