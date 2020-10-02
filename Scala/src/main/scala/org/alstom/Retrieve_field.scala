package org.alstom

//All imports
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn.readLine

object RetrieveField {
	def main(args: Array[String]) {
		//Creating Spark Session
		val spark = SparkSession.builder().appName("RetrieveField").master("local[2]").config("spark.cassandra.connection.host","10.22.14.174").config("org.apache.cassandra.auth.user_name", "cassandra").config("org.apache.cassandra.auth.password", "cassandra").getOrCreate()


		//Accessing Jupiter database
		val field_update = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "instance_field_replay" -> "field_update", "keyspace" -> "jupiter2" )).load()


		//Asking for the information input
		val field_name = ""
		var src_ty = -1
		var src_id = -1
		var dest_ty = -1
		var dest_id = -1
		var seq_nb = -1
		var start_log_time = -1
		var end_log_time = -1
		var duration = -1
var chosen_value = -1



		//Filtering for the given field updates
		def get_field_updates (field_update:DataFrame, field_name:String, src_ty:Int, src_id:Int, dest_ty:Int,dest_id:Int, seq_nb:Int,start_log_time:Int, end_log_time:Int, chosen_value:Int ) : DataFrame = {
			var selected_field_update = field_update
			if (field_name!=""){selected_field_update=selected_field_update.filter(field_update("field_name")===field_name)}
			if (chosen_value!= -1){selected_field_update=selected_field_update.filter(field_update("new_value")===chosen_value)}
			if (seq_nb != -1){
				if (src_ty!= -1){selected_field_update=selected_field_update.filter(field_update("src_ty")===src_ty)}
				if (src_id!= -1){selected_field_update=selected_field_update.filter(field_update("src_id")===src_id)}
				if (dest_ty!= -1){selected_field_update=selected_field_update.filter(field_update("dest_ty")===dest_ty)}
				if (dest_id!= -1){selected_field_update=selected_field_update.filter(field_update("dest_id")===dest_id)}
				val w2 = Window.orderBy(col("seq_nb"))
				selected_field_update = selected_field_update.filter(field_update("seq_nb")>seq_nb).withColumn("row",row_number.over(w2)).where($"row" === 1).drop("row")
				selected_field_update = selected_field_update.withColumn("field_value", $"prev_value").drop("new_value").drop("prev_value").drop("seq_nb").drop("msg_id")
				selected_field_update = selected_field_update.select("dest_id","dest_ty","field_name","src_id","src_ty","field_value","log_time")
				
				return selected_field_update
			} else {
				val w3 = Window.orderBy(asc("log_time"))
				val w4 = Window.orderBy(desc("log_time"))
				var first_row = selected_field_update.withColumn("row",row_number.over(w3)).where($"row" === 1).drop("row")
				var last_row = selected_field_update.withColumn("row",row_number.over(w4)).where($"row" === 1).drop("row")
				
				if (src_ty!= -1){selected_field_update=selected_field_update.filter(field_update("src_ty")===src_ty)}
				if (src_id!= -1){selected_field_update=selected_field_update.filter(field_update("src_id")===src_id)}
				if (dest_ty!= -1){selected_field_update=selected_field_update.filter(field_update("dest_ty")===dest_ty)}
				if (dest_id!= -1){selected_field_update=selected_field_update.filter(field_update("dest_id")===dest_id)}
				if (start_log_time!= -1){selected_field_update=selected_field_update.filter(field_update("log_time")>=start_log_time)}
				if (end_log_time!= -1){selected_field_update=selected_field_update.filter(field_update("log_time")<=end_log_time)	}

				selected_field_update = selected_field_update.withColumn("field_value", $"new_value").drop("prev_value").drop("new_value").drop("seq_nb").drop("msg_id")
				selected_field_update = selected_field_update.select("dest_id","dest_ty","field_name","src_id","src_ty","field_value","log_time").orderBy(asc("log_time"))
				
				if (start_log_time!= -1 && chosen_value== -1){
					first_row = first_row.withColumn("field_value", $"prev_value").drop("log_time").drop("prev_value").drop("new_value").drop("seq_nb").drop("msg_id")
					first_row = first_row.withColumn("log_time", lit(start_log_time))
					selected_field_update=first_row.union(selected_field_update)
				}
				if (end_log_time!= -1 && chosen_value== -1){
					last_row = last_row.withColumn("field_value", $"new_value").drop("log_time").drop("prev_value").drop("new_value").drop("seq_nb").drop("msg_id")
					last_row = last_row.withColumn("log_time", lit(end_log_time))
					selected_field_update=selected_field_update.union(last_row)
				}
				return selected_field_update
			}
		}

		//Find all fields 
		val chosen_field_updates = get_field_updates(field_update, field_name, src_ty, src_id, dest_ty,dest_id, seq_nb,start_log_time, end_log_time, chosen_value)
		chosen_field_updates.show
//voila
}
}