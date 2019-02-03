package edu.northeastern.sparklings.JoinWithHbase;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseInsertData extends Configured implements Tool {
	
	public final static byte[] INFO_COLUMNFAMILY=Bytes.toBytes("info");
	public final static byte[] FL_DATE=Bytes.toBytes("FL_DATE");
	public final static byte[] ORIGIN_CITY_NAME=Bytes.toBytes("ORIGIN_CITY_NAME");
	public final static byte[] DEST_AIRPORT_ID=Bytes.toBytes("DEST_AIRPORT_ID");
	public final static byte[] DEST_CITY_NAME=Bytes.toBytes("DEST_CITY_NAME");
	public final static byte[] DEP_TIME=Bytes.toBytes("DEP_TIME");
	public final static byte[] DEP_DELAY=Bytes.toBytes("DEP_DELAY");
	public final static byte[] ARR_TIME=Bytes.toBytes("ARR_TIME");
	public final static byte[] ARR_DELAY=Bytes.toBytes("ARR_DELAY");
	public final static  byte[] CANCELLED=Bytes.toBytes("CANCELLED");
	public final static byte[] CRS_ELAPSED_TIME=Bytes.toBytes("CRS_ELAPSED_TIME");
	public final static byte[] ACTUAL_ELAPSED_TIME=Bytes.toBytes("ACTUAL_ELAPSED_TIME");
	public final static byte[] DISTANCE=Bytes.toBytes("DISTANCE");
	public final static byte[] CARRIER_DELAY=Bytes.toBytes("CARRIER_DELAY");
	public final static byte[] WEATHER_DELAY=Bytes.toBytes("WEATHER_DELAY");
	public final static byte[] NAS_DELAY=Bytes.toBytes("NAS_DELAY");
	public final static byte[] SECURITY_DELAY=Bytes.toBytes("SECURITY_DELAY");
	public final static byte[] LATE_AIRCRAFT_DELAY=Bytes.toBytes("LATE_AIRCRAFT_DELAY");
	public final static int AIRLINE_ID_LENGTH=5;
	
	 
	 static class HBaseFlightTable extends Mapper<LongWritable, Text, NullWritable, Put> {
		 
		 
		 @Override
		 public void map(LongWritable key, Text value, Context context) throws
	        IOException, InterruptedException {
			 byte[] row = new byte[AIRLINE_ID_LENGTH*2 + Bytes.SIZEOF_LONG];
			 String[] data = value.toString().replaceAll("\"","").split(",", -1);
			 System.out.println("Cancelled status is " + data[11]);
			 System.out.println(Arrays.toString(data));
			 if(!data[0].contains("FL_DATE") && !data[11].equals("1.00")) {
				 if(!data[7].equals("") || !data[9].equals("")) {
			 String timeStamp =  String.valueOf(System.currentTimeMillis());
			 Bytes.putBytes(row, 0, Bytes.toBytes(data[1]), 0, AIRLINE_ID_LENGTH);
			 Bytes.putBytes(row, AIRLINE_ID_LENGTH, Bytes.toBytes(data[4]), 0, AIRLINE_ID_LENGTH);
			 Bytes.putLong(row, AIRLINE_ID_LENGTH * 2,Long.parseLong(timeStamp));
			  
			 Put put = new Put(row);
			 put.add(INFO_COLUMNFAMILY, FL_DATE, Bytes.toBytes(data[0]));
			 put.add(INFO_COLUMNFAMILY,
					 ORIGIN_CITY_NAME, Bytes.toBytes(data[2]));
			 put.add(INFO_COLUMNFAMILY,
					 DEST_CITY_NAME, Bytes.toBytes(data[5]));
			 put.add(INFO_COLUMNFAMILY,
					 DEP_TIME, Bytes.toBytes(data[7]));
			 put.add(INFO_COLUMNFAMILY,
					 DEP_DELAY, Bytes.toBytes(data[8]));
			 put.add(INFO_COLUMNFAMILY,
					 ARR_TIME, Bytes.toBytes(data[9]));
			 put.add(INFO_COLUMNFAMILY,
					 ARR_DELAY, Bytes.toBytes(data[10]));
			 put.add(INFO_COLUMNFAMILY,
					 CANCELLED, Bytes.toBytes(data[11]));
			 put.add(INFO_COLUMNFAMILY,
					 CRS_ELAPSED_TIME, Bytes.toBytes(data[12]));
			 put.add(INFO_COLUMNFAMILY,
					 ACTUAL_ELAPSED_TIME, Bytes.toBytes(data[13]));
			 put.add(INFO_COLUMNFAMILY,
					 DISTANCE, Bytes.toBytes(data[14]));
			 put.add(INFO_COLUMNFAMILY,
					 CARRIER_DELAY, Bytes.toBytes(data[15]));
			 put.add(INFO_COLUMNFAMILY,
					 WEATHER_DELAY, Bytes.toBytes(data[16]));
			 put.add(INFO_COLUMNFAMILY,
					 NAS_DELAY, Bytes.toBytes(data[17]));
			 put.add(INFO_COLUMNFAMILY,
					 SECURITY_DELAY, Bytes.toBytes(data[18]));
			 put.add(INFO_COLUMNFAMILY,
					 LATE_AIRCRAFT_DELAY, Bytes.toBytes(data[19]));
			 
			 
			 context.write(null, put);
		 }
			 }
		 }
	 }

@Override
public int run(String[] args) throws Exception {
	if (args.length != 1) {
	      System.err.println("Usage: HbaseInsertData <input>");
	      return -1;
	    }
	
	
		Configuration conf =  HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "ec2-35-175-136-54.compute-1.amazonaws.com");   
       final Job job = Job.getInstance(conf, "AirlineAnalysisInsertData");
      // job.getConfiguration().set("hbase.zookeeper.quorum", "ec2-35-175-245-41.compute-1.amazonaws.com");
       job.setJarByClass(HbaseInsertData.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "airlines");
       job.setMapperClass(HBaseFlightTable.class);
       job.setNumReduceTasks(0);
       job.setOutputFormatClass(TableOutputFormat.class);
       return job.waitForCompletion(true) ? 0 : 1; 

}
public static void main(String[] args) {
	 int exitCode = 0;
	try {
		exitCode = ToolRunner.run(new HbaseInsertData(), args);
	} catch (Exception e) {
		e.printStackTrace();
	}
		    System.exit(exitCode);
}


}
