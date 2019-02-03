package edu.northeastern.sparklings.JoinWithHbase;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.northeastern.sparklings.JoinWithHbase.HbaseInsertData.HBaseFlightTable;

public class HbaseJoins extends Configured implements Tool {
	
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

	
	public static class HbaseJoinMapper extends TableMapper<Text, Text> {
		
		 public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			    // process data for the row from the Result instance.
			 		byte[] rowKey = row.get();
			 		String stringFied = new String(rowKey);
			 		byte[] arrivalDate = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("FL_DATE"));
			 		byte[] arrivalTime = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("ARR_TIME"));
			 		if(!arrivalTime.equals("") || arrivalTime!=null) {
			 		context.write(new Text(stringFied.substring(0,5)), new Text(stringFied.substring(5,10) + "," 
			 		+ Bytes.toString(arrivalDate) + "," + Bytes.toString(arrivalTime)));
			 		}
			   }
	}
	
	public static class HbaseJoinReducer extends  TableReducer<Text, Text, Text> {
		
		public  static HTable hTable;
		public static  HTable sinkTable;
		public void setup(Context context) {
			 Configuration conf = context.getConfiguration();
			 try {
				 hTable = new HTable(conf, "airlines");
				 sinkTable = new HTable(conf, "AirlinesTwoHop");
//				 Connection connection = ConnectionFactory.createConnection(conf);
//				 Admin admin = connection.getAdmin();
//				 if(admin.tableExists(TableName.valueOf("AirLinesTwoHop"))) {
//					 if(!admin.isTableDisabled(TableName.valueOf("AirLinesTwoHop"))) {
//						 admin.disableTable((TableName.valueOf("AirLinesTwoHop")));
//					 }
//					 admin.truncateTable(TableName.valueOf("AirLinesTwoHop"), false);
//				 }
//				 admin.close();
				 
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		 public void reduce(Text key, final Iterable<Text> values, Context context) throws InterruptedException, IOException {
			    // process data for every city in this month
			 	
			 for(Text t: values) {
			 		String[] tvalues = t.toString().split(",");
			 		System.out.println(Arrays.toString(tvalues));
			 		String destination=tvalues[0];
			 		ResultScanner resultScanner;
			 		 LocalDateTime arrival;
			 		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HHmm");  
			 		//Arrival time on this destination
			 		try {
			 	    arrival = LocalDateTime.parse(tvalues[1] + " " + tvalues[2], formatter);
			 		
				 	byte[] row = new byte[5];
				 	Bytes.putBytes(row, 0, Bytes.toBytes(destination), 0, 5);
				 	Scan scan = new Scan(row);
				 	Filter prefixFilter = new PrefixFilter(row);
				 	
				 	//fetch the departure time from this destination to other places
			 		
				 	scan.setFilter(prefixFilter);
				 	 resultScanner = hTable.getScanner(scan);
			 		} catch(Exception e) {
			 			continue;
			 		}
				 	for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
				 	    Get get = new Get(result.getRow());
				 	    Result entireRow = hTable.get(get); 
				 	    String cancelledStatus = new String(entireRow.getValue(Bytes.toBytes("info"), Bytes.toBytes("CANCELLED")));
				 	    if(!cancelledStatus.equals("1.00")) {
				 	    String departureDate = new String(entireRow.getValue(Bytes.toBytes("info"), Bytes.toBytes("FL_DATE")));
				 	    //departure time from current place
				 		try {
				 	    String departureTime = new String(entireRow.getValue(Bytes.toBytes("info"), Bytes.toBytes("DEP_TIME")));
				 		String arrivalOnNext = new String(entireRow.getValue(Bytes.toBytes("info"), Bytes.toBytes("ARR_TIME")));
				 		
				 		if((departureTime!= null  || !departureTime.equals("")) && (arrivalOnNext!=null || !arrivalOnNext.equals(""))) {
				 		
				 		LocalDateTime departure = LocalDateTime.parse(departureDate+ " " + departureTime, formatter);
				 		System.out.println(arrival);
				 		System.out.println(departure);
				 		
				 		long hours = arrival.until(departure, ChronoUnit.HOURS);
				 		System.out.println(hours);
				 		
				 		if(hours >=10 && hours <=72 ) {
				 			System.out.println("entered 10");
				 	    String newRowKey = key.toString() + destination + new String(result.getRow()).substring(5, 10) + String.valueOf(System.currentTimeMillis()) ;
				 	    Put put = new Put(Bytes.toBytes(newRowKey));
				 	    byte[] middleFam = Bytes.toBytes("Location1");
				 	    byte[] middleArrivalColumn = Bytes.toBytes("ArrivalTime");
				 	    byte[] middleValue = Bytes.toBytes(arrival.toString());
				 	    put.add(middleFam, middleArrivalColumn, middleValue);
				 	    
				 	    byte[] middleDepartureColumn = Bytes.toBytes("DepartureTime");
				 	    byte[] middleDepartureValue = Bytes.toBytes(departure.toString());
				 	    put.add(middleFam, middleDepartureColumn, middleDepartureValue);
				 	    LocalDateTime loc2arrival = LocalDateTime.parse(
				 	    		new String(entireRow.getValue(Bytes.toBytes("info"), Bytes.toBytes("FL_DATE"))) + " " + arrivalOnNext, formatter);
				 	    byte[] endFam = Bytes.toBytes("Location2");
				 	    byte[] endArrivalColumn = Bytes.toBytes("ArrivalTime");
				 	    byte[] endValue = Bytes.toBytes(loc2arrival.toString());
				 	    put.add(endFam, endArrivalColumn, endValue);
				 	    context.write(null,  put);
				 	    }
				 	    
				 		}
				 		}catch(Exception e) {
				 			continue;
				 		}
				 		}  
				 	    }
			 	    
			 	}

			   }
	}
	
	public static class ThreeHopMapper extends TableMapper<Text, Text> {
		
		 public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			    // process data for the row from the Result instance.
			 		byte[] rowKey = row.get();
			 		String stringFied = new String(rowKey);
			 		byte[] departureDate = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("FL_DATE"));
			 		byte[] departureTime = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("DEP_TIME"));
			 		byte[] arrivalTime = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("ARR_TIME"));
			 		System.out.println(stringFied);
			 		context.write(new Text(stringFied.substring(0,5)), new Text(stringFied.substring(5,10) + "," + Bytes.toString(departureDate) + "," + Bytes.toString(departureTime)
			 																									+ "," + Bytes.toString(arrivalTime)));
			   }
	}
	
	
	
	public static class threeHopReducer extends  TableReducer<Text, Text, Text> {
		
		public  static HTable hTable;
		public void setup(Context context) {
			 Configuration conf = context.getConfiguration();
			 try {
				 hTable = new HTable(conf, "AirLinesTwoHop");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		 public void reduce(Text key, final Iterable<Text> values, Context context) throws InterruptedException, IOException {
			    // process data for every city in this month
			 	for(Text t: values) {
			 		String departuretime =  t.toString().split(",")[1] + " " + t.toString().split(",")[2];
			 		String destination =    t.toString().split(",")[0];
			 		String arrivalTime =    t.toString().split(",")[1] + " " + t.toString().split(",")[3];
				 	Scan scan = new Scan();
				 	FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
				 	RowFilter rowfilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(key.toString().substring(key.toString().length() - 5)));
				 	filterList.addFilter(rowfilter);
				 	scan.setFilter(filterList);
				 	
				 	//apply the filter on two hop table
				 	ResultScanner resultScanner = hTable.getScanner(scan);
				 	
				 	
				 	for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
				 	    Get get = new Get(result.getRow());
				 	    Result entireRow = hTable.get(get); 
				 	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HHmm");  
				 	    LocalDateTime arrival = LocalDateTime.parse(new String(entireRow.getValue(Bytes.toBytes("Location2"), Bytes.toBytes("ArrivalTime"))));
				 	    LocalDateTime departureTime = LocalDateTime.parse(departuretime, formatter);
				 	    LocalDateTime arrivalFromLocation3= LocalDateTime.parse(arrivalTime, formatter);
				 	    long hours = arrival.until(departureTime, ChronoUnit.HOURS);
				 	    if(hours>=10 && hours <=72) {
				 	    String newRowKey = new String(result.getRow()).substring(0, 15) + destination + String.valueOf(System.currentTimeMillis());
				 	    System.out.println(newRowKey);
				 	    Put put = new Put(Bytes.toBytes(newRowKey));
				 	    byte[] ThirdFam = Bytes.toBytes("Location3");
				 	    byte[] middleDepartureColumn = Bytes.toBytes("DepartureTime");
				 	    byte[] middleDepartureValue = Bytes.toBytes(departureTime.toString());
				 	    put.add(ThirdFam, middleDepartureColumn, middleDepartureValue);
				 	    byte[] endArrival = Bytes.toBytes("ArrivalTime");
				 	    byte[] endArrivalTime = Bytes.toBytes(arrivalFromLocation3.toString());
				 	   put.add(ThirdFam, endArrival, endArrivalTime);
				 	    for (Cell cell : result.rawCells())  {
				 		   byte[] family = CellUtil.cloneFamily(cell);
				 		   byte[] column = CellUtil.cloneQualifier(cell);
				 		   byte[] value = CellUtil.cloneValue(cell);
				 		   put.add(family,column,value);
				 		 
				 	   }
				 	   context.write(null, put);
				 	    }
				 	   
				 	}
			 	}

			   }
	
	}
	
	public static class FourHopReducer extends  TableReducer<Text, Text, Text> {
		
		public  static HTable hTable;
		public void setup(Context context) {
			
			 Configuration conf = context.getConfiguration();
			 try {
				 hTable = new HTable(conf, "AirLinesThreeHop");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		 public void reduce(Text key, final Iterable<Text> values, Context context) throws InterruptedException, IOException {
			    // process data for every city in this month
			 	for(Text t: values) {
			 		String departuretime =  t.toString().split(",")[1] + " " + t.toString().split(",")[2];
			 		String destination =    t.toString().split(",")[0];
			 		String arrivalTime =    t.toString().split(",")[1] + " " + t.toString().split(",")[3];
				 	Scan scan = new Scan();
				 	FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
				 	RowFilter rowfilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(key.toString().substring(key.toString().length() - 5)));
				 	filterList.addFilter(rowfilter);
				 	scan.setFilter(filterList);
				 	
				 	//apply the filter on two hop table
				 	ResultScanner resultScanner = hTable.getScanner(scan);
				 	
				 	
				 	for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
				 	    Get get = new Get(result.getRow());
				 	    Result entireRow = hTable.get(get); 
				 	    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HHmm");  
				 	    LocalDateTime arrival = LocalDateTime.parse(new String(entireRow.getValue(Bytes.toBytes("Location3"), Bytes.toBytes("ArrivalTime"))));
				 	   LocalDateTime arrivalFromLocation4= LocalDateTime.parse(arrivalTime, formatter);
				 	    LocalDateTime departureTime = LocalDateTime.parse(departuretime, formatter);
				 	    long hours = arrival.until(departureTime, ChronoUnit.HOURS);
				 	    if(hours>=10 && hours <=72) {
				 	    String newRowKey = new String(result.getRow()).substring(0, 20) + destination + String.valueOf(System.currentTimeMillis());
				 	    System.out.println(newRowKey);
				 	    Put put = new Put(Bytes.toBytes(newRowKey));
				 	    byte[] ThirdFam = Bytes.toBytes("Location4");
				 	    byte[] middleDepartureColumn = Bytes.toBytes("DepartureTime");
				 	    byte[] middleDepartureValue = Bytes.toBytes(departureTime.toString());
				 	    put.add(ThirdFam, middleDepartureColumn, middleDepartureValue);
				 	    byte[] endArrival = Bytes.toBytes("ArrivalTime");
				 	    byte[] endArrivalTime = Bytes.toBytes(arrivalFromLocation4.toString());
				 	    put.add(ThirdFam, endArrival, endArrivalTime);
				 	    for (Cell cell : result.rawCells())  {
				 		   byte[] family = CellUtil.cloneFamily(cell);
				 		   byte[] column = CellUtil.cloneQualifier(cell);
				 		   byte[] value = CellUtil.cloneValue(cell);
				 		   put.add(family,column,value);
				 		 
				 	   }
				 	   context.write(null, put);
				 	    }
				 	   
				 	}
			 	}

			   }
	
	}
	
	@Override
	public int run(String[] args) throws Exception {

	       
	       System.out.println("Computing Two Hop");
	       Configuration conf = getConf();
		  conf.set("hbase.zookeeper.quorum", "ec2-35-175-136-54.compute-1.amazonaws.com");
	       final Job job = Job.getInstance(conf, "AirlineAnalysisJoinJob");
	       job.setJarByClass(HbaseJoins.class);
	       Scan s  = new Scan();
	       s.setCaching(500); //set caching of the MR job
	       s.setCacheBlocks(false);
	       TableMapReduceUtil.initTableMapperJob(
	    		   "airlines", s, HbaseJoinMapper.class, Text.class, Text.class,job);
	       TableMapReduceUtil.initTableReducerJob("AirLinesTwoHop", HbaseJoinReducer.class,job);
	       boolean b = job.waitForCompletion(true);
	       if (!b) {
	           throw new IOException("error with job!");
	       }
	       
	       //job to get the three hop path
	       System.out.println("Computing Three Hop");
	       
	       final Job twoHopJob = Job.getInstance(conf, "Three Hop Job");
	       twoHopJob.setJarByClass(HbaseJoins.class);
	       TableMapReduceUtil.initTableMapperJob(
	    		   "airlines", s, ThreeHopMapper.class, Text.class, Text.class,twoHopJob);
	       TableMapReduceUtil.initTableReducerJob("AirLinesThreeHop", threeHopReducer.class,twoHopJob);
	       boolean c = twoHopJob.waitForCompletion(true);
	       if (!c) {
	           throw new IOException("error with three hop job!");
	       }  
	       
	       System.out.println("Computing Four Hop");
	       final Job fourHopJob = Job.getInstance(conf, "Four Hop Job");
	       fourHopJob.setJarByClass(HbaseJoins.class);
	       TableMapReduceUtil.initTableMapperJob(
	    		   "airlines", s, ThreeHopMapper.class, Text.class, Text.class,fourHopJob);
	       TableMapReduceUtil.initTableReducerJob("AirLinesFourHop", FourHopReducer.class,fourHopJob);
	       boolean d = fourHopJob.waitForCompletion(true);
	       if (!d) {
	           throw new IOException("error with four hop job!");
	       }   
	       
	       return 1;
	}
	
	public static void main(String[] args) {
		 int exitCode = 0;
		try {
			exitCode = ToolRunner.run(HBaseConfiguration.create(),
				        new HbaseJoins(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
			    System.exit(exitCode);
	}

}
