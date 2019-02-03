package airlineAnalysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PreprocessMapper extends Mapper<Object, Text, AirlineRowWritable, Text> {
	
	//COLUMNS: 8 is DEP_DELAY, 10 is ARR_DELAY, 11 is CANCELLED, 12 is CRS_ELAPSED_TIME, 14 is DISTANCE, 15 onwards are delays

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		String rowData[] = value.toString().split(",");
		
		for(int i =0;i<rowData.length;i++) {
			rowData[i] = rowData[i].replace("\"", "");
		}
		//Ignore header row
		if(rowData[0].equals("FL_DATE") ||  rowData[13].isEmpty()  ) {
			return;
		}
		// Ignore cancelled flights
		if(rowData[11].equals("1.00")) {
			return;
		}
		AirlineRowWritable airlineRow = new AirlineRowWritable(new Text(rowData[0]),
				new Text(rowData[1]), 
				new Text(rowData[2]+rowData[3]), 
				new Text(rowData[4]),
				new Text(rowData[5]+rowData[6]),
				new Text(rowData[7]), 
				new Text(rowData[9]),
				new DoubleWritable(Double.parseDouble(rowData[13])),
				new Text(),
				new Text()); 


		context.write(airlineRow, new Text());

	}
}