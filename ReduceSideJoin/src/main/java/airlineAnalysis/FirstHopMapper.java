package airlineAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FirstHopMapper extends Mapper<Object, Text, Text, AirlineRowWritable> {

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		String rowData[] = value.toString().split(",");
		
				
		AirlineRowWritable airlineRow = new AirlineRowWritable(new Text(rowData[0]),
				new Text(rowData[1]), 
				new Text(rowData[2]), 
				new Text(rowData[3]),
				new Text(rowData[4]),
				new Text(rowData[5]), 
				new Text(rowData[6]),
				new DoubleWritable(Double.parseDouble(rowData[7])),
				new Text(),
				new Text()); 

		//airlineRow.setTag("in");
		context.write(airlineRow.getDestAirportId(),airlineRow.withTag("in") );
		
		//airlineRow.setTag("out");
		context.write(airlineRow.getOriginAirportId(), airlineRow.withTag("out"));

	}
}