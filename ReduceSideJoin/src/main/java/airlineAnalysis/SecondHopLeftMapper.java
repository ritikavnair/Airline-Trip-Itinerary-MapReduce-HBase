package airlineAnalysis;

import java.io.IOException;
import java.util.StringJoiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SecondHopLeftMapper extends Mapper<Object, Text, Text, AirlineRowWritable> {

	@Override
	public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
		String connectingFlights[] = value.toString().split("----");
		
		String lastFlight[]= connectingFlights[ connectingFlights.length-1].split(",");
		
		StringJoiner pastFlights = new StringJoiner("----");
		for(int i =0;i< connectingFlights.length-1;i++) {
			pastFlights.add(connectingFlights[i]);
		}
		
		AirlineRowWritable airlineRow = new AirlineRowWritable(new Text(lastFlight[0]),
				new Text(lastFlight[1]), 
				new Text(lastFlight[2]), 
				new Text(lastFlight[3]),
				new Text(lastFlight[4]),
				new Text(lastFlight[5]), 
				new Text(lastFlight[6]),
				new DoubleWritable(Double.parseDouble(lastFlight[7])),
				new Text("in"),
				new Text(pastFlights.toString())); 

		
		context.write(airlineRow.getDestAirportId(),airlineRow);
		

	}
}