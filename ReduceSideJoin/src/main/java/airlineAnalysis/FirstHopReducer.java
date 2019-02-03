package airlineAnalysis;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class FirstHopReducer extends Reducer<Text, AirlineRowWritable, Text, Text> {
   

    @Override
    public void reduce(final Text key, final Iterable<AirlineRowWritable> values, final Context context) throws IOException, InterruptedException {
       

		List<AirlineRowWritable> outFlights = new ArrayList<>();
		List<AirlineRowWritable> inFlights = new ArrayList<>();

		//Segregate into leaving and arriving flights
    	for(AirlineRowWritable flight : values) {
    		if(flight.getTag().equals("out")) {
    			outFlights.add(new AirlineRowWritable(flight) );
    		}
    		if(flight.getTag().equals("in")) {
    			inFlights.add(new AirlineRowWritable(flight));
    		}
    	}
    	
    	//Join all possible pairs in both lists
    	for(AirlineRowWritable outflight : outFlights) {
        	for(AirlineRowWritable inflight : inFlights) {
        		if(inflight.getOriginAirportId().equals(outflight.getDestAirportId())) {
        			//mid-journey we don't want to go back to where we came from
        			continue;
        		}
        		String reachDate =inflight.getFlightDate().toString();
        		String leaveDate = outflight.getFlightDate().toString();
        		
        		
        		//LocalTime reachTime =  LocalTime.parse(inflight.getArrivalTime().toString(),DateTimeFormatter.ofPattern("HHmm"));
        		//LocalTime leaveTime =  LocalTime.parse(outflight.getDepartureTime().toString(),DateTimeFormatter.ofPattern("HHmm"));
        		
        		LocalDateTime reachTime =  LocalDateTime.parse(reachDate+"-"+inflight.getArrivalTime().toString(),DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmm"));
        		LocalDateTime leaveTime =  LocalDateTime.parse(leaveDate+"-"+outflight.getDepartureTime().toString(),DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmm"));
        		
        		Long timeSpentInCity = Duration.between(reachTime, leaveTime).toHours();
        		if( timeSpentInCity>=10 && timeSpentInCity <=72) {
        		
        		
        		//if((ChronoUnit.HOURS.between(reachTime, leaveTime) >=10 && reachDate.equals(leaveDate))
        		//		|| leaveDate.isAfter(reachDate) ) {
        			
        			context.write(new Text(inflight.toString()), new Text(outflight.toString()) );
        		}
        		
        	}
    	}
    		
    		
        
    }
}