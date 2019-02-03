package airlineAnalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.util.StringJoiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AirlineRowWritable implements WritableComparable<AirlineRowWritable>{
	private Text flightDate;
	private Text originAirportId;
	private Text originCity;
	private Text destAirportId;
	private Text destCity;
	private Text departureTime;
	private Text arrivalTime;
	private DoubleWritable flightDuration;
	private Text tag;
	private Text pastItinerary;
	

	public AirlineRowWritable() {
		flightDate = new Text();
		originAirportId=new Text();
		originCity=new Text();
		destAirportId=new Text();
		destCity=new Text();
		departureTime=new Text();
		arrivalTime = new Text();
		flightDuration=new DoubleWritable(0);
		tag=new Text("");
		pastItinerary = new Text();
	}

	public AirlineRowWritable(Text flDate,	Text oAirportId,	 Text oCity,  Text dAirportId,
			Text dCity, Text depime, Text arrTime, DoubleWritable flDuration, Text flightTag, Text pastInfo) {
		flightDate = flDate;
		originAirportId=oAirportId;
		originCity=oCity;
		destAirportId=dAirportId;
		destCity=dCity;
		departureTime=depime;
		arrivalTime = arrTime;
		flightDuration=flDuration;
		tag=flightTag;
		pastItinerary = pastInfo;
	}
	
	public AirlineRowWritable(AirlineRowWritable row) {
		flightDate = new Text(row.flightDate);
		originAirportId=new Text(row.originAirportId);
		originCity=new Text(row.originCity);
		destAirportId=new Text(row.destAirportId);
		destCity=new Text(row.destCity);
		departureTime=new Text(row.departureTime);
		arrivalTime = new Text(row.arrivalTime);
		flightDuration=new DoubleWritable(row.flightDuration.get());
		tag=new Text(row.tag);
		pastItinerary = new Text(row.pastItinerary);
	}
	
	

	public Text getOriginAirportId() {
		return originAirportId;
	}

	public Text getDestAirportId() {
		return destAirportId;
	}
	
	public Text getDepartureTime() {
		return departureTime;
	}
	
	public Text getArrivalTime() {
		return arrivalTime;
	}
	
	public String getTag() {
		return tag.toString();
	}

	public Text getFlightDate() {
		return flightDate;
	}
	
	public String getPastItinerary() {
		return pastItinerary.toString();
	}
	
	public void setTag(String flightTag) {
		this.tag = new Text(flightTag);
	}
	
	
	
	public AirlineRowWritable withTag(String flightTag) {
		return new AirlineRowWritable(this.flightDate,
				this.originAirportId,this.originCity, 
				this.destAirportId,this.destCity,
				this.departureTime,this.arrivalTime,
				this.flightDuration,new Text(flightTag),
				this.pastItinerary);
		
	}
	
	public Text getHomeAirport() {
		String[] pastRoute = this.getPastItinerary().split("----");
		if(pastRoute!=null && pastRoute.length>0) {
			String[] firstFlight = pastRoute[0].split(",");
			if(firstFlight!=null && firstFlight.length>0) {
				return new Text(firstFlight[1]);
			}
		}
		return null;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		flightDate.write(out); 
		originAirportId.write(out);
		originCity.write(out);
		destAirportId.write(out);
		destCity.write(out);
		departureTime.write(out);
		arrivalTime.write(out); 
		flightDuration.write(out);
		tag.write(out);
		pastItinerary.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		flightDate.readFields(in); 
		originAirportId.readFields(in);
		originCity.readFields(in);
		destAirportId.readFields(in);
		destCity.readFields(in);
		departureTime.readFields(in);
		arrivalTime.readFields(in); 
		flightDuration.readFields(in);
		tag.readFields(in);
		pastItinerary.readFields(in);

	}

	@Override
	public int compareTo(AirlineRowWritable o) {
		LocalDate thisDate = LocalDate.parse(flightDate.toString());
		LocalDate thatDate = LocalDate.parse(o.flightDate.toString());

		if(thisDate.compareTo(thatDate)==0) {
			return this.originAirportId.compareTo(o.originAirportId);
		}
		return thisDate.compareTo(thatDate);
	}

	@Override
	public String toString() {
		StringJoiner sj = new StringJoiner(",");
	
		sj.add(flightDate.toString());
		sj.add(originAirportId.toString());
		sj.add(originCity.toString());
		sj.add(destAirportId.toString());
		sj.add(destCity.toString());
		sj.add(departureTime.toString());
		sj.add(arrivalTime.toString());
		sj.add(flightDuration.toString());
		//sj.add(tag.toString());
		//sj.add("["+ pastItinerary.toString()+"]");
		String output = sj.toString();
		if(! pastItinerary.toString().isEmpty()) {
			output = pastItinerary.toString()+"----" + sj.toString();
		}
		return output;

	}


}
