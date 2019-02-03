package airlineAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PreprocessReducer extends Reducer<AirlineRowWritable, Text, Text, Text> {
   

    @Override
    public void reduce(final AirlineRowWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
       
        context.write(new Text(key.toString()), new Text(""));
    }
}
