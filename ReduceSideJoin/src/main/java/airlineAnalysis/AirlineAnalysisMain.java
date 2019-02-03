package airlineAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AirlineAnalysisMain extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(AirlineAnalysisMain.class);


	@Override
	public int run(final String[] args) throws Exception {

		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "AirlineAnalysisMain");
		job.setJarByClass(AirlineAnalysisMain.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		// setting mapper class
		job.setMapperClass(PreprocessMapper.class);

		// setting reducer class
		job.setReducerClass(PreprocessReducer.class);        
		//job.setNumReduceTasks(0);

		// setting the output key class
		job.setOutputKeyClass(AirlineRowWritable.class);
		// setting the output value class
		job.setOutputValueClass(Text.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"Processed"));


		if(! job.waitForCompletion(true)) {
			throw new Exception("Preprocessing Job failed");
		}
		
		// JOB2: First HOP
		final Job firstHopJob = Job.getInstance(conf, "First Hop");
		firstHopJob.setJarByClass(AirlineAnalysisMain.class);
		final Configuration firstHopJobConf = firstHopJob.getConfiguration();
		firstHopJobConf.set("mapreduce.output.textoutputformat.separator", "----");


		// setting mapper class
		firstHopJob.setMapperClass(FirstHopMapper.class);

		// setting reducer class
		firstHopJob.setReducerClass(FirstHopReducer.class);        

		// setting the output key class
		firstHopJob.setOutputKeyClass(Text.class);
		// setting the output value class
		firstHopJob.setOutputValueClass(AirlineRowWritable.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(firstHopJob, new Path(args[1]+"Processed"));
		FileOutputFormat.setOutputPath(firstHopJob, new Path(args[1]+"1"));

		if(! firstHopJob.waitForCompletion(true)) {
			throw new Exception("Preprocessing Job failed");
		}

		// JOB3: Second HOP
		final Job secondHopJob = Job.getInstance(conf, "Second Hop");
		secondHopJob.setJarByClass(AirlineAnalysisMain.class);
		final Configuration secondHopJobConf = secondHopJob.getConfiguration();
		secondHopJobConf.set("mapreduce.output.textoutputformat.separator", "----");


		// setting mapper class
		MultipleInputs.addInputPath(secondHopJob, new Path(args[1]+"1"), TextInputFormat.class,SecondHopLeftMapper.class);
		MultipleInputs.addInputPath(secondHopJob, new Path(args[1]+"Processed"), TextInputFormat.class,SecondHopRightMapper.class);
		// setting reducer class
		secondHopJob.setReducerClass(FirstHopReducer.class);        

		// setting the output key class
		secondHopJob.setOutputKeyClass(Text.class);
		// setting the output value class
		secondHopJob.setOutputValueClass(AirlineRowWritable.class);

		FileOutputFormat.setOutputPath(secondHopJob, new Path(args[1]+"2"));
		
		if(! secondHopJob.waitForCompletion(true)) {
			throw new Exception("Second hop Job failed");
		}

		
		// JOB4: Third HOP back home
		final Job thirdHopJob = Job.getInstance(conf, "Third Hop");
		thirdHopJob.setJarByClass(AirlineAnalysisMain.class);
		final Configuration thirdHopJobConf = thirdHopJob.getConfiguration();
		thirdHopJobConf.set("mapreduce.output.textoutputformat.separator", "----");


		// setting mapper class
		MultipleInputs.addInputPath(thirdHopJob, new Path(args[1]+"2"), TextInputFormat.class,SecondHopLeftMapper.class);
		MultipleInputs.addInputPath(thirdHopJob, new Path(args[1]+"Processed"), TextInputFormat.class,SecondHopRightMapper.class);
		// setting reducer class
		thirdHopJob.setReducerClass(ThirdHopReducer.class);        

		// setting the output key class
		thirdHopJob.setOutputKeyClass(Text.class);
		// setting the output value class
		thirdHopJob.setOutputValueClass(AirlineRowWritable.class);
		FileOutputFormat.setOutputPath(thirdHopJob, new Path(args[1]+"3"));


		return thirdHopJob.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new AirlineAnalysisMain(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}


