package com.autohome.adrd.algo.keyword_targeting.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper;
/**
 * Hadoop Task Base Processor
 * author : wang chao
 */
public abstract class AbstractProcessor implements Tool {

	protected Configuration _conf = new Configuration();
	protected Options _options = new Options();
	protected String _input = null;
	protected String _output = null;
	protected String _numReduce = null;
	protected String _jarpath = null;
	protected Boolean _input_rcfile = false;
	protected Boolean _input_seq = false;
	protected Boolean _output_seq = false;
	protected static final Log LOG = LogFactory.getLog(AbstractProcessor.class.getName());

	public AbstractProcessor() {
		setupOptions();
	}

	@Override
	public int run(String[] args) throws Exception {
		parseArgv(args);	
		setJars();
		
		//256M split
		String value = Long.toString(4 * 67108864L);
        _conf.set("mapred.min.split.size", value);
        _conf.set("table.input.split.minSize", value);
        
		Job job = new Job(_conf, this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());

		// these four configuration can be overwritted in configJob()
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if(_input_rcfile)
		{
			MapReduceInputFormatWrapper.setInputFormat(RCFileInputFormat.class, job);
			FileInputFormat.setInputPaths(job, _input);
		}
		else if(_input_seq)
		{
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPaths(job, _input);
		}
		else
		{
			//MultipleInputs.addInputPath();
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPaths(job, _input);
		}
		
		if (_output_seq) 
		{
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		} 
		else 
		{
			job.setOutputFormatClass(TextOutputFormat.class);
		}
		Path outputPath = new Path(_output);
		FileSystem fs = FileSystem.get(outputPath.toUri(), _conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (_numReduce != null) {
			int numReduce = Integer.parseInt(_numReduce);
			job.setNumReduceTasks(numReduce);
		} else {
			job.setNumReduceTasks(0);
		}

		configJob(job);

		// List non-default properties to the terminal and exit.
		// Configuration.main(null);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return _conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		_conf = conf;

	}

	public void setJars() throws IOException {
		File libFile = new File(_jarpath);
		String libFiles[] = libFile.list();
		if(libFiles == null)
			return;
		for (String name : libFiles) {
			String path = "";
			if (name.endsWith(".jar")) {
				path = "file://" + _jarpath + name;
				addTmpJar(new Path(path), _conf);
			}
		}
	}

	/**
	 * Implement this function to specify your task's unique configuration
	 * 
	 * @param job
	 */
	protected abstract void configJob(Job job);

	@SuppressWarnings("static-access")
	private Option createOption(String name, String desc, String argName,
			int max, boolean required) {
		return OptionBuilder.withArgName(argName).hasArgs(max).withDescription(
				desc).isRequired(required).create(name);
	}

	@SuppressWarnings("static-access")
	private Option createBoolOption(String name, String desc) {
		return OptionBuilder.withDescription(desc).create(name);
	}

	protected void setupOptions() {
		Option input = createOption("input", "DFS input file(s) for the Map step", "path", Integer.MAX_VALUE, true);
		Option output = createOption("output", "DFS output directory for the Reduce step", "path", 1, true);
		Option numReduce = createOption("numReduce", "Optional.", "spec", 1, false);
		Option jarpath = createOption("jarpath", "jar directory.", "path", 1, false);
		Option input_seq = createBoolOption("input_seq", "is input sequenced file");
		Option output_seq = createBoolOption("output_seq", "should output sequenced");
		Option input_rcfile = createBoolOption("input_rcfile", "is input is rcfile format file");
		Option help = createBoolOption("help", "print this help message");
		_options.addOption(input).addOption(output).addOption(numReduce).addOption(jarpath).addOption(input_rcfile).addOption(help).addOption(input_seq).addOption(output_seq);
	}
 
	protected void parseArgv(String[] args) {
		CommandLine cmdLine = null;
		try {
			cmdLine = new BasicParser().parse(_options, args);
		} catch (Exception oe) {
			exitUsage(true);
		}

		if (cmdLine != null) {
			if (cmdLine.hasOption("help")) {
				exitUsage(true);
			}
			_input = cmdLine.getOptionValue("input");
			_output = cmdLine.getOptionValue("output");
			_numReduce = cmdLine.getOptionValue("numReduce");
			_jarpath = cmdLine.getOptionValue("jarpath");
			_input_rcfile = cmdLine.hasOption("input_rcfile") ? true : false;
			_input_seq = cmdLine.hasOption("input_seq") ? true : false;
			_output_seq = cmdLine.hasOption("output_seq") ? true : false;
		} else {
			exitUsage(true);
		}

		if (_input == null) {
			fail("Required argument: -input <path>");
		}
		if (_output == null) {
			fail("Required argument: -output <path>");
		}
		if (_jarpath == null) {
			_jarpath="/root/lib/";
		}
	}

	protected static void exitUsage(boolean generic) {
		System.out.println("Usage: $HADOOP_HOME/bin/hadoop jar jarFile Launcher Processor [Options]");
		System.out.println("Options:");
		System.out.println("  -input    <path>     inputs, seperated by comma");
		System.out.println("  -output   <path>     output directory");
		System.out.println("  -numReduce <num>  optional");
		System.out.println("  -input_rcfile inputs is rcfile format, optional");
		System.out.println("  -input_seq inputs is sequenced, optional");
		System.out.println("  -output_seq outputs should be sequenced, optional");
		System.out.println("  -help");

		if (generic) {
			GenericOptionsParser.printGenericCommandUsage(System.out);
		}
		fail("");
	}

	protected static void addTmpJar(Path jarPath, Configuration conf) throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.get(jarPath.toUri(), conf);
		String newJarPath = jarPath.makeQualified(fs).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}

	private static void fail(String message) {
		System.err.println(message);
		System.exit(-1);
	}
}
