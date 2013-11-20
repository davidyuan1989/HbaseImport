
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Bulk.HBaseKVMapper;
import Bulk.HColumnEnum;

/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
public class Driver {
	public static void main(String[] args) throws Exception {

		// HBase Configuration
		Configuration conf = new Configuration();

		// Pass parameters to Mad Reduce
		conf.set("hbase.table.name1", "timeTable");
		conf.set("hbase.table.name2", "userTable");
		//conf.set("parameter2","pass parameter example" );
		
		// Workaround
		SchemaMetrics.configureGlobally(conf);

		// Load hbase-site.xml
		HBaseConfiguration.addHbaseResources(conf);

		// Create the job
		Job job = new Job(conf, "HBase Bulk Import");

		job.setJarByClass(HBaseKVMapper.class);
		job.setMapperClass(HBaseKVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		// Get the table
		HTable userTable = new HTable(conf, "userTable");
		// Get the table
		HTable timeTable = new HTable(conf, "timeTable");

		// Auto configure partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, userTable);
		
		// Auto configure partitioner and reducer
		//HFileOutputFormat.configureIncrementalLoad(job, userTable);

		// Save output path and input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Wait for HFiles creations
		job.waitForCompletion(true);

		// Load generated HFiles into table
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(new Path(HColumnEnum.UserOutput), userTable);
		loader.doBulkLoad(new Path(HColumnEnum.TimeOutput), timeTable);
		
	}
}