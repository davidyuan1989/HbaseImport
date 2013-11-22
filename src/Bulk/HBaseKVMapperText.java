package Bulk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Mapper Class
 */
public class HBaseKVMapperText extends Mapper<LongWritable, Text, Text, Text> {

	// protected String filenameKey;

	// private MultipleOutputs<ImmutableBytesWritable,KeyValue> mos;

	private JSONParser parser = new JSONParser();
	private String bucket;

	// Set column family name
	// final static byte[] SRV_COL_FAM = "tw".getBytes();
	// Number of fields in text file

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		bucket = Utility.getRandomizedBucket();
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Boolean has_retweet = true;
		JSONObject json;

		try {
			json = (JSONObject) parser.parse(value.toString());

			String time = Utility.convertDateFormat(json.get("created_at")
					.toString());
			String id = json.get("id").toString();
			String text = json.get("text").toString();

			JSONObject user = (JSONObject) json.get("user");
			String user_id = user.get("id").toString();

			JSONObject re_status = (JSONObject) json.get("retweeted_status");
			String re_id = null, re_user_id = null;
			JSONObject re_user = null;
			if (re_status != null) {
				re_id = re_status.get("id").toString();
				re_user = (JSONObject) re_status.get("user");
				re_user_id = re_user.get("id").toString();
			} else {
				has_retweet = false;
			}

			String timeKey = String.format("%s", bucket + HColumnEnum.splitWord + time
					+ HColumnEnum.splitWord + id + HColumnEnum.splitWord
					+ "txt");
			
			if (!text.equals("")) {
				// mos.write(HColumnEnum.TimeTable,timeKey,
				// kv,HColumnEnum.TimeOutput);
				context.write(new Text(timeKey), new Text(HColumnEnum.TXT
						+ HColumnEnum.splitWord + text));
			}

			// mos.write(HColumnEnum.UserTable,userKey,
			// kv,HColumnEnum.UserOutput);

			if (!user_id.equals("")) {

				// mos.write(HColumnEnum.UserTable,userKey,
				// kv,HColumnEnum.UserOutput);
				String userKey = String.format("%s", bucket + HColumnEnum.splitWord + user_id
						+ HColumnEnum.splitWord + id + HColumnEnum.splitWord
						+ "uid");
				context.write(new Text(userKey), new Text(HColumnEnum.USER_ID
						+ HColumnEnum.splitWord + user_id));
			}

			if (has_retweet) {
				if (!re_id.equals("")) {
					// mos.write(HColumnEnum.UserTable,userKey,
					// kv,HColumnEnum.UserOutput);
					String userKey = String.format("%s", bucket + HColumnEnum.splitWord + user_id
							+ HColumnEnum.splitWord + id + HColumnEnum.splitWord
							+ "r_id");
					context.write(new Text(userKey), new Text(HColumnEnum.RETEWEET_ID
							+ HColumnEnum.splitWord + re_id));
				}
				if (!re_user_id.equals("")) {
					// mos.write(HColumnEnum.UserTable,userKey,
					// kv,HColumnEnum.UserOutput);
					String userKey = String.format("%s", bucket + HColumnEnum.splitWord + user_id
							+ HColumnEnum.splitWord + id + HColumnEnum.splitWord
							+ "r_uid");
					context.write(new Text(userKey), new Text(HColumnEnum.REUSER_ID
							+ HColumnEnum.splitWord + re_user_id));
				}
			}

			String userKey = String.format("%s", bucket + HColumnEnum.splitWord + user_id
					+ HColumnEnum.splitWord + id + HColumnEnum.splitWord
					+ "r_st");
			
			context.write(new Text(userKey), new Text(HColumnEnum.HAS_RETEWEET
					+ HColumnEnum.splitWord + has_retweet.toString()));

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
		}
		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);
	}
}
