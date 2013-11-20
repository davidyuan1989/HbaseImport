package Bulk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Mapper Class
 */
public class HBaseKVMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	
	//protected String filenameKey;
	private RecordWriter<ImmutableBytesWritable, KeyValue> writerUser;
	private RecordWriter<ImmutableBytesWritable, KeyValue> writerTime;
	private JSONParser parser = new JSONParser();
	private String bucket;
	
	// Set column family name
	final static byte[] SRV_COL_FAM = "tweet".getBytes();
	// Number of fields in text file
	final static int NUM_FIELDS = 3;

	CSVParser csvParser = new CSVParser();
	String tableName = "";

	ImmutableBytesWritable userKey = new ImmutableBytesWritable();
	ImmutableBytesWritable timeKey = new ImmutableBytesWritable();
	KeyValue kv;
	
	

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
		tableName = c.get("hbase.table.name");
		String parameter2 = c.get("parameter2");
		bucket ="00";
		setOutPath(context);
	}
	
	private void setOutPath(Context context)throws IOException, InterruptedException{
        // base output folder
        Path baseOutputPath = FileOutputFormat.getOutputPath(context);
        // output file name
        final Path outputFilePathUser = new Path(baseOutputPath, HColumnEnum.UserOutput);
        final Path outputFilePathTime = new Path(baseOutputPath, HColumnEnum.TimeOutput);
        // We need to override the getDefaultWorkFile path to stop the file being created in the _temporary/taskid folder
        TextOutputFormat<ImmutableBytesWritable, KeyValue> tofUser = 
        		new TextOutputFormat<ImmutableBytesWritable, KeyValue>() {
            @Override
            public Path getDefaultWorkFile(TaskAttemptContext context,
                    String extension) throws IOException {
                return outputFilePathUser;
            }
        };
        // We need to override the getDefaultWorkFile path to stop the file being created in the _temporary/taskid folder
        TextOutputFormat<ImmutableBytesWritable, KeyValue> tofTime =
        		new TextOutputFormat<ImmutableBytesWritable, KeyValue>() {
            @Override
            public Path getDefaultWorkFile(TaskAttemptContext context,
                    String extension) throws IOException {
                return outputFilePathTime;
            }
        };
        // create a record writer that will write to the desired output subfolder
        writerUser = tofUser.getRecordWriter(context);
        writerTime = tofTime.getRecordWriter(context);
	}
	
	
	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Boolean has_retweet = true;
		JSONObject json;
		
		try {
			json = (JSONObject) parser.parse(value.toString());
			
			String time = json.get("created_at").toString();
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
			
			userKey.set(String.format("%s", bucket+"|"+time+"|"+id).getBytes());
			if(!text.equals("")){
				kv = new KeyValue(userKey.get(), SRV_COL_FAM,
						HColumnEnum.SRV_COL_TXT.getColumnName(), text.getBytes());
				writerTime.write(userKey, kv);
			}

			timeKey.set(String.format("%s", bucket+"|"+user_id+"|"+id).getBytes());
			
			kv = new KeyValue(userKey.get(), SRV_COL_FAM,
					HColumnEnum.SRV_COL_RST.getColumnName(), has_retweet
							.toString().getBytes());
			writerUser.write(timeKey, kv);
			
			if(!user_id.equals("")){
				kv = new KeyValue(userKey.get(), SRV_COL_FAM,
						HColumnEnum.SRV_COL_UID.getColumnName(), text.getBytes());
				writerTime.write(userKey, kv);
			}
			
			if(has_retweet){
				if(!re_id.equals("")){
					kv = new KeyValue(userKey.get(), SRV_COL_FAM,
							HColumnEnum.SRV_COL_RID.getColumnName(), re_id.getBytes());
					writerTime.write(userKey, kv);
				}
				if(!re_user_id.equals("")){
					kv = new KeyValue(userKey.get(), SRV_COL_FAM,
							HColumnEnum.SRV_COL_RID.getColumnName(), re_id.getBytes());
					writerTime.write(userKey, kv);
				}
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
		}
		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);
	}
}
