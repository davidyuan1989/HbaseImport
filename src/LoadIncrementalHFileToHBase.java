import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;  
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;  
  
public class LoadIncrementalHFileToHBase {   
  
    public static void main(String[] args) throws IOException {  
        Configuration conf = HBaseConfiguration.create();  
        HTable table = new HTable(conf, args[0]);
        SchemaMetrics.configureGlobally(conf);
        LoadIncrementalHFiles loader;
		try {
			loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(new Path(args[1]), table); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }  
  
}  