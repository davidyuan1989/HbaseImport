package Bulk;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.jruby.lexer.yacc.RubyYaccLexer.Keyword;


public class HBaseHFileReducer extends
		Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {
	
	final static byte[] SRV_COL_FAM_TW = "tw".getBytes();
	
	final static byte[] SRV_COL_FAM_RTW = "rtw".getBytes();
	
	ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
	
//	private MultipleOutputs<ImmutableBytesWritable,KeyValue> mos;
	KeyValue kv;
	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
//		mos = new MultipleOutputs<ImmutableBytesWritable,KeyValue>(context);
		
	}
	
	protected void reduce(ImmutableBytesWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		String value = "";
		//createRowKey(new String(key.get()));
		while (values.iterator().hasNext()) {
			value = values.iterator().next().toString();
			if (value != null && !"".equals(value)) {
				Boolean isTimeTable = createKeyValueInTimeTable(key,value.toString());
				if (kv != null && isTimeTable){
					context.write(rowKey, kv);
				}
				else if(kv != null){
					context.write(rowKey, kv);
				}
			}
		}
	}
	
	private void createRowKey(String key){
		try {
			String[] keyword = key.split(HColumnEnum.splitWord);
			rowKey.set(String.format("%s", keyword[0]+"|"+keyword[1]+"|"+keyword[2]).getBytes());
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println("key:"+key);
		}
	}
	
	private boolean createKeyValueInTimeTable(ImmutableBytesWritable rowKey, String str) {
		String[] keyword = str.split(HColumnEnum.splitWord);
		if(keyword[0].equals(HColumnEnum.TXT)){
			kv = new KeyValue(rowKey.get(), SRV_COL_FAM_TW,
					HColumnEnum.SRV_COL_TXT.getColumnName(), keyword[1].getBytes());
			return true;
		}
		else if(keyword[0].equals(HColumnEnum.HAS_RETEWEET)){
			kv = new KeyValue(rowKey.get(), SRV_COL_FAM_RTW,
					HColumnEnum.SRV_COL_RST.getColumnName(),keyword[1].getBytes());
		}
		else if(keyword[0].equals(HColumnEnum.USER_ID)){
			kv = new KeyValue(rowKey.get(), SRV_COL_FAM_RTW,
					HColumnEnum.SRV_COL_UID.getColumnName(), keyword[1].getBytes());
		}
		else if(keyword[0].equals(HColumnEnum.RETEWEET_ID)){
			kv = new KeyValue(rowKey.get(), SRV_COL_FAM_RTW,
					HColumnEnum.SRV_COL_RID.getColumnName(), keyword[1].getBytes());
		}
		else if(keyword[0].equals(HColumnEnum.REUSER_ID)){
			kv = new KeyValue(rowKey.get(), SRV_COL_FAM_RTW,
					HColumnEnum.SRV_COL_RID.getColumnName(), keyword[1].getBytes());
		}
		return false;
	}
}
