package Bulk;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;


public class Utility {

	public static String convertDateFormat(String utcFormatDate) {
		try {
			
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
		calendar.setTime(sdf.parse(utcFormatDate));
		Date date = calendar.getTime();
		SimpleDateFormat outsdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return outsdf.format(date); 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public static String getRandomizedBucket() {
		return "0" + String.valueOf(new Random().nextInt(9));
	}
}
