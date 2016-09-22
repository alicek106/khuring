package creativedesign.namu.assistance;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AssistClass {
	public static String getTimeStamp() {
		Date date = new Date();
	    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");

	    return df.format(date);
	}
}
