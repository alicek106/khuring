package creativedesign.namu.assistance;

import java.sql.SQLException;

import org.eclipse.paho.client.mqttv3.MqttException;
import com.mongodb.MongoException;

public class PrintConsole {
	private static boolean printFlag = true;
	
	public static final String PRINT_CONNECT 		= "++++++++++++++++++++++ CONNECT ++++++++++++++++++++++";
	public static final String PRINT_DISCONNECT 	= "+++++++++++++++++++++ DISCONNECT ++++++++++++++++++++";
	public static final String PRINT_SUBSCRIBE 		= "+++++++++++++++++++++ SUBSCRIBE +++++++++++++++++++++";
	public static final String PRINT_PUBLISH		= "++++++++++++++++++++++ PUBLISH ++++++++++++++++++++++";
	public static final String PRINT_MESSAGE 		= "++++++++++++++++++++++ MESSAGE ++++++++++++++++++++++";
	public static final String PRINT_ERROR 			= "+++++++++++++++++++++++ ERROR +++++++++++++++++++++++";
	
	public PrintConsole(boolean flag) {
		printFlag = flag;
	}
	
	public static void setFlag (boolean flag) {
		printFlag = flag;
	}

	public static void printStart(String title) {
		if (printFlag) {
			//System.out.println("++++++++++++++++++++++++++"+title+"++++++++++++++++++++++++++++");
			System.out.println("\n" + title);
		}
	}

	public static void printMid(String context) {
		if (printFlag) {
			System.out.println("+ " + context);
		}
	}

	public static void printEnd() {
		if (printFlag) {
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
		}
	}

	public static void printWhole(String title, String[] context) {
		if (printFlag) {
			//printStart
			System.out.println("\n" + title);
			//printMid
			for (int i = 0; i < context.length; i++) {
				System.out.println("+ " + context[i]);
			}
			//printEnd
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
		}
	}

	// print MqttException
	public static void printError(MqttException e) {
		if (printFlag) {
			System.out.println("++++++++++++++++++++++MQTT ERROR+++++++++++++++++++++++");
			System.out.println("+ reason: " + e.getReasonCode());
			System.out.println("+ msg: " + e.getMessage());
			System.out.println("+ loc: " + e.getLocalizedMessage());
			System.out.println("+ cause: " + e.getCause());
			System.out.println("+ excep: " + e);
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			e.printStackTrace();
		}
	}
	
	// print MongoException
	public static void printError(MongoException e) {
		if (printFlag) {
			System.out.println("+++++++++++++++++++++MONGO ERROR+++++++++++++++++++++++");
			System.out.println("+ code: " + e.getCode());
			System.out.println("+ msg: " + e.getMessage());
			System.out.println("+ loc: " + e.getLocalizedMessage());
			System.out.println("+ cause: " + e.getCause());
			System.out.println("+ excep: " + e);
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			e.printStackTrace();
		}
	}
	
	// print SQLException
	public static void printError(SQLException e) {
		if (printFlag) {
			System.out.println("+++++++++++++++++++++++SQL ERROR+++++++++++++++++++++++");
			System.out.println("+ code: " + e.getErrorCode());
			System.out.println("+ msg: " + e.getMessage());
			System.out.println("+ loc: " + e.getLocalizedMessage());
			System.out.println("+ cause: " + e.getCause());
			System.out.println("+ excep: " + e);
			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			e.printStackTrace();
		}
	}
}
