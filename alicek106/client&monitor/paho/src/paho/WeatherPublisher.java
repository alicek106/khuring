package paho;

import java.io.BufferedReader;

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

public class WeatherPublisher {

	static MqttClient client = null;

	final static String MQTT_BROKER_IP = "tcp://163.180.118.59:1883";

	// publisher id and pw
	final static String MONITOR_USERNAME = "test5";
	final static String MONITOR_PASSWORD = "test1234";

	
	public static void main(String[] args) throws Exception{

		client = new MqttClient(MQTT_BROKER_IP, // URI
				MqttClient.generateClientId(), // ClientId
				new MemoryPersistence());

		MqttConnectOptions connOpts = new MqttConnectOptions();
		connOpts.setCleanSession(true);
		connOpts.setUserName(MONITOR_USERNAME);
		connOpts.setPassword(MONITOR_PASSWORD.toCharArray());

		client.connect(connOpts);
		System.out.println("[INFO] Weather publisher is connected to Broker successfully.");
		JSONParser parser = new JSONParser();
		while(true){
			String result = getWeatherInfo();
			//System.out.println(result);
			JSONObject obj = (JSONObject)parser.parse(result);
			System.out.println("TEST : " + obj);
			Double temp = (Double) obj.get("temperature");
			Double hum = (Double) obj.get("humidity");
			String words = String.format("오늘 교정의 온도는 %2.2f \n습도는 %2.2f입니다.\n좋은 하루 보내세요!", temp, hum);
			String group = new String("온습도".getBytes("utf-8"), "utf-8");
			words = new String(words.getBytes("utf-8"), "utf-8");
			System.out.println(words);
			JSONObject jsob = new JSONObject();
			jsob.put("group", group);
			jsob.put("title", "weatherInfo");
			jsob.put("contents", words);

			MqttMessage message = new MqttMessage(jsob.toString().getBytes());
			message.setQos(1);

			System.out.println("[INFO] sending weather info...");
			client.publish("NAMUCLIENT/group/topic/send", message);
			System.out.println("[INFO] sent weather info");
			
			Thread.sleep(10000);
		}
	}
	
	private static String getWeatherInfo(){
		try {
			URL url = new URL("http://163.180.118.57:5050/sensor");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();

			conn.setDoOutput(true);
			conn.setRequestMethod("GET");

			OutputStream os = conn.getOutputStream();

			os.flush();

			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String decodedString;
			decodedString = in.readLine();
			
			in.close();
			
			return decodedString;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}
}
