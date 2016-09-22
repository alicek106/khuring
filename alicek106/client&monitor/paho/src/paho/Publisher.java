package paho;

import java.util.ArrayList;
import java.util.HashMap;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Publisher {
	static ArrayList<String> publisherList = new ArrayList<String>();

	static MqttClient client = null;

	final static String MQTT_BROKER_IP = "tcp://163.180.117.247:1883";

	// publisher id and pw
	final static String MONITOR_USERNAME = "test3";
	final static String MONITOR_PASSWORD = "test1234";

	public static void main(String[] args) {

		try {
			client = new MqttClient(MQTT_BROKER_IP, // URI
					MqttClient.generateClientId(), // ClientId
					new MemoryPersistence());

			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			connOpts.setUserName(MONITOR_USERNAME);
			connOpts.setPassword(MONITOR_PASSWORD.toCharArray());

			client.connect(connOpts);
			System.out.println("[INFO] publisher is connected to Broker successfully.");

			client.setCallback(new MqttCallback() {

				@Override
				public void connectionLost(Throwable cause) {
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
				}

				@Override
				public void messageArrived(String arg0, MqttMessage arg1) throws Exception {

				}

			});

			JSONObject jsob = new JSONObject();
			jsob.put("group", "testgroup");
			jsob.put("interval", 10);

			MqttMessage message = new MqttMessage(jsob.toString().getBytes());
			message.setQos(1);

			System.out.println("[INFO] sending broadcast request..");
			client.publish("NAMUCLIENT/user/keepalive", message);
			System.out.println("[INFO] sent braodcast request.");
			
			
			return;
			
			/* broad cast 테스트 */
			
			/*
			
			
			JSONObject jsob = new JSONObject();
			jsob.put("group", "HighQuilityRestaurant");
			jsob.put("title", "this is title!");
			jsob.put("contents", "this is test message!");

			MqttMessage message = new MqttMessage(jsob.toString().getBytes());
			message.setQos(1);

			System.out.println("[INFO] sending broadcast request..");
			client.publish("NAMUCLIENT/group/topic/send", message);
			System.out.println("[INFO] sent braodcast request.");
			
			
			return;*/
			
			/* group register 테스트 */
			
			/*
			
			JSONArray jsonarray = new JSONArray();
			jsonarray.add("test");
			
			JSONObject jsob = new JSONObject();
			jsob.put("group", "lol2");
			jsob.put("member", jsonarray);

			System.out.println(jsob);
			
			MqttMessage message = new MqttMessage(jsob.toString().getBytes());
			message.setQos(1);

			System.out.println("[INFO] sending broadcast request..");
			client.publish("NAMUCLIENT/group/register", message);
			System.out.println("[INFO] sent braodcast request.");
			*/
			// 통신할 토픽들

		}

		catch (MqttException e) {
			e.printStackTrace();
		}

	}

}
