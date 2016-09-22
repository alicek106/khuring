package paho;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
// import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import alicek106.JSONParser.JSONParserAssist;

public class client {
static ArrayList<String> publisherList = new ArrayList<String>();
	
	static MqttClient client = null;
	
	final static String MQTT_BROKER_IP = "tcp://163.180.117.247:1883";
	
	// publisher test
	final static String MONITOR_USERNAME = "test4";
	final static String MONITOR_PASSWORD = "test1234";

	public static void subscribe(String group){

		JSONObject jsob = new JSONObject();
		jsob.put("group", group);

		MqttMessage message = new MqttMessage(jsob.toString().getBytes());
		message.setQos(1);

		System.out.println("[INFO] sending subscribe message...");
		
		try {
			client.publish("NAMUCLIENT/group/subscribe", message);
		} 
		
		catch (MqttException e) {
			e.printStackTrace();
		}
		System.out.println("[INFO] sent subscribe message");
	}
	
	public static void unsubscribe(String group){
		JSONObject jsob = new JSONObject();
		jsob.put("group", group);

		MqttMessage message = new MqttMessage(jsob.toString().getBytes());
		message.setQos(1);

		System.out.println("[INFO] sending unsubscribe message...");
		
		try {
			client.publish("NAMUCLIENT/group/unsubscribe", message);
		} 
		
		catch (MqttException e) {
			e.printStackTrace();
		}
		System.out.println("[INFO] sent unsubscribe message");
	}
	
	public static void getSubscribingGroups(){
		JSONObject jsob = new JSONObject();
		
		jsob.put("username", MONITOR_USERNAME);
		
		MqttMessage message = new MqttMessage(jsob.toString().getBytes());
		message.setQos(1);

		System.out.println("[INFO] getting subscribe information...");
		
		try {
			client.publish("NAMUCLIENT/group/list", message);
		} 
		
		catch (MqttException e) {
			e.printStackTrace();
		}
		System.out.println("[INFO] got subscribe info!");
	}
	static boolean test = false;
	static int interval = 0;
	
	public static void main(String[] args) {

		
		
		try 
		{
			client = new MqttClient( 
					MQTT_BROKER_IP, //URI 
				    MqttClient.generateClientId(), //ClientId 
				    new MemoryPersistence());
			
			MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(MONITOR_USERNAME);
            connOpts.setPassword(MONITOR_PASSWORD.toCharArray());
            
			client.connect(connOpts);
			
			System.out.println("[INFO] client is connected to Broker successfully.");
			
			client.setCallback(new MqttCallback() {
			 
			            @Override
			            public void connectionLost(Throwable cause) { 
			            }

						@Override
						public void deliveryComplete(IMqttDeliveryToken arg0) {		
						}

						@Override
						public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
							System.out.println(arg0 + " / " + arg1);
							if(arg0.equals("NAMUCLIENT/keepalive")){
								test = true;
								
								
								JSONObject jsob = JSONParserAssist.parseString(arg1.toString());
								interval = ((Long)jsob.get("interval")).intValue();
								
								System.out.println(interval);
								/*
								connOpts.setKeepAliveInterval(interval);
								client.connect(connOpts);*/
							}
							
							
						}
						
			        });
			
			while(true){
				if(test){
					System.out.println("re-connecting....");
					connOpts.setKeepAliveInterval(interval);
					client.disconnect();
					client.connect(connOpts);
					test = false;
				}
				
				try{
					Thread.sleep(1000);
				}
				
				catch(Exception e){
					e.printStackTrace();
				}
			}
			
			/*
			Scanner scan = new Scanner(System.in);
			int sel;
			
			while(true){
				printMenu();
				sel = scan.nextInt();
				
				switch(sel){
					case 1:{
						System.out.print("topic : ");
						String topic = scan.next();
						subscribe(topic);
						break;
					}
					
					case 2:{
						System.out.print("topic : ");
						String topic = scan.next();
						unsubscribe(topic);
						break;
					}
					
					case 3:{
						getSubscribingGroups();
						break;
					}
				}
				
				
			}*/
			
			// 구독정보 테스트
			
			//unsubscribe("testgroup");
			
			
			
		} 
		
		catch (MqttException e) {
			e.printStackTrace();
		} 

	}
	
	private static void printMenu(){
		System.out.println("**********************************");
		System.out.println("* your name : " + MONITOR_USERNAME);
		System.out.println("*                                *");
		System.out.println("*              MENU              *");
		System.out.println("*       1. subscribe topic       *");
		System.out.println("*       2. unsubscribe topic     *");
		System.out.println("*       3. check subscribe info  *");
		System.out.println("*                                *");
		System.out.println("**********************************");
	}
	
}
