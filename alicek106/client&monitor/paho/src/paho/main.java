package paho;

import java.util.ArrayList;
import java.util.Arrays;
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

public class main {

	static ArrayList<String> publisherList = new ArrayList<String>();
	
	static HashMap<String, String> userList = new HashMap<String, String>();
	
	static HashMap<String, String> publisherClientIDList = new HashMap<String, String>();
	static ArrayList<String> userClientIDList = new ArrayList<String>();
	
	static MqttClient client = null;
	
	final static String MQTT_BROKER_IP = "tcp://163.180.117.247:1883";
	
	final static String MONITOR_USERNAME = "monitor";
	final static String MONITOR_PASSWORD = "coin200779";
	
	static private void initPublisherList(){
		// username alicek106 is publisher
		publisherList.add("alicek106");
	}
	
	static private void initUserList(){
		
		// user alicek107 and alicek108 is user
		userList.put("alicek106", "coin200779");
		userList.put("alicek107", "coin200779");
		userList.put("alicek108", "coin200779");
		
	}
	
	public static void main(String[] args) {

		initPublisherList();
		initUserList();
		
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
			System.out.println("[INFO] Monitor is connected to Broker successfully.");
			
			client.setCallback(new MqttCallback() {
			 
			            @Override
			            public void connectionLost(Throwable cause) { 
			            }

						@Override
						public void deliveryComplete(IMqttDeliveryToken arg0) {		
						}

						@Override
						public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
							System.out.println(arg0 + " : " + arg1);
							
							JSONObject job = parseJSON(arg1.toString());

							if(arg0.equals("NAMU/user/login")){
								processLogin(job);
							}
							
							else if(arg0.equals("NAMU/user/permission")){
								processPermission(job);
							}
							
							else if(arg0.equals("NAMU/group/list")){
								processGroupList(job);
							}
						}
						
			        });

			// ����� ���ȵ�
			client.subscribe("NAMU/user/login", 1);
			client.subscribe("NAMU/user/permission", 1);
			client.subscribe("NAMU/group/list", 1);

		} 
		
		catch (MqttException e) {
			e.printStackTrace();
		} 

	}
	
	static private void processLogin(JSONObject job){
		
		MqttMessage msg = new MqttMessage();
		msg.setQos(1);
		
		job.put("client_id", job.get("client_id"));
		
		String userName = (String)job.get("username");
		String password = (String)job.get("password");
		
		System.out.println("[INFO] user name : " + userName + ", password : " + password);
		// �α��� ������ ���, �̸� �ʱ�ȭ�س��� ��������Ʈ�� �� ���̵�� ��й�ȣ�� ��ġ�ϴ��� Ȯ��
		if(userList.containsKey(userName)){
			
			if(userList.get(userName).equals(password)){
				job.put("result", 0);
				msg.setPayload(job.toString().getBytes());
				
				// ����� �갡 �ۺ������� Ȯ��. �ۺ��Ÿ� �ۺ��� ����Ʈ�� Ŭ���̾�Ʈ ���̵� ����.
				if(publisherList.contains(userName)){
					System.out.println("[INFO] Publisher connected.");
					System.out.println("[INFO] Publisher name : " + userName);
					publisherClientIDList.put(userName, (String)job.get("client_id"));
					
				}
				
				else{
					System.out.println("[INFO] Client ID is saved.");
					userClientIDList.add((String)job.get("client_id"));
				}
				
				new Thread()
				{
				    public void run() {
				    	try {
				    		System.out.println("[INFO] login success message is being sending...");
							client.publish("NAMU/user/response", msg);
							System.out.println("[INFO] login success message was sent.");
						} catch (MqttException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				    }
				}.start();
				return;
			}
		}
		
		// �α��� ������ ���
		
			
		job.put("result", 2);
		msg.setPayload(job.toString().getBytes());
		
		new Thread()
		{
		    public void run() {
		    	try {
		    		System.out.println("[INFO] login failed message is being sending...");
					client.publish("NAMU/user/response", msg);
					System.out.println("[INFO] login failed message was sent.");
				} catch (MqttException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		}.start();
		
		
	}

	static private void processPermission(JSONObject job){
		MqttMessage msg = new MqttMessage();
		msg.setQos(1);
		
		// ������ �ۺ��� ����Ʈ�� �ۺ��ÿ�û���� Ŭ���̾�Ʈ ���̵� ����ִ��� Ȯ��.
		if(publisherClientIDList.containsValue((String)job.get("client_id"))){
			job.put("permission", 1);

			msg.setPayload(job.toString().getBytes());
			
			new Thread()
			{
			    public void run() {
			    	try {
			    		System.out.println("[INFO] permission success message is being sending... : " + msg.toString());
						client.publish("NAMU/user/response", msg);
						System.out.println("[INFO] permission success message was sent.");
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
			}.start();
		}
		
		// �ش� �갡 �ۺ��Ű� �ƴ� ��쿡 ���Ѵ�.
		else{
			job.put("permission", 2);

			msg.setPayload(job.toString().getBytes());
			
			new Thread()
			{
			    public void run() {
			    	try {
			    		System.out.println("[INFO] permission fail message is being sending... : " + msg.toString());
						client.publish("NAMU/user/response", msg);
						System.out.println("[INFO] permission fail message was sent.");
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
			}.start();
		}
	}

	static private void processGroupList(JSONObject job) {
		MqttMessage msg = new MqttMessage();
		msg.setQos(1);
		
		JSONArray jar = new JSONArray();

		for(int i = 0; i < userClientIDList.size(); i++){
			jar.add(userClientIDList.get(i));
		}
		
		job.put("member", jar);

		msg.setPayload(job.toString().getBytes());

		new Thread() {
			public void run() {
				try {
					System.out.println("[INFO] group list info message is being sending... : " + msg.toString());
					client.publish("NAMU/group/list/response", msg);
					System.out.println("[INFO] group list info message was sent.");
				} catch (MqttException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();

	}

	static private JSONObject parseJSON(String jsonString){
		JSONObject job = new JSONObject();
		
    	try {

    	    Object obj = JSONValue.parseWithException(jsonString.toString());

    	    job = (JSONObject)obj;

    	    return job;
    	} catch (Exception e) {

    		e.printStackTrace();
    		
    		return null;

    	}
	}
}
