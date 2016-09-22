package creativedesign.namu.monitor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import creativedesign.namu.assistance.AssistClass;
import creativedesign.namu.assistance.PrintConsole;

public class NamuNodeMonitor implements MqttCallback {

	private int qos;
	private String mqttBrokerUri;
	private String monitorID;
	private String monitorPW;

	private MqttClient namuNodeMonitor;
	private MqttConnectOptions connOpts;
	
	private MysqlAccessor mysqlAccessor;
	
	private JSONParser jsonParser;

	public NamuNodeMonitor(String _mqttBrokerUri, int _qos) {
		// TODO Auto-generated constructor stub
		System.out.println("NAMU init.");

		mqttBrokerUri = _mqttBrokerUri; // mqttbroker (tcp, ssl/tls socket)
		monitorID = "monitor";
		monitorPW = "coin200779";
		
		qos = _qos;
		
		mysqlAccessor = new MysqlAccessor();
		jsonParser = new JSONParser();
	}

	// initialize
	public void start() {
		MemoryPersistence persistence = new MemoryPersistence();

		// namu initialize
		try {
			
			namuNodeMonitor = new MqttClient(mqttBrokerUri, MqttClient.generateClientId(), persistence);
			connOpts = new MqttConnectOptions();
			connOpts.setUserName(monitorID);
			connOpts.setPassword(monitorPW.toCharArray());
			connOpts.setCleanSession(true);
			namuNodeMonitor.setCallback(this);
			
			// connect with mqtt broker
			namuNodeMonitor.connect(connOpts);
			
			PrintConsole.printWhole(PrintConsole.PRINT_CONNECT,
					new String[]{"Connecting to broker: "+mqttBrokerUri, "Connected"});
			

			// subscribe managing topics
			subscribeHandlingTopics();
			
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			PrintConsole.printError(e);
		}
		
		/*
		 * //publish message String topic = "NAMU"; String content =
		 * "Message from namu";
		 * 
		 * System.out.println("Publishing message: " + content); MqttMessage
		 * message = new MqttMessage(content.getBytes()); message.setQos(qos);
		 * namuNodeMonitor.publish(topic, message); System.out.println(
		 * "Message published");
		 */

		// disconnect with broker
		// disconnect();
	}

	// connect with broker
	public void connect() throws MqttException {
		/*
		 * System.out.println("Connecting to broker: " + mqttBrokerUri);
		 * namuNodeMonitor.connect(connOpts);
		 * System.out.println("Connected");
		 */
		
	}

	// disconnect with broker
	public void disconnect() throws MqttException {
		namuNodeMonitor.disconnect();
		PrintConsole.printWhole(PrintConsole.PRINT_DISCONNECT, new String[]{"Disconnected"});
		System.exit(0);
	}

	// subscribe handling managing topics
	private void subscribeHandlingTopics() throws MqttException {
		ArrayList<String> context = new ArrayList<String>();
		
		context.add("Subscribing NAMU handling topics");
		namuNodeMonitor.subscribe("NAMU/#", qos);
		context.add("	- NAMU/#");
		context.add("Complete Subscribing!");
		
		PrintConsole.printWhole(PrintConsole.PRINT_SUBSCRIBE, context.toArray(new String[context.size()]));
	}
	
	// logging to DB
	private void loggingToDB(String timestamp, String topic, String message) {
		mysqlAccessor.loggingToDB(timestamp, topic, message);
	}

	// NAMU/user - handling USER event
	private void handleUser(String topic, MqttMessage message) throws MqttPersistenceException, MqttException, ParseException {
		String[] topicarr = topic.split("/");
		
		JSONObject userInfo = (JSONObject) jsonParser.parse(message.toString());
		JSONObject jsonPublish = new JSONObject();
		
		String timeStamp = AssistClass.getTimeStamp();
		long messageID = (long)userInfo.get("message_id");
		
		//System.out.println("messageID : " + messageID);
		
		if(topicarr[2].equals("login")) {
			//login permission request to DB
			//get login permission through database
			//return client_id or null
			String findResultClientID = mysqlAccessor.checkLogin(userInfo.get("username").toString(),
																 userInfo.get("password").toString(),
																 userInfo.get("client_id").toString());
			//response json
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("client_id", userInfo.get("client_id").toString());
			
			if(findResultClientID != null) {
				jsonPublish.put("permission", 2);
				jsonPublish.put("result", 0);
				jsonPublish.put("error", 0);

				//logging connection message to DB
				loggingToDB(timeStamp, topic, userInfo.toString());
			} else {
				jsonPublish.put("permission", 0);
				jsonPublish.put("result", 2);
				jsonPublish.put("error", "Wrong username or password");
			}
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/user/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		else if(topicarr[2].equals("permission")) {
			String client_id = userInfo.get("client_id").toString();
			
			int findResultPermission = mysqlAccessor.getUserPermission(client_id);
			
			//response json
			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("client_id", userInfo.get("client_id").toString());
			
			if(findResultPermission != -1) {
				jsonPublish.put("permission", findResultPermission);
				jsonPublish.put("result", 0);
				jsonPublish.put("error", 0);
			} else {
				jsonPublish.put("permission", findResultPermission);
				jsonPublish.put("result", 2);
				jsonPublish.put("error", "no permission");
			}
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/user/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		else if (topicarr[2].equals("disconnect")){
			//logging connection message to DB
			loggingToDB(timeStamp, topic, userInfo.toString());
		}
	}
	
	// NAMU/group - handling GROUP event
	private void handleGroup(String topic, MqttMessage message) throws ParseException, MqttPersistenceException, MqttException, SQLException {
		String[] topicarr = topic.split("/");
		
		JSONObject jsonGroup = (JSONObject) jsonParser.parse(message.toString());
		JSONObject jsonPublish = new JSONObject();
		
		String timeStamp = AssistClass.getTimeStamp();
		long messageID = (long)jsonGroup.get("message_id");

		//group list request to DB
		if(topicarr[2].equals("userlist")) {
			String groupName = jsonGroup.get("group").toString();
			ResultSet resultSet = mysqlAccessor.getGroupUserList(groupName);

			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("group", groupName.toString());
			
			if(resultSet != null) {
				JSONArray memberArray = new JSONArray();
				
				while(resultSet.next()) { 
					memberArray.add(resultSet.getString("client_id"));
				}
				
				jsonPublish.put("member", memberArray);
			} else
				jsonPublish.put("member", null);
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/group/userlist/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		//subscribe request
		else if(topicarr[2].equals("subscribe")) {
			String groupName = jsonGroup.get("group").toString();
			String client_id = jsonGroup.get("client_id").toString();
			
			int addUserToGroupResult = mysqlAccessor.addUserToGroup(groupName, client_id);
			
			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("result", addUserToGroupResult);
			jsonPublish.put("error", "TODO");
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/group/subscribe/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		//unsubscribe request
		else if(topicarr[2].equals("unsubscribe")) {
			String groupName = jsonGroup.get("group").toString();
			String client_id = jsonGroup.get("client_id").toString();
			
			int deleteUserFromGroupResult = mysqlAccessor.deleteUserFromGroup(groupName, client_id);
			
			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("result", deleteUserFromGroupResult);
			jsonPublish.put("error", "TODO");
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/group/unsubscribe/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		//group register completed
		else if(topicarr[2].equals("register")) {
			String groupName = jsonGroup.get("group").toString();
			JSONArray jsonList = (JSONArray) jsonGroup.get("member");
			ArrayList<String> userList = new ArrayList<String>();
			
			for(int i=0; i<jsonList.size(); i++) 
				userList.add(jsonList.get(i).toString());
			
			int registerGroupResult = mysqlAccessor.registerGroup(groupName, userList);
			
			jsonPublish.put("timestamp", timeStamp);
			jsonPublish.put("message_id", messageID);
			jsonPublish.put("group", groupName);
			jsonPublish.put("result", registerGroupResult);
			jsonPublish.put("error", "TODO");
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/group/register/response", new MqttMessage(jsonPublish.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
		//group list
		else if(topicarr[2].equals("list")) {

			String userName = jsonGroup.get("username").toString();
			ArrayList<String> userList = new ArrayList<String>();
			
			JSONObject result = mysqlAccessor.getGroupList(userName);
			
			result.put("timestamp", timeStamp);
			result.put("message_id", messageID);
			//jsonPublish.put("group_list", groupName);
			if(result != null)
				result.put("result", 0);
			else {
				result.put("result", -1);
			}
			result.put("error", "TODO");
			
			new Thread() {
				public void run() {
					try {
						namuNodeMonitor.publish("NAMU/group/list/response", new MqttMessage(result.toString().getBytes()));
					} catch (MqttPersistenceException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
	}
	
	//send setting keep-alive message
	public void setKeepAlive(String clientid, int interval) {
		JSONObject jsonMsg = new JSONObject();
		jsonMsg.put("client_id", clientid);
		jsonMsg.put("interval", interval);
		
		new Thread() {
			public void run() {
				try {
					namuNodeMonitor.publish("NAMU/user/keepalive", new MqttMessage(jsonMsg.toString().getBytes()));
				} catch (MqttPersistenceException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MqttException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}
	
	
	/**
	 * 
	 * messageArrived This callback is invoked when a message is received on a
	 * subscribed topic.
	 * 
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		// TODO Auto-generated method stub
		PrintConsole.printWhole(PrintConsole.PRINT_MESSAGE,
				new String[]{"Topic:" + topic, "Message: " + new String(message.getPayload())});

		if(topic.contains("NAMU/group")) {
			handleGroup(topic, message);
		} //User info Request
		else if(topic.contains("NAMU/user")) {
			handleUser(topic, message);
		}
	}

	/**
	 * 
	 * connectionLost This callback is invoked upon losing the MQTT connection.
	 * 
	 */
	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		PrintConsole.printWhole(PrintConsole.PRINT_DISCONNECT, new String[]{"Connection Lost with MQTT broker!"});
		//System.out.println("Connection Lost with MQTT broker!");
	}

	/**
	 * 
	 * deliveryComplete This callback is invoked when a message published by
	 * this client is successfully received by the broker.
	 * 
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub

	}
}
