package creativedesign.namu.assistance;

import creativedesign.namu.monitor.NamuNodeMonitor;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String mqttBrokerUri		= "tcp://163.180.117.199:1883";
		String mqttBrokerUri		= "tcp://163.180.117.247:1883";
		String clientID				= "monitor";
		int qos						= 2;
		
		NamuNodeMonitor nnm = new NamuNodeMonitor(mqttBrokerUri, qos);
		nnm.start();
	}

}
