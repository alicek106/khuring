/*
 * Copyright (c) 2012-2015 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.moquette.spi.impl;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import io.moquette.server.ConnectionDescriptor;
import io.moquette.server.netty.AutoFlushHandler;
import io.moquette.server.netty.NettyUtils;
import io.moquette.spi.ClientSession;
import io.moquette.spi.IMatchingCondition;
import io.moquette.spi.IMessagesStore;
import io.moquette.spi.IMessagesStore.StoredMessage;
import io.moquette.spi.ISessionsStore;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import io.moquette.spi.impl.subscriptions.SubscriptionsStore;
import io.moquette.spi.impl.subscriptions.Subscription;

import static io.moquette.parser.netty.Utils.VERSION_3_1;
import static io.moquette.parser.netty.Utils.VERSION_3_1_1;
import io.moquette.interception.messages.InterceptAcknowledgedMessage;
import io.moquette.parser.proto.messages.AbstractMessage;
import io.moquette.parser.proto.messages.AbstractMessage.QOSType;
import io.moquette.parser.proto.messages.ConnAckMessage;
import io.moquette.parser.proto.messages.ConnectMessage;
import io.moquette.parser.proto.messages.PubAckMessage;
import io.moquette.parser.proto.messages.PubCompMessage;
import io.moquette.parser.proto.messages.PubRecMessage;
import io.moquette.parser.proto.messages.PubRelMessage;
import io.moquette.parser.proto.messages.PublishMessage;
import io.moquette.parser.proto.messages.SubAckMessage;
import io.moquette.parser.proto.messages.SubscribeMessage;
import io.moquette.parser.proto.messages.UnsubAckMessage;
import io.moquette.parser.proto.messages.UnsubscribeMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alicek106.JSONParser.JSONParserAssist;

/**
 * Class responsible to handle the logic of MQTT protocol it's the director of
 * the protocol execution.
 *
 * Used by the front facing class SimpleMessaging.
 *
 * @author andrea
 */
public class ProtocolProcessor {

	static final class WillMessage {
		private final String topic;
		private final ByteBuffer payload;
		private final boolean retained;
		private final QOSType qos;

		public WillMessage(String topic, ByteBuffer payload, boolean retained, QOSType qos) {
			this.topic = topic;
			this.payload = payload;
			this.retained = retained;
			this.qos = qos;
		}

		public String getTopic() {
			return topic;
		}

		public ByteBuffer getPayload() {
			return payload;
		}

		public boolean isRetained() {
			return retained;
		}

		public QOSType getQos() {
			return qos;
		}

	}

	private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor.class);

	private HashMap<String, ConnectionInfo> connectionQueue = new HashMap<String, ConnectionInfo>();
	private HashMap<Integer, String> publishQueue = new HashMap<Integer, String>();
	private HashMap<Integer, String> groupListQueue = new HashMap<Integer, String>();

	// message ID, keep Alive Interval
	private HashMap<Integer, Integer> keepAliveTestQueue = new HashMap<Integer, Integer>();

	class ConnectionInfo {
		ConnectMessage msg;
		Channel channel;

		ConnectionInfo(Channel channel, ConnectMessage msg) {
			this.channel = channel;
			this.msg = msg;
		}

		public ConnectMessage getMsg() {
			return msg;
		}

		public void setMsg(ConnectMessage msg) {
			this.msg = msg;
		}

		public Channel getChannel() {
			return channel;
		}

		public void setChannel(Channel channel) {
			this.channel = channel;
		}
	}

	protected ConcurrentMap<String, ConnectionDescriptor> m_clientIDs;
	private SubscriptionsStore subscriptions;
	private boolean allowAnonymous;
	private IAuthorizator m_authorizator;
	private IMessagesStore m_messagesStore;
	private ISessionsStore m_sessionsStore;
	private IAuthenticator m_authenticator;
	private BrokerInterceptor m_interceptor;
	private final int CONNECT_SUCCESS = 1;
	private final int CONNECT_FAIL = 2;
	private int message_id = 0;

	// maps clientID to Will testament, if specified on CONNECT
	private ConcurrentMap<String, WillMessage> m_willStore = new ConcurrentHashMap<>();

	ProtocolProcessor() {
	}

	/**
	 * @param subscriptions
	 *            the subscription store where are stored all the existing
	 *            clients subscriptions.
	 * @param storageService
	 *            the persistent store to use for save/load of messages for QoS1
	 *            and QoS2 handling.
	 * @param sessionsStore
	 *            the clients sessions store, used to persist subscriptions.
	 * @param authenticator
	 *            the authenticator used in connect messages.
	 * @param allowAnonymous
	 *            true connection to clients without credentials.
	 * @param authorizator
	 *            used to apply ACL policies to publishes and subscriptions.
	 * @param interceptor
	 *            to notify events to an intercept handler
	 */
	void init(SubscriptionsStore subscriptions, IMessagesStore storageService, ISessionsStore sessionsStore,
			IAuthenticator authenticator, boolean allowAnonymous, IAuthorizator authorizator,
			BrokerInterceptor interceptor) {
		this.m_clientIDs = new ConcurrentHashMap<>();
		this.m_interceptor = interceptor;
		this.subscriptions = subscriptions;
		this.allowAnonymous = allowAnonymous;
		m_authorizator = authorizator;
		LOG.trace("subscription tree on init {}", subscriptions.dumpTree());
		m_authenticator = authenticator;
		m_messagesStore = storageService;
		m_sessionsStore = sessionsStore;
	}

	public void internalPublish(String msg, String topic) {
		message_id++;
		PublishMessage message = new PublishMessage();
		message.setTopicName(topic);
		message.setRetainFlag(true);
		// message.setQos(AbstractMessage.QOSType.MOST_ONE);
		// message.setQos(AbstractMessage.QOSType.LEAST_ONE);
		message.setQos(AbstractMessage.QOSType.MOST_ONE);
		message.setPayload(ByteBuffer.wrap(msg.getBytes()));
		internalPublish(message);
	}

	public void processConnect(Channel channel, ConnectMessage msg) {
		LOG.debug("CONNECT for client <{}>", msg.getClientID());
		if (msg.getProtocolVersion() != VERSION_3_1 && msg.getProtocolVersion() != VERSION_3_1_1) {
			ConnAckMessage badProto = new ConnAckMessage();
			badProto.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
			LOG.warn("processConnect sent bad proto ConnAck");
			channel.writeAndFlush(badProto);
			channel.close();
			return;
		}

		if (msg.getClientID() == null || msg.getClientID().length() == 0) {
			ConnAckMessage okResp = new ConnAckMessage();
			okResp.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
			channel.writeAndFlush(okResp);
			m_interceptor.notifyClientConnected(msg);
			return;
		}

		// handle user authentication

		byte[] pwd = msg.getPassword();
		String userName = msg.getUsername();

		if (userName.equals("monitor")) {
			System.out.println("You are monitor!");
			connectAck(channel, msg, CONNECT_SUCCESS);
		}

		else {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("username", msg.getUsername());
			jsonObject.put("password", new String(pwd));
			jsonObject.put("client_id", msg.getClientID());
			jsonObject.put("message_id", message_id);

			// jsonObject.put("message_id", seqNum);

			System.out.println("Connecting waiting..");

			connectionQueue.put(msg.getClientID(), new ConnectionInfo(channel, msg));

			internalPublish(jsonObject.toString(), "NAMU/user/login");
		}

	}

	public void connectAck(Channel channel, ConnectMessage msg, int success) {

		if (success == CONNECT_SUCCESS) {
			NettyUtils.userName(channel, msg.getUsername());
			// if an old client with the same ID already exists close its
			// session.
			if (m_clientIDs.containsKey(msg.getClientID())) {
				LOG.info("Found an existing connection with same client ID <{}>, forcing to close", msg.getClientID());
				// clean the subscriptions if the old used a cleanSession = true
				Channel oldChannel = m_clientIDs.get(msg.getClientID()).channel;
				ClientSession oldClientSession = m_sessionsStore.sessionForClient(msg.getClientID());
				oldClientSession.disconnect();
				NettyUtils.sessionStolen(oldChannel, true);
				oldChannel.close();
				LOG.debug("Existing connection with same client ID <{}>, forced to close", msg.getClientID());
			}

			ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), channel, msg.isCleanSession());
			m_clientIDs.put(msg.getClientID(), connDescr);

			int keepAlive = msg.getKeepAlive();
			LOG.debug("Connect with keepAlive {} s", keepAlive);
			NettyUtils.keepAlive(channel, keepAlive);
			// session.attr(NettyUtils.ATTR_KEY_CLEANSESSION).set(msg.isCleanSession());
			NettyUtils.cleanSession(channel, msg.isCleanSession());
			// used to track the client in the subscription and publishing
			// phases.
			// session.attr(NettyUtils.ATTR_KEY_CLIENTID).set(msg.getClientID());
			NettyUtils.clientID(channel, msg.getClientID());
			LOG.debug("Connect create session <{}>", channel);

			setIdleTime(channel.pipeline(), Math.round(keepAlive * 1.5f));

			// Handle will flag
			if (msg.isWillFlag()) {
				AbstractMessage.QOSType willQos = AbstractMessage.QOSType.valueOf(msg.getWillQos());
				byte[] willPayload = msg.getWillMessage();
				ByteBuffer bb = (ByteBuffer) ByteBuffer.allocate(willPayload.length).put(willPayload).flip();
				// save the will testament in the clientID store
				WillMessage will = new WillMessage(msg.getWillTopic(), bb, msg.isWillRetain(), willQos);
				m_willStore.put(msg.getClientID(), will);
				LOG.info("Session for clientID <{}> with will to topic {}", msg.getClientID(), msg.getWillTopic());
			}

			ConnAckMessage okResp = new ConnAckMessage();
			okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);

			ClientSession clientSession = m_sessionsStore.sessionForClient(msg.getClientID());
			boolean isSessionAlreadyStored = clientSession != null;
			if (!msg.isCleanSession() && isSessionAlreadyStored) {
				okResp.setSessionPresent(true);
			}
			if (isSessionAlreadyStored) {
				clientSession.cleanSession(msg.isCleanSession());
			}
			channel.writeAndFlush(okResp);
			m_interceptor.notifyClientConnected(msg);

			if (!isSessionAlreadyStored) {
				LOG.info("Create persistent session for clientID <{}>", msg.getClientID());
				clientSession = m_sessionsStore.createNewSession(msg.getClientID(), msg.isCleanSession());
			}
			clientSession.activate();
			if (msg.isCleanSession()) {
				clientSession.cleanSession();
			}
			LOG.info("Connected client ID <{}> with clean session {}", msg.getClientID(), msg.isCleanSession());
			if (!msg.isCleanSession()) {
				// force the republish of stored QoS1 and QoS2
				republishStoredInSession(clientSession);
			}
			int flushIntervalMs = 500;
			setupAutoFlusher(channel.pipeline(), flushIntervalMs);
			LOG.info("CONNECT processed");
		}

		else if (success == CONNECT_FAIL) {
			failedCredentials(channel);
			channel.close();
		}
	}

	public void processPublish(Channel channel, PublishMessage msg) {

		final AbstractMessage.QOSType qos = msg.getQos();
		final Integer messageID = msg.getMessageID();
		final String clientID = NettyUtils.clientID(channel);
		final String topic = msg.getTopicName();
		LOG.trace("PUB --PUBLISH--> SRV executePublish invoked with {}", msg);

		String username = NettyUtils.userName(channel);

		if (msg.getTopicName().equals("NAMUCLIENT/user/keepalive")) {
			// python 웹으로부터 특정 그룹에 대한 interval 조정 요청이 들어옴.
			System.out.println("[INFO] message of keep-alive");
			try {
				JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array(), "utf-8"));

				String keepAliveTargetGroup = (String) result.get("group");
				int keepAliveInterval = ((Long) result.get("interval")).intValue();

				sendPubAck(clientID, messageID);

				m_interceptor.notifyTopicPublished(msg, clientID, username);

				// 테스트용 message Publish

				JSONObject messageJSONObject = new JSONObject();// JSONParserAssist.parseString(new
																// String(msg.getPayload().array()));

				messageJSONObject.put("message_id", message_id);
				messageJSONObject.put("group", keepAliveTargetGroup);

				keepAliveTestQueue.put(message_id, keepAliveInterval);

				internalPublish(messageJSONObject.toString(), "NAMU/group/userlist");
			} catch (Exception e) {

			}
			return;
		}

		if (msg.getTopicName().equals("NAMU/user/response")) {

			JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

			// publish 의 경우 퍼블리셔 인증을 위한 것. 만약 publishQueue의 해당 meesage_id 가
			// 들어있다면 퍼블리시 인증요청에 대한 응답일 것임.
			if (publishQueue.containsKey(((Long) result.get("message_id")).intValue())) {

				JSONObject contents = JSONParserAssist
						.parseString(publishQueue.get(((Long) result.get("message_id")).intValue()));
				// 큐에서 삭제.
				publishQueue.remove(((Long) result.get("message_id")).intValue());

				// 결과값이 1인 경우, 즉 퍼블리셔인 경우에는 정상적으로 퍼블리시를 하기 위해 모니터에 클라이언트 ID를
				// 요청해야 한다.
				if (((Long) result.get("permission")).intValue() == 1) {
					publishQueue.put(message_id, contents.toString());
					JSONObject group = new JSONObject();
					group.put("group", contents.get("group"));
					group.put("message_id", message_id);

					internalPublish(group.toString(), "NAMU/group/userlist");

				}

				else {
					System.out.println("[ERROR] you are not publisher!");
					// 인증 실패 시 아무것도 하지 않는다.
				}

				sendPubAck(clientID, messageID);
				m_interceptor.notifyTopicPublished(msg, clientID, username);

			}

			// client가 연결하려는 경우. 위의 if로 걸러지지 않은 경우.
			else {
				ConnectionInfo info = connectionQueue.get((String) result.get("client_id"));
				if (((Long) result.get("result")).intValue() == 0) {
					System.out.println("success connect");
					connectAck(info.getChannel(), info.getMsg(), CONNECT_SUCCESS);
				}

				else {
					System.out.println("fail connect");
					connectAck(info.getChannel(), info.getMsg(), CONNECT_FAIL);
				}

				sendPubAck(clientID, messageID);
				m_interceptor.notifyTopicPublished(msg, clientID, username);

				connectionQueue.remove((String) result.get("client_id"));
				return;
			}
		}

		// 지금 하고있는것
		else if (msg.getTopicName().equals("NAMUCLIENT/group/list")) {

			JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

			result.put("message_id", message_id);

			// publishQueue.put(message_id, new
			// String(msg.getPayload().array()));

			groupListQueue.put(message_id, clientID);

			// publish 에게 ACK 반환.
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

			internalPublish(result.toString(), "NAMU/group/list");

		}

		else if (msg.getTopicName().equals("NAMU/group/list/response")) {

			JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

			if ((groupListQueue.containsKey(((Long) result.get("message_id")).intValue()))) {
				System.out.println("[INFO] group list response arrived.");

				// 큐로부터 클라이언트 ID 가져온다.
				String client_id = groupListQueue.get(((Long) result.get("message_id")).intValue());

				// 리스트 작성. 클라이언트 ID 하나한테 보내는거라 사실 의미는 없음.
				ArrayList<String> list = new ArrayList<String>();
				list.add(client_id);

				msg.setTopicName("NAMUCLIENT/group/list/response");

				IMessagesStore.StoredMessage pubMsg = asStoredMessage(msg);

				groupListQueue.remove(((Long) result.get("message_id")).intValue());

				System.out.println("[INFO] sending broadcasting...");
				sendBroadCast(pubMsg, list);

				System.out.println("[INFO] sent braodcasting");
			}

			// publish 에게 ACK 반환.
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

		}

		else if ((msg.getTopicName().equals("NAMU/group/userlist/response"))) {

			// keep alive 임시용
			if (keepAliveTestQueue.containsKey(
					((Long) JSONParserAssist.parseString(new String(msg.getPayload().array())).get("message_id"))
							.intValue())) {
				System.out.println("[INFO] keep alive response arrived");

				sendPubAck(clientID, messageID);

				m_interceptor.notifyTopicPublished(msg, clientID, username);

				JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

				ArrayList<String> list = new ArrayList<String>();

				JSONArray arr = (JSONArray) result.get("member");

				for (int i = 0; i < arr.size(); i++) {
					list.add((String) arr.get(i));
				}

				JSONObject keepaliveJSONObject = new JSONObject();

				keepaliveJSONObject.put("interval",
						keepAliveTestQueue.get(((Long) result.get("message_id")).intValue()));

				msg.setPayload(ByteBuffer.wrap(keepaliveJSONObject.toString().getBytes()));

				msg.setTopicName("NAMUCLIENT/keepalive");

				IMessagesStore.StoredMessage pubMsg = asStoredMessage(msg);

				System.out.println("[INFO] sending broadcasting...(keep alive)");

				sendBroadCast(pubMsg, list);

				System.out.println("[INFO] sent braodcasting (keep alive)");
				return;
			}

			JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

			String contents = publishQueue.get(((Long) result.get("message_id")).intValue());

			ArrayList<String> list = new ArrayList<String>();

			JSONArray arr = (JSONArray) result.get("member");

			for (int i = 0; i < arr.size(); i++) {
				list.add((String) arr.get(i));
			}

			msg.setPayload(ByteBuffer.wrap(contents.getBytes()));
			msg.setTopicName("NAMUCLIENT/message");

			IMessagesStore.StoredMessage pubMsg = asStoredMessage(msg);

			System.out.println("[INFO] sending broadcasting...");
			sendBroadCast(pubMsg, list);

			System.out.println("[INFO] sent braodcasting");

		}

		else if ((msg.getTopicName().equals("NAMU/group/unsubscribe/response"))) {

			System.out.println("[INFO] unsubscribe response arrived..");
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

		}

		else if ((msg.getTopicName().equals("NAMU/group/subscribe/response"))) {

			System.out.println("[INFO] subscribe response arrived.");
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

		}

		else if ((msg.getTopicName().equals("NAMU/group/register/response"))) {

			System.out.println("[INFO] group register arrived..");
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

		}

		else if ((msg.getTopicName().equals("NAMUCLIENT/group/register"))) {

			JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array()));

			result.put("message_id", message_id);

			// publish 에게 ACK 반환.
			sendPubAck(clientID, messageID);
			m_interceptor.notifyTopicPublished(msg, clientID, username);

			// result 그대로 써도 상관 없다.
			internalPublish(result.toString(), "NAMU/group/register");

		}

		// 퍼블리셔로부터 메시지 전송 요청.
		else if ((msg.getTopicName().equals("NAMUCLIENT/group/topic/send"))) {

			JSONObject jsob = new JSONObject();

			jsob.put("client_id", clientID);
			jsob.put("message_id", message_id);
			try {
				String payload = new String(msg.getPayload().array(), "utf-8");
				payload = new String(msg.getPayload().array());
				publishQueue.put(message_id, payload);

				// publish 에게 ACK 반환.
				sendPubAck(clientID, messageID);
				m_interceptor.notifyTopicPublished(msg, clientID, username);

				// 권한 인증 요청
				internalPublish(jsob.toString(), "NAMU/user/permission");
			} catch (Exception e) {

			}

		}

		else if ((msg.getTopicName().equals("NAMUCLIENT/group/subscribe"))) {
			try {
				JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array(), "utf-8"));

				String groupName = (String) result.get("group");

				JSONObject jsob = new JSONObject();

				jsob.put("client_id", clientID);
				jsob.put("group", groupName);
				jsob.put("message_id", message_id);

				// publish 에게 ACK 반환.
				sendPubAck(clientID, messageID);
				m_interceptor.notifyTopicPublished(msg, clientID, username);

				internalPublish(jsob.toString(), "NAMU/group/subscribe");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/*
		 * else if((msg.getTopicName().equals("NAMUCLIENT/group/subscribe"))){
		 * JSONObject result = JSONParserAssist.parseString(new
		 * String(msg.getPayload().array()));
		 * 
		 * String groupName = (String)result.get("group");
		 * 
		 * JSONObject jsob = new JSONObject();
		 * 
		 * jsob.put("client_id", clientID); jsob.put("group", groupName);
		 * jsob.put("message_id", message_id);
		 * 
		 * // publish 에게 ACK 반환. sendPubAck(clientID, messageID);
		 * m_interceptor.notifyTopicPublished(msg, clientID, username);
		 * 
		 * internalPublish(jsob.toString() ,"NAMU/group/unsubscribe"); }
		 */

		else if ((msg.getTopicName().equals("NAMUCLIENT/group/unsubscribe"))) {
			try {
				JSONObject result = JSONParserAssist.parseString(new String(msg.getPayload().array(), "utf-8"));

				String groupName = (String) result.get("group");

				JSONObject jsob = new JSONObject();

				jsob.put("client_id", clientID);
				jsob.put("group", groupName);
				jsob.put("message_id", message_id);

				// publish 에게 ACK 반환.
				sendPubAck(clientID, messageID);
				m_interceptor.notifyTopicPublished(msg, clientID, username);

				internalPublish(jsob.toString(), "NAMU/group/unsubscribe");
			} catch (Exception e) {

			}
		}

		// normal publish (using topic)
		else {
			LOG.info("PUBLISH from clientID <{}> on topic <{}> with QoS {}", clientID, topic, qos);

			// String guid = null;

			IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
			toStoreMsg.setClientID(clientID);

			/*
			 * if (qos == AbstractMessage.QOSType.MOST_ONE) { //QoS0
			 * route2Subscribers(toStoreMsg); } else if (qos ==
			 * AbstractMessage.QOSType.LEAST_ONE) { //QoS1
			 */
			route2Subscribers(toStoreMsg);
			sendPubAck(clientID, messageID);
			LOG.debug("replying with PubAck to MSG ID {}", messageID);

			/*
			 * } else if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) { //QoS2
			 * guid = m_messagesStore.storePublishForFuture(toStoreMsg);
			 * sendPubRec(clientID, messageID); //Next the client will send us a
			 * pub rel //NB publish to subscribers for QoS 2 happen upon PUBREL
			 * from publisher }
			 */

			/*
			 * if (msg.isRetainFlag()) { if (qos ==
			 * AbstractMessage.QOSType.MOST_ONE) { //QoS == 0 && retain => clean
			 * old retained m_messagesStore.cleanRetained(topic); } else { if
			 * (!msg.getPayload().hasRemaining()) {
			 * m_messagesStore.cleanRetained(topic); } else { if (guid == null)
			 * { //before wasn't stored guid =
			 * m_messagesStore.storePublishForFuture(toStoreMsg); }
			 * m_messagesStore.storeRetained(topic, guid); } } }
			 */

			m_interceptor.notifyTopicPublished(msg, clientID, username);
		}
	}

	private void setupAutoFlusher(ChannelPipeline pipeline, int flushIntervalMs) {
		AutoFlushHandler autoFlushHandler = new AutoFlushHandler(flushIntervalMs, TimeUnit.MILLISECONDS);
		try {
			pipeline.addAfter("idleEventHandler", "autoFlusher", autoFlushHandler);
		} catch (NoSuchElementException nseex) {
			// the idleEventHandler is not present on the pipeline
			pipeline.addFirst("autoFlusher", autoFlushHandler);
		}
	}

	private void setIdleTime(ChannelPipeline pipeline, int idleTime) {
		if (pipeline.names().contains("idleStateHandler")) {
			pipeline.remove("idleStateHandler");
		}
		pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
	}

	private void failedCredentials(Channel session) {
		ConnAckMessage okResp = new ConnAckMessage();
		okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
		session.writeAndFlush(okResp);
	}

	/**
	 * Republish QoS1 and QoS2 messages stored into the session for the
	 * clientID.
	 */
	private void republishStoredInSession(ClientSession clientSession) {
		LOG.trace("republishStoredInSession for client <{}>", clientSession);
		List<IMessagesStore.StoredMessage> publishedEvents = clientSession.storedMessages();
		if (publishedEvents.isEmpty()) {
			LOG.info("No stored messages for client <{}>", clientSession.clientID);
			return;
		}

		LOG.info("republishing stored messages to client <{}>", clientSession.clientID);
		for (IMessagesStore.StoredMessage pubEvt : publishedEvents) {
			// TODO put in flight zone
			directSend(clientSession, pubEvt.getTopic(), pubEvt.getQos(), pubEvt.getMessage(), false,
					pubEvt.getMessageID());
			clientSession.removeEnqueued(pubEvt.getGuid());
		}
	}

	public void processPubAck(Channel channel, PubAckMessage msg) {
		try {
			String clientID = NettyUtils.clientID(channel);
			int messageID = msg.getMessageID();
			String username = NettyUtils.userName(channel);
			StoredMessage inflightMsg = m_sessionsStore.getInflightMessage(clientID, messageID);

			// Remove the message from message store
			ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
			verifyToActivate(clientID, targetSession);
			targetSession.inFlightAcknowledged(messageID);

			String topic = inflightMsg.getTopic();

			m_interceptor.notifyMessageAcknowledged(new InterceptAcknowledgedMessage(inflightMsg, topic, username));
		} catch (Exception e) {

		}
	}

	private void verifyToActivate(String clientID, ClientSession targetSession) {
		if (m_clientIDs.containsKey(clientID)) {
			targetSession.activate();
		}
	}

	private static IMessagesStore.StoredMessage asStoredMessage(PublishMessage msg) {
		IMessagesStore.StoredMessage stored = new IMessagesStore.StoredMessage(msg.getPayload().array(), msg.getQos(),
				msg.getTopicName());
		stored.setRetained(msg.isRetainFlag());
		stored.setMessageID(msg.getMessageID());
		return stored;
	}

	private static IMessagesStore.StoredMessage asStoredMessage(WillMessage will) {
		IMessagesStore.StoredMessage pub = new IMessagesStore.StoredMessage(will.getPayload().array(), will.getQos(),
				will.getTopic());
		pub.setRetained(will.isRetained());
		return pub;
	}

	/**
	 * Intended usage is only for embedded versions of the broker, where the
	 * hosting application want to use the broker to send a publish message.
	 * Inspired by {@link #processPublish} but with some changes to avoid
	 * security check, and the handshake phases for Qos1 and Qos2. It also
	 * doesn't notifyTopicPublished because using internally the owner should
	 * already know where it's publishing.
	 */
	public void internalPublish(PublishMessage msg) {
		final AbstractMessage.QOSType qos = msg.getQos();
		final String topic = msg.getTopicName();
		LOG.info("embedded PUBLISH on topic <{}> with QoS {}", topic, qos);

		String guid = null;
		IMessagesStore.StoredMessage toStoreMsg = asStoredMessage(msg);
		toStoreMsg.setClientID("BROKER_SELF");
		toStoreMsg.setMessageID(1);
		if (qos == AbstractMessage.QOSType.EXACTLY_ONCE) { // QoS2
			guid = m_messagesStore.storePublishForFuture(toStoreMsg);
		}
		route2Subscribers(toStoreMsg);

		if (!msg.isRetainFlag()) {
			return;
		}
		if (qos == AbstractMessage.QOSType.MOST_ONE || !msg.getPayload().hasRemaining()) {
			// QoS == 0 && retain => clean old retained
			m_messagesStore.cleanRetained(topic);
			return;
		}
		if (guid == null) {
			// before wasn't stored
			guid = m_messagesStore.storePublishForFuture(toStoreMsg);
		}
		m_messagesStore.storeRetained(topic, guid);
	}

	/**
	 * Specialized version to publish will testament message.
	 */
	private void forwardPublishWill(WillMessage will, String clientID) {
		// it has just to publish the message downstream to the subscribers
		// NB it's a will publish, it needs a PacketIdentifier for this conn,
		// default to 1
		Integer messageId = null;
		if (will.getQos() != AbstractMessage.QOSType.MOST_ONE) {
			messageId = m_sessionsStore.nextPacketID(clientID);
		}

		IMessagesStore.StoredMessage tobeStored = asStoredMessage(will);
		tobeStored.setClientID(clientID);
		tobeStored.setMessageID(messageId);
		route2Subscribers(tobeStored);
	}

	/**
	 * Flood the subscribers with the message to notify. MessageID is optional
	 * and should only used for QoS 1 and 2
	 */
	void route2Subscribers(IMessagesStore.StoredMessage pubMsg) {
		final String topic = pubMsg.getTopic();
		final AbstractMessage.QOSType publishingQos = pubMsg.getQos();
		final ByteBuffer origMessage = pubMsg.getMessage();
		LOG.debug("route2Subscribers republishing to existing subscribers that matches the topic {}", topic);
		if (LOG.isTraceEnabled()) {
			LOG.trace("content <{}>", DebugUtils.payload2Str(origMessage));
			LOG.trace("subscription tree {}", subscriptions.dumpTree());
		}
		// if QoS 1 or 2 store the message
		String guid = null;
		if (publishingQos == QOSType.EXACTLY_ONCE || publishingQos == QOSType.LEAST_ONE) {
			guid = m_messagesStore.storePublishForFuture(pubMsg);
		}

		for (final Subscription sub : subscriptions.matches(topic)) {
			AbstractMessage.QOSType qos = publishingQos;
			if (qos.byteValue() > sub.getRequestedQos().byteValue()) {
				qos = sub.getRequestedQos();
			}
			ClientSession targetSession = m_sessionsStore.sessionForClient(sub.getClientId());
			verifyToActivate(sub.getClientId(), targetSession);

			LOG.debug("Broker republishing to client <{}> topic <{}> qos <{}>, active {}", sub.getClientId(),
					sub.getTopicFilter(), qos, targetSession.isActive());
			ByteBuffer message = origMessage.duplicate();
			if (qos == AbstractMessage.QOSType.MOST_ONE && targetSession.isActive()) {
				// QoS 0
				directSend(targetSession, topic, qos, message, false, null);
			} else {
				// QoS 1 or 2
				// if the target subscription is not clean session and is not
				// connected => store it
				if (!targetSession.isCleanSession() && !targetSession.isActive()) {
					// store the message in targetSession queue to deliver
					targetSession.enqueueToDeliver(guid);
				} else {
					// publish
					if (targetSession.isActive()) {
						int messageId = targetSession.nextPacketId();
						targetSession.inFlightAckWaiting(guid, messageId);
						directSend(targetSession, topic, qos, message, false, messageId);
					}
				}
			}
		}
	}

	protected void directSend(ClientSession clientsession, String topic, AbstractMessage.QOSType qos,
			ByteBuffer message, boolean retained, Integer messageID) {
		String clientId = clientsession.clientID;
		LOG.debug("directSend invoked clientId <{}> on topic <{}> QoS {} retained {} messageID {}", clientId, topic,
				qos, retained, messageID);
		PublishMessage pubMessage = new PublishMessage();
		pubMessage.setRetainFlag(retained);
		pubMessage.setTopicName(topic);
		pubMessage.setQos(qos);
		pubMessage.setPayload(message);

		LOG.info("send publish message to <{}> on topic <{}>", clientId, topic);
		if (LOG.isDebugEnabled()) {
			LOG.debug("content <{}>", DebugUtils.payload2Str(message));
		}
		// set the PacketIdentifier only for QoS > 0
		if (pubMessage.getQos() != AbstractMessage.QOSType.MOST_ONE) {
			pubMessage.setMessageID(messageID);
		} else {
			if (messageID != null) {
				throw new RuntimeException("Internal bad error, trying to forwardPublish a QoS 0 message "
						+ "with PacketIdentifier: " + messageID);
			}
		}

		if (m_clientIDs == null) {
			throw new RuntimeException("Internal bad error, found m_clientIDs to null while it should be "
					+ "initialized, somewhere it's overwritten!!");
		}
		// LOG.trace("clientIDs are {}", m_clientIDs);
		if (m_clientIDs.get(clientId) == null) {
			// TODO while we were publishing to the target client, that client
			// disconnected,
			// could happen is not an error HANDLE IT
			throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache <%s>",
					clientId, m_clientIDs));
		}
		Channel channel = m_clientIDs.get(clientId).channel;
		LOG.trace("Session for clientId {}", clientId);
		if (channel.isWritable()) {
			// if channel is writable don't enqueue
			channel.write(pubMessage);
		} else {
			// enqueue to the client session
			clientsession.enqueue(pubMessage);
		}
	}

	private void sendPubRec(String clientID, int messageID) {
		LOG.trace("PUB <--PUBREC-- SRV sendPubRec invoked for clientID {} with messageID {}", clientID, messageID);
		PubRecMessage pubRecMessage = new PubRecMessage();
		pubRecMessage.setMessageID(messageID);
		m_clientIDs.get(clientID).channel.writeAndFlush(pubRecMessage);
	}

	private void sendPubAck(String clientId, int messageID) {
		LOG.trace("sendPubAck invoked");
		PubAckMessage pubAckMessage = new PubAckMessage();
		pubAckMessage.setMessageID(messageID);

		try {
			if (m_clientIDs == null) {
				throw new RuntimeException(
						"Internal bad error, found m_clientIDs to null while it should be initialized, somewhere it's overwritten!!");
			}
			LOG.debug("clientIDs are {}", m_clientIDs);
			if (m_clientIDs.get(clientId) == null) {
				throw new RuntimeException(String.format("Can't find a ConnectionDescriptor for client %s in cache %s",
						clientId, m_clientIDs));
			}
			m_clientIDs.get(clientId).channel.writeAndFlush(pubAckMessage);
		} catch (Throwable t) {
			LOG.error(null, t);
		}
	}

	/**
	 * Second phase of a publish QoS2 protocol, sent by publisher to the broker.
	 * Search the stored message and publish to all interested subscribers.
	 */
	public void processPubRel(Channel channel, PubRelMessage msg) {
		String clientID = NettyUtils.clientID(channel);
		int messageID = msg.getMessageID();
		LOG.debug("PUB --PUBREL--> SRV processPubRel invoked for clientID {} ad messageID {}", clientID, messageID);
		ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
		verifyToActivate(clientID, targetSession);
		IMessagesStore.StoredMessage evt = targetSession.storedMessage(messageID);
		route2Subscribers(evt);

		if (evt.isRetained()) {
			final String topic = evt.getTopic();
			if (!evt.getMessage().hasRemaining()) {
				m_messagesStore.cleanRetained(topic);
			} else {
				m_messagesStore.storeRetained(topic, evt.getGuid());
			}
		}

		sendPubComp(clientID, messageID);
	}

	private void sendPubComp(String clientID, int messageID) {
		LOG.debug("PUB <--PUBCOMP-- SRV sendPubComp invoked for clientID {} ad messageID {}", clientID, messageID);
		PubCompMessage pubCompMessage = new PubCompMessage();
		pubCompMessage.setMessageID(messageID);

		m_clientIDs.get(clientID).channel.writeAndFlush(pubCompMessage);
	}

	public void processPubRec(Channel channel, PubRecMessage msg) {
		String clientID = NettyUtils.clientID(channel);
		int messageID = msg.getMessageID();
		ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
		verifyToActivate(clientID, targetSession);
		// remove from the inflight and move to the QoS2 second phase queue
		targetSession.inFlightAcknowledged(messageID);
		targetSession.secondPhaseAckWaiting(messageID);
		// once received a PUBREC reply with a PUBREL(messageID)
		LOG.debug("\t\tSRV <--PUBREC-- SUB processPubRec invoked for clientID {} ad messageID {}", clientID, messageID);
		PubRelMessage pubRelMessage = new PubRelMessage();
		pubRelMessage.setMessageID(messageID);
		pubRelMessage.setQos(AbstractMessage.QOSType.LEAST_ONE);

		channel.writeAndFlush(pubRelMessage);
	}

	public void processPubComp(Channel channel, PubCompMessage msg) {
		String clientID = NettyUtils.clientID(channel);
		int messageID = msg.getMessageID();
		StoredMessage inflightMsg = m_sessionsStore.getInflightMessage(clientID, messageID);

		LOG.debug("\t\tSRV <--PUBCOMP-- SUB processPubComp invoked for clientID {} ad messageID {}", clientID,
				messageID);
		// once received the PUBCOMP then remove the message from the temp
		// memory
		ClientSession targetSession = m_sessionsStore.sessionForClient(clientID);
		verifyToActivate(clientID, targetSession);
		targetSession.secondPhaseAcknowledged(messageID);
		String username = NettyUtils.userName(channel);
		String topic = inflightMsg.getTopic();
		m_interceptor.notifyMessageAcknowledged(new InterceptAcknowledgedMessage(inflightMsg, topic, username));
	}

	public void processDisconnect(Channel channel) throws InterruptedException {
		channel.flush();
		String clientID = NettyUtils.clientID(channel);
		boolean cleanSession = NettyUtils.cleanSession(channel);
		LOG.info("DISCONNECT client <{}> with clean session {}", clientID, cleanSession);
		ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
		clientSession.disconnect();

		m_clientIDs.remove(clientID);
		channel.close();

		// cleanup the will store
		m_willStore.remove(clientID);

		String username = NettyUtils.userName(channel);
		m_interceptor.notifyClientDisconnected(clientID, username);
		LOG.info("DISCONNECT client <{}> finished", clientID, cleanSession);
	}

	public void processConnectionLost(String clientID, boolean sessionStolen, Channel channel) {
		ConnectionDescriptor oldConnDescr = new ConnectionDescriptor(clientID, channel, true);
		m_clientIDs.remove(clientID, oldConnDescr);
		// If already removed a disconnect message was already processed for
		// this clientID
		if (sessionStolen) {
			// de-activate the subscriptions for this ClientID
			ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
			clientSession.deactivate();
			LOG.info("Lost connection with client <{}>", clientID);
		}
		// publish the Will message (if any) for the clientID
		if (!sessionStolen && m_willStore.containsKey(clientID)) {
			WillMessage will = m_willStore.get(clientID);
			forwardPublishWill(will, clientID);
			m_willStore.remove(clientID);
		}
	}

	/**
	 * Remove the clientID from topic subscription, if not previously
	 * subscribed, doesn't reply any error
	 */
	public void processUnsubscribe(Channel channel, UnsubscribeMessage msg) {
		List<String> topics = msg.topicFilters();
		int messageID = msg.getMessageID();
		String clientID = NettyUtils.clientID(channel);

		LOG.debug("UNSUBSCRIBE subscription on topics {} for clientID <{}>", topics, clientID);

		ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
		verifyToActivate(clientID, clientSession);
		for (String topic : topics) {
			boolean validTopic = SubscriptionsStore.validate(topic);
			if (!validTopic) {
				// close the connection, not valid topicFilter is a protocol
				// violation
				channel.close();
				LOG.warn("UNSUBSCRIBE found an invalid topic filter <{}> for clientID <{}>", topic, clientID);
				return;
			}

			subscriptions.removeSubscription(topic, clientID);
			clientSession.unsubscribeFrom(topic);
			String username = NettyUtils.userName(channel);
			m_interceptor.notifyTopicUnsubscribed(topic, clientID, username);
		}

		// ack the client
		UnsubAckMessage ackMessage = new UnsubAckMessage();
		ackMessage.setMessageID(messageID);

		LOG.info("replying with UnsubAck to MSG ID {}", messageID);
		channel.writeAndFlush(ackMessage);
	}

	public void processSubscribe(Channel channel, SubscribeMessage msg) {
		String clientID = NettyUtils.clientID(channel);
		LOG.debug("SUBSCRIBE client <{}> packetID {}", clientID, msg.getMessageID());

		ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
		verifyToActivate(clientID, clientSession);
		// ack the client
		SubAckMessage ackMessage = new SubAckMessage();
		ackMessage.setMessageID(msg.getMessageID());

		String username = NettyUtils.userName(channel);
		List<Subscription> newSubscriptions = new ArrayList<>();
		for (SubscribeMessage.Couple req : msg.subscriptions()) {
			if (!m_authorizator.canRead(req.topicFilter, username, clientSession.clientID)) {
				// send SUBACK with 0x80, the user hasn't credentials to read
				// the topic
				LOG.debug("topic {} doesn't have read credentials", req.topicFilter);
				ackMessage.addType(AbstractMessage.QOSType.FAILURE);
				continue;
			}

			AbstractMessage.QOSType qos = AbstractMessage.QOSType.valueOf(req.qos);
			Subscription newSubscription = new Subscription(clientID, req.topicFilter, qos);
			boolean valid = clientSession.subscribe(req.topicFilter, newSubscription);
			ackMessage.addType(valid ? qos : AbstractMessage.QOSType.FAILURE);
			if (valid) {
				newSubscriptions.add(newSubscription);
			}
		}

		// save session, persist subscriptions from session
		LOG.debug("SUBACK for packetID {}", msg.getMessageID());
		if (LOG.isTraceEnabled()) {
			LOG.trace("subscription tree {}", subscriptions.dumpTree());
		}
		channel.writeAndFlush(ackMessage);

		// fire the publish
		for (Subscription subscription : newSubscriptions) {
			subscribeSingleTopic(subscription, username);
		}
	}

	private boolean subscribeSingleTopic(final Subscription newSubscription, String username) {
		subscriptions.add(newSubscription.asClientTopicCouple());

		// scans retained messages to be published to the new subscription
		// TODO this is ugly, it does a linear scan on potential big dataset
		Collection<IMessagesStore.StoredMessage> messages = m_messagesStore.searchMatching(new IMatchingCondition() {
			@Override
			public boolean match(String key) {
				return SubscriptionsStore.matchTopics(key, newSubscription.getTopicFilter());
			}
		});

		ClientSession targetSession = m_sessionsStore.sessionForClient(newSubscription.getClientId());
		verifyToActivate(newSubscription.getClientId(), targetSession);
		for (IMessagesStore.StoredMessage storedMsg : messages) {
			// fire the as retained the message
			LOG.debug("send publish message for topic {}", newSubscription.getTopicFilter());
			// forwardPublishQoS0(newSubscription.getClientId(),
			// storedMsg.getTopic(), storedMsg.getQos(), storedMsg.getPayload(),
			// true);
			Integer packetID = storedMsg.getQos() == QOSType.MOST_ONE ? null : targetSession.nextPacketId();
			directSend(targetSession, storedMsg.getTopic(), storedMsg.getQos(), storedMsg.getPayload(), true, packetID);
		}

		// notify the Observables
		m_interceptor.notifyTopicSubscribed(newSubscription, username);
		return true;
	}

	public void notifyChannelWritable(Channel channel) {
		String clientID = NettyUtils.clientID(channel);
		ClientSession clientSession = m_sessionsStore.sessionForClient(clientID);
		boolean emptyQueue = false;
		while (channel.isWritable() && !emptyQueue) {
			AbstractMessage msg = clientSession.dequeue();
			if (msg == null) {
				emptyQueue = true;
			} else {
				channel.write(msg);
			}
		}
		channel.flush();
	}

	void sendBroadCast(IMessagesStore.StoredMessage pubMsg, ArrayList<String> list) {

		final String topic = pubMsg.getTopic();
		final AbstractMessage.QOSType publishingQos = pubMsg.getQos();
		final ByteBuffer origMessage = pubMsg.getMessage();

		if (LOG.isTraceEnabled()) {
			LOG.trace("content <{}>", DebugUtils.payload2Str(origMessage));
			LOG.trace("subscription tree {}", subscriptions.dumpTree());
		}
		// if QoS 1 or 2 store the message

		String guid = null;

		/*
		 * if (publishingQos == QOSType.EXACTLY_ONCE || publishingQos ==
		 * QOSType.LEAST_ONE) { guid =
		 * m_messagesStore.storePublishForFuture(pubMsg); }
		 */

		try {
			for (int i = 0; i < list.size(); i++) {
				AbstractMessage.QOSType qos = publishingQos;

				ClientSession targetSession = m_sessionsStore.sessionForClient(list.get(i));
				verifyToActivate(list.get(i), targetSession);

				ByteBuffer message = origMessage.duplicate();
				if (qos == AbstractMessage.QOSType.MOST_ONE && targetSession.isActive()) {
					// QoS 0
					directSend(targetSession, topic, qos, message, false, null);
				} else {
					// QoS 1 or 2
					// if the target subscription is not clean session and is
					// not connected => store it
					if (!targetSession.isCleanSession() && !targetSession.isActive()) {
						// store the message in targetSession queue to deliver
						targetSession.enqueueToDeliver(guid);
					} else {
						// publish
						if (targetSession.isActive()) {
							int messageId = targetSession.nextPacketId();
							targetSession.inFlightAckWaiting(guid, messageId);
							directSend(targetSession, topic, qos, message, false, messageId);
						}
					}
				}

			}
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
