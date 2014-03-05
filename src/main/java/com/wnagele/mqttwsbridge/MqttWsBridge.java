package com.wnagele.mqttwsbridge;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.server.WebSocketServerFactory;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.io.ByteStreams;

@WebSocket
public class MqttWsBridge {
	@Option(name = "--mqttServer", usage = "MQTT server", required = true, metaVar = "tcp://localhost:1883")
    public String[] mqttServers;
	@Option(name = "--mqttSubTopic", usage = "MQTT subscribe topic", metaVar = "some/topic")
	public String[] mqttSubTopics;
	@Option(name = "--mqttPubTopic", usage = "MQTT publish topic", metaVar = "some/topic")
	public String[] mqttPubTopics;
	@Option(name = "--mqttQos", usage = "MQTT QoS level", metaVar = "0, 1, 2")
	public int mqttQos = 0;
    @Option(name = "--websocketPort", usage = "Websocket server port", metaVar = "8080")
    public int websocketPort = 8080;
    @Option(name = "--transformerClass", usage = "Class implementing a transformer", metaVar = "com.wnagele.MyOwnTransformer")
    public String transformerClass = null;

	private Map<String, MqttClient> clients = new HashMap<String, MqttClient>();
	protected Transformer transformer = new Transformer.Default();

	public static void main(String[] args) throws Exception {
		try {
			MqttWsBridge bridge = new MqttWsBridge();
			CmdLineParser parser = new CmdLineParser(bridge);
			try {
				parser.parseArgument(args);
				bridge.run();
			} catch (CmdLineException e) {
				parser.printUsage(System.err);
			}
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private MqttWsBridge copy() {
		MqttWsBridge copy = new MqttWsBridge();
		copy.mqttServers = mqttServers;
		copy.mqttSubTopics = mqttSubTopics;
		copy.mqttPubTopics = mqttPubTopics;
		copy.websocketPort = websocketPort;
		copy.transformerClass = transformerClass;
		copy.transformer = transformer;
		return copy;
	}

	public void run() throws Exception {
		if (transformerClass != null)
			transformer = Transformer.class.cast(Class.forName(transformerClass).newInstance());

		Server server = new Server(websocketPort);
		server.setHandler(new WebSocketHandler() {
			@Override
			public void configure(WebSocketServletFactory factory) {
				factory.setCreator(new WebSocketServerFactory() {
					@Override
					public Object createWebSocket(UpgradeRequest req, UpgradeResponse resp) {
						return copy();
					}
				});
			}	
		});
		server.start();
		server.join();
	}

	@OnWebSocketClose
	public void onClose(int statusCode, String reason) {
		for (Map.Entry<String, MqttClient> entry : clients.entrySet()) {
			try {
				System.out.println("Disconnecting " + entry.getKey());
				MqttClient client = entry.getValue();
				if (client.isConnected())
					client.disconnect();
				client.close();
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
	}

	@OnWebSocketMessage
	public void onTextMessage(String message) throws MqttException {
		sendMessage(message.getBytes());
	}

	@OnWebSocketMessage
	public void onBinaryMessage(InputStream in) throws IOException, MqttException {
		sendMessage(ByteStreams.toByteArray(in));
	}

	private void sendMessage(byte[] payload) throws MqttException {
		for (Map.Entry<String, MqttClient> entry : clients.entrySet()) {
			String server = entry.getKey();
			MqttClient client = entry.getValue();
			for (String topic : mqttPubTopics) {
				System.out.println("Publish to " + topic + " for " + server);
				MqttMessage mqttMessage = new MqttMessage(payload);
				mqttMessage.setQos(mqttQos);
				client.publish(topic, mqttMessage);
			}
		}
	}

	@OnWebSocketConnect
	public void onConnect(final Session session) throws IOException, MqttException {
		MqttCallback callback = new MqttCallback() {
			@Override
			public void connectionLost(Throwable t) {
				t.printStackTrace();
				try {
					session.disconnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {}

			@Override
			public void messageArrived(String topic, final MqttMessage msg) throws Exception {
				System.out.println("Sending message");
				transformer.transform(session.getRemote(), msg);
			}
		};

		MemoryPersistence persistence = new MemoryPersistence();
		MqttConnectOptions connOpt = new MqttConnectOptions();
		String clientId = MqttClient.generateClientId();
		for (String server : mqttServers) {
			MqttClient client = new MqttClient(server, clientId, persistence);
			clients.put(server, client);
			client.setCallback(callback);
			System.out.println("Connecting " + server);
			client.connect(connOpt);
			for (String topic : mqttSubTopics) {
				System.out.println("Subscribe to " + topic + " for " + server);
				client.subscribe(topic, mqttQos);
			}
		}
	}
}