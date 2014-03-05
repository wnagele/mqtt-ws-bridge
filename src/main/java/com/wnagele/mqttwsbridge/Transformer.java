package com.wnagele.mqttwsbridge;

import java.nio.ByteBuffer;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public interface Transformer {
	public void transform(RemoteEndpoint remote, MqttMessage msg);

	class Default implements Transformer {
		@Override
		public void transform(RemoteEndpoint remote, MqttMessage msg) {
			try {
				byte[] payload = msg.getPayload();
				remote.sendBytes(ByteBuffer.allocate(payload.length).put(payload));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}