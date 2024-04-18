package org.apache.kafka.solace.kafkaproxy;

/*
 * Copyright 2021 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessage.MessageUserPropertyConstants;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.common.util.DestinationType;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ProxyPubSubPlusSession {
    private static final Logger log = LoggerFactory.getLogger(ProxyPubSubPlusSession.class);
    private final Set<ProxyChannel> channels;
    private final JCSMPSession session;
    private final XMLMessageProducer publisher;
    private final String topicSeparatorReplace;
    private final ExecutorService publishService;

    private final DestinationType destinationType;
    private final boolean appendPartition;

    private long publishCount = 0;
    ScheduledExecutorService publishCountLogger;

    public ProxyPubSubPlusSession(ProxyConfig proxyConfig,
            ProxyChannel channel,
            byte[] username, byte[] password)
            throws UnsupportedEncodingException, InvalidPropertiesException, JCSMPException {
		channels = new HashSet<ProxyChannel>();
		addChannel(channel);
		final JCSMPProperties properties = new JCSMPProperties();

        properties.setProperty(JCSMPProperties.HOST, proxyConfig.getString(JCSMPProperties.HOST));
        properties.setProperty(JCSMPProperties.VPN_NAME, proxyConfig.getString(JCSMPProperties.VPN_NAME));
		properties.setProperty(JCSMPProperties.USERNAME, new String(username, "UTF-8"));
		properties.setProperty(JCSMPProperties.PASSWORD, new String(password, "UTF-8"));
        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255);

        topicSeparatorReplace = proxyConfig.getString(ProxyConfig.SEPARATOR_CONFIG);
        destinationType = proxyConfig.getString(ProxyConfig.DESTINATION_TYPE_CONFIG).equalsIgnoreCase("topic") ?
            DestinationType.topic : DestinationType.queue;
        appendPartition = proxyConfig.getBoolean(ProxyConfig.DESTINATION_INCLUDE_PARTITION_CONFIG);
        
		log.info("Creating new session to Solace event broker");
		session =  JCSMPFactory.onlyInstance().createSession(properties);
		
		publisher = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			public void responseReceivedEx(Object correlationKey) {
				final ProxyChannel.ProduceAckState produceAckState = (ProxyChannel.ProduceAckState) correlationKey;
				produceAckState.addToWorkQueue();
	        }

			public void handleErrorEx(Object correlationKey, JCSMPException e, long timestamp) {
				log.info("Got publish exception: " + e);
				final ProxyChannel.ProduceAckState produceAckState = (ProxyChannel.ProduceAckState) correlationKey;
				// in the rare case of not working, we construct a new produceAckState so that 
				// We can indicate that the publish did not work. This is done since produceAckState
				// is immutable and failures should be very rare
				new ProxyChannel.ProduceAckState(produceAckState, false).addToWorkQueue();
            }
	    });
        
        publishService = Executors.newSingleThreadExecutor();
        publishService.submit(() -> Thread.currentThread().setName("Publish_Service"));
    }

    public void addChannel(ProxyChannel channel) {
        synchronized (channels) {
            channels.add(channel);
            log.info("Added channel to session, new count: " + channels.size());
        }
    }

    public void removeChannel(ProxyChannel channel) {
        synchronized (channels) {
            channels.remove(channel);
            log.info("Removed channel from session, new count: " + channels.size());
            if (channels.isEmpty()) {
                ProxyPubSubPlusClient.getInstance().removeSession(this);
                log.info("No more channels for session, closing session");
                close();
            }
        }
    }
    
    // Used to fail all channels that use this API session when we need to fail
    // and we do not have a reference to the particular proxy channel that has
    // an issue. Should never happen.
    private void failAllChannels() {
        synchronized (channels) {
            for (ProxyChannel channel : channels) {
                new ProxyChannel.Close(channel, "Session to Solace broker going down").addToWorkQueue();
            }
        }
    }
    
    public void connect(ProxyChannel.AuthorizationResult authResult) {
        ProxyPubSubPlusClient.getExecutorService().execute(new Runnable() {
            public void run() {
                try {
                    session.connect();
                    authResult.addToWorkQueue();
                } catch (Exception e) {
                    log.warn("Session connection failed: " + e);
                    new ProxyChannel.AuthorizationResult(authResult, false).addToWorkQueue();
                }
            }
        });
        publishCountLogger = Executors.newScheduledThreadPool(1);
        publishCountLogger.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("Publish_Rate_Logger");
                if(publishCount > 0) {
                    log.debug("Published Message Rate: {} msg/s", publishCount);
                }
                publishCount = 0;
            }

        }, 1000, 1000, TimeUnit.MILLISECONDS); 
    }
    
    private Destination createSolaceDestination(String kafkaTopic, int partitionId) {
        String solaceDestinationName = kafkaTopic;

    	if (!topicSeparatorReplace.isEmpty()) {
            solaceDestinationName = solaceDestinationName.replaceAll(
                String.format("[{0}]", topicSeparatorReplace), "/");  // replace all _ or . with /
            solaceDestinationName = solaceDestinationName.replaceAll("//", "/_/");   // any empty levels replace with a _
            solaceDestinationName = solaceDestinationName.replaceFirst("^/", "_/");  // no leading empty level
            solaceDestinationName = solaceDestinationName.replaceFirst("/$", "/_");  // no trailing empty level
        }

        if (appendPartition) {
            solaceDestinationName = String.join(destinationType == DestinationType.topic ? "/" : "-",
                solaceDestinationName,
                String.valueOf(partitionId));
        }

		return destinationType == DestinationType.topic ?
            JCSMPFactory.onlyInstance().createTopic(solaceDestinationName) :
            JCSMPFactory.onlyInstance().createQueue(solaceDestinationName);
    }

    public void publish(String topic, int partitionId, byte[] payload, byte[] key, ProxyChannel.ProduceAckState produceAckState) {
        publishService.submit(() -> {
            if(publishService.isShutdown()) {
                return;
            }
            try {
                BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                msg.setDeliveryMode(DeliveryMode.PERSISTENT);
                msg.setCorrelationKey(produceAckState);
                msg.writeAttachment(payload);
                if (key != null) {
                    SDTMap solaceMsgProperties = JCSMPFactory.onlyInstance().createMap();
                    solaceMsgProperties.putString(MessageUserPropertyConstants.QUEUE_PARTITION_KEY, new String(key, "UTF-8"));
                    msg.setProperties(solaceMsgProperties);
                }
    
                final Destination destination = createSolaceDestination(topic, partitionId);
                publisher.send(msg, destination);
                publishCount++;
            } catch (UnsupportedEncodingException | JCSMPException e) {
                log.info("Publish did not work: " + e);
                new ProxyChannel.ProduceAckState(produceAckState, false).addToWorkQueue();
            }
        });
    }
    
    public void close() {
        publishService.shutdown();
        session.closeSession();
        publisher.close();
        publishCountLogger.shutdown();
    }
        
}