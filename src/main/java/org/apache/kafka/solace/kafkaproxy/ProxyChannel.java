package org.apache.kafka.solace.kafkaproxy;

/*
 * Copyright 2021 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyChannel {
	private static final Logger log = LoggerFactory.getLogger(ProxyChannel.class);
	private final Queue<Send> sendQueue;
	private final TransportLayer transportLayer;
	private final ProxyReactor.ListenPort listenPort;
    private final int partitionsPerTopic;
	private ProxyPubSubPlusSession session;
	private final ByteBuffer size; // holds the size field (first 4 bytes) of a received message
	private ByteBuffer buffer; // byte buffer used to hold all of message except for first 4 bytes
	private int requestedBufferSize = -1;
	private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	private static final List<String> saslMechanisms = List.of("PLAIN");
	private ProxySasl proxySasl = new ProxySasl();
	private boolean enableKafkaSaslAuthenticateHeaders;
	private ProduceResponseProcessing produceResponseProcesssing;
    private int inFlightRequestCount = 0;   // count of requests being processed asynchronously (e.g. authentication or produce requests)
    private RequestAndSize savedRequestAndSize = null;   // saved information for request that could not be processed immediately
    private RequestHeader savedRequestHeader = null;     // saved information for request that could not be processed immediately

	final static class ProduceAckState extends ProxyReactor.WorkEntry {
        private final ProduceResponse produceResponse;
		private final RequestHeader requestHeader;
		private final boolean lastInRequest;
		private final boolean success;
        private final PartitionProduceResponse partitionProduceResponse;

        public ProduceAckState(ProxyChannel proxyChannel,
                ProduceResponse produceResponse,
                ProduceResponseData.PartitionProduceResponse partitionProduceResponse,
				RequestHeader requestHeader, boolean lastInRequest) {
			super(proxyChannel);
            this.produceResponse = produceResponse;
            this.partitionProduceResponse = partitionProduceResponse;
			this.requestHeader = requestHeader;
			this.lastInRequest = lastInRequest;
			this.success = true;
		}

		// creates new ProduceAckState from existing one but sets new value for 'worked'
		// Everything is final and it is very unusual for 'worked' to not be true, so in
		// failure cases we construct a new entry from an existing entry
		public ProduceAckState(ProduceAckState srcState, boolean success) {
			super(srcState.getProxyChannel());
            this.produceResponse = srcState.getProduceResponse();
            this.partitionProduceResponse = srcState.getPartitionProduceResponse();
			this.requestHeader = srcState.getRequestHeader();
			this.lastInRequest = srcState.isLastInRequest();
			this.success = success;
		}

        public ProduceResponse getProduceResponse() {
            return produceResponse;
        }

        public PartitionProduceResponse getPartitionProduceResponse() {
            return partitionProduceResponse;
        }

		public RequestHeader getRequestHeader() {
			return requestHeader;
		}

		public boolean isLastInRequest() {
			return lastInRequest;
		}

		public boolean isSuccess() {
			return success;
		}

		@Override
		public String toString() {
			final String hdrString = (requestHeader == null) ? "null" : requestHeader.toString();
			return "ProduceAckState{" + "produceResponse=" + produceResponse + ", partitionProduceResponse=" + partitionProduceResponse
					+ ", requestHeader=" + hdrString + ", lastInRequest="
					+ lastInRequest + ", worked=" + success + "}";
		}
	}

	final static class AuthorizationResult extends ProxyReactor.WorkEntry {
		private final RequestHeader requestHeader;
		private final boolean worked;

		public AuthorizationResult(ProxyChannel proxyChannel, RequestHeader requestHeader) {
			super(proxyChannel);
			this.requestHeader = requestHeader;
			this.worked = true;
		}

		// creates new AuthorizationResult from existing one but sets new value for
		// 'worked'
		// Everything is final and it is very unusual for 'worked' to not be true, so in
		// failure cases we construct a new entry from an existing entry
		public AuthorizationResult(AuthorizationResult srcResult, boolean worked) {
			super(srcResult.getProxyChannel());
			this.requestHeader = srcResult.getRequestHeader();
			this.worked = worked;
		}

		public RequestHeader getRequestHeader() {
			return requestHeader;
		}

		public boolean getWorked() {
			return worked;
		}

		@Override
		public String toString() {
			final String hdrString = (requestHeader == null) ? "null" : requestHeader.toString();
			return "AuthorizationResult{" + ", requestHeader=" + hdrString + ", worked=" + worked + "}";
		}
	}

	final static class Close extends ProxyReactor.WorkEntry {
		private final String reason;

		public Close(ProxyChannel proxyChannel, String reason) {
			super(proxyChannel);
			this.reason = reason;
		}

		public String getReason() {
			return reason;
		}

		@Override
		public String toString() {
			return "Close{" + "reason=" + reason + "}";
		}
	}

	private class ProduceResponseProcessing {
		ProduceResponseProcessing() {

		}

		void handleProduceAckState(ProduceAckState produceAckState) {
            RequestHeader requestHeader = produceAckState.getRequestHeader();
            
            if (!produceAckState.isSuccess()) {
                produceAckState.getPartitionProduceResponse()
                    .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code());
            }

			if (produceAckState.isLastInRequest()) {
                ProduceResponse produceResponse = produceAckState.getProduceResponse();
                try {
                    responseToSend(produceResponse, ApiKeys.PRODUCE, 
                        requestHeader.toResponseHeader(), requestHeader.apiVersion());
				} catch (IOException e) {
					close("Could not send PRODUCE response: " + e);
				}
			}
		}
	}

	ProxyChannel(SocketChannel socketChannel, TransportLayer transportLayer, ProxyReactor.ListenPort listenPort, int partitionsPerTopic)
			throws IOException {
		this.transportLayer = transportLayer;
		this.listenPort = listenPort;
        this.partitionsPerTopic = partitionsPerTopic;
		size = ByteBuffer.allocate(4);
		enableKafkaSaslAuthenticateHeaders = false;
		produceResponseProcesssing = new ProxyChannel.ProduceResponseProcessing();
		listenPort.addChannel(this);
		sendQueue = new LinkedList<Send>();
	}

	String getHostName() {
		if (transportLayer != null) {
			return transportLayer.socketChannel().socket().getInetAddress().getHostName();
		} else {
			return "";
		}
	}

	ProxyReactor.ListenPort getListenPort() {
		return listenPort;
	}

    private void responseToSend(AbstractResponse response, ApiKeys apiKey, ResponseHeader header, short version)
            throws IOException {
        log.trace("Response details {}", response);
        Send send = response.toSend(header, version);
        dataToSend(send, apiKey);
    }

	// normally we will not end up with buffered data so we avoid
	// adding the new send to the sendQueue, only doing so if necessary
	private void dataToSend(Send send, ApiKeys apiKey) throws IOException {
        
        if (apiKey != ApiKeys.PRODUCE) {
            log.debug("Sending {} response to {}",
                apiKey, transportLayer.socketChannel().socket().getRemoteSocketAddress());
        } else {
            log.trace("Sending {} response to {}",
                apiKey, transportLayer.socketChannel().socket().getRemoteSocketAddress());            
        }
		if (sendQueue.isEmpty()) {
			send.writeTo(transportLayer);
			if (!send.completed()) {
				sendQueue.add(send);
				transportLayer.addInterestOps(SelectionKey.OP_WRITE);
			}
		} else {
			sendQueue.add(send);
			// send queue was not empty before so we must already have write interest
		}
	}

	public void authorizationResult(RequestHeader requestHeader, boolean worked) {
		try {
			SaslAuthenticateResponse saslAuthenticateResponse;
			// For versions with SASL_AUTHENTICATE header, send a response to
			// SASL_AUTHENTICATE request even if token is empty.
			if (worked) {
				proxySasl.setComplete(true);
			}
			if (enableKafkaSaslAuthenticateHeaders) {
				if (worked) {
					saslAuthenticateResponse = new SaslAuthenticateResponse(new SaslAuthenticateResponseData()
							.setErrorCode(Errors.NONE.code()).setAuthBytes(new byte[0]).setSessionLifetimeMs(0L));
				} else {
					saslAuthenticateResponse = new SaslAuthenticateResponse(
							new SaslAuthenticateResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
									.setAuthBytes(new byte[0]).setSessionLifetimeMs(0L));
				}
                responseToSend(saslAuthenticateResponse, ApiKeys.SASL_AUTHENTICATE,
                    requestHeader.toResponseHeader(), requestHeader.apiVersion());
			} else {
				if (worked) {
					Send netOutBuffer = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(new byte[0]));
					dataToSend(netOutBuffer, ApiKeys.SASL_AUTHENTICATE);
				} else {
					// TBD - how to report an error with no kafka heaader for SASL?
					Send netOutBuffer = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(new byte[0]));
					dataToSend(netOutBuffer, ApiKeys.SASL_AUTHENTICATE);
				}
			}
			if (!worked) {
				close("due to authentication failure");
			}
		} catch (Exception e) {
			log.info("Could not send authorization result: " + e);
			close("due to could not send authentication result");
		}
	}

	// returns true if caller should keep reading & parsing, false to stop
	private boolean parseRequest(ByteBuffer buffer) throws IOException, SaslAuthenticationException {
		RequestHeader header;
		ApiKeys apiKey;
		if (enableKafkaSaslAuthenticateHeaders || !proxySasl.authenticating()) {
			header = RequestHeader.parse(buffer);
			apiKey = header.apiKey();
            short version = header.apiVersion();
            if (apiKey != ApiKeys.PRODUCE) {
                log.debug("Received {} (v{}) request from {}",
                    apiKey, version, transportLayer.socketChannel().socket().getRemoteSocketAddress());
            } else {
                log.trace("Received {} (v{}) request from {}",
                    apiKey, version, transportLayer.socketChannel().socket().getRemoteSocketAddress());
            }
            
			proxySasl.adjustState(apiKey);
		} else {
            log.debug("Received SASL authentication request without Kafka header (remote " + 
                      transportLayer.socketChannel().socket().getRemoteSocketAddress()
                      + ")");
			byte[] clientToken = new byte[buffer.remaining()];
			buffer.get(clientToken, 0, clientToken.length);
			ProxyChannel.AuthorizationResult authResult = new ProxyChannel.AuthorizationResult(this, null);
            inFlightRequestCount++;
			try {
				session = proxySasl.authenticate(authResult, clientToken);
			} catch (Exception e) {
				log.info("Sasl authentication failed: " + e);
				authorizationResult(null, false);
			}
			return false;
		}
        
		short apiVersion = header.apiVersion();
		RequestAndSize requestAndSize;
		if (apiKey == API_VERSIONS && !API_VERSIONS.isVersionSupported(apiVersion)) {
			ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest(new ApiVersionsRequestData(), (short) 0,
					Short.valueOf(header.apiVersion()));
			requestAndSize = new RequestAndSize(apiVersionsRequest, 0);
			return handleRequest(requestAndSize, header);
		} else {
			try {
				requestAndSize = AbstractRequest.parseRequest(apiKey, apiVersion, buffer);
			} catch (Throwable ex) {
				throw new InvalidRequestException(
						"Error getting request for apiKey: " + apiKey + ", apiVersion: " + header.apiVersion(), ex);
			}
			return handleRequest(requestAndSize, header);
		}
	}
    
    // Delays a request that cannot be immediately handled due to other in-flight requests that are asynchronous
    // in nature (e.g. PRODUCE request). This dealys a request that we normally handle synchronously.
	private boolean delayRequest(RequestAndSize requestAndSize, RequestHeader requestHeader)
            throws InvalidRequestException {
        if ((savedRequestAndSize == null) && (savedRequestHeader == null)) {
            savedRequestAndSize = requestAndSize;
            savedRequestHeader = requestHeader;
            // We stop reading from the channel until this saved request can be processed (when no more requests in flight)
            transportLayer.removeInterestOps(SelectionKey.OP_READ);
        } else {
            throw new InvalidRequestException("Attempt to delay request when another request already delayed");
        }
        
        return false; // stop reading messages
    }

	// returns true if caller should keep reading & parsing, false to stop
	private boolean handleRequest(RequestAndSize requestAndSize, RequestHeader requestHeader)
			throws IOException, InvalidRequestException, SaslAuthenticationException {

		short version = requestHeader.apiVersion();
		ApiKeys apiKey = requestAndSize.request.apiKey();

        log.trace("Request details: {}", requestAndSize.request);

		switch (apiKey) {
            case API_VERSIONS: {
                if (inFlightRequestCount > 0) return delayRequest(requestAndSize, requestHeader);
                ApiVersionsResponse defaultApiVersionResponse = ApiVersionsResponse
                        .defaultApiVersionsResponse(ApiMessageType.ListenerType.ZK_BROKER);
                ApiVersionCollection apiVersions = new ApiVersionCollection();
                for (ApiVersion apiVersion : defaultApiVersionResponse.data().apiKeys()) {
                    // ApiVersion can NOT be reused in second ApiVersionCollection
                    // due to the internal pointers it contains.
                    apiVersions.add(apiVersion.duplicate());

                }
                ApiVersionsResponseData data = new ApiVersionsResponseData().setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0).setApiKeys(apiVersions);
                ApiVersionsResponse apiVersionResponse = new ApiVersionsResponse(data);
                responseToSend(apiVersionResponse, apiKey, requestHeader.toResponseHeader(), version);
                break;
            }
            case SASL_HANDSHAKE: {
                if (inFlightRequestCount > 0) return delayRequest(requestAndSize, requestHeader);
                if (requestHeader.apiVersion() >= 1) {
                    // SASL Authenticate will be wrapped in a kafka request
                    // Otherwise it will not be formatted as a kafka request
                    enableKafkaSaslAuthenticateHeaders = true;
                }
                SaslHandshakeResponse saslHandshakeResponse = new SaslHandshakeResponse(
                        new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()).setMechanisms(saslMechanisms));
                responseToSend(saslHandshakeResponse, apiKey, requestHeader.toResponseHeader(), version);
                break;
            }
            case SASL_AUTHENTICATE: {
                SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) requestAndSize.request;
                ProxyChannel.AuthorizationResult authResult = new ProxyChannel.AuthorizationResult(this, requestHeader);
                inFlightRequestCount++;
                try {
                    session = proxySasl.authenticate(authResult, saslAuthenticateRequest.data().authBytes());
                } catch (Exception e) {
                    log.info("Sasl authentication failed: " + e);
                    authorizationResult(requestHeader, false);
                }
                return false; // we are either waiting for authentication or could not even try to connect
            }
            case INIT_PRODUCER_ID: {
            	log.warn("we got an INIT_PRODUCER_ID, this is currently unhandled");
                if (inFlightRequestCount > 0) return delayRequest(requestAndSize, requestHeader);
                InitProducerIdRequest request = (InitProducerIdRequest)requestAndSize.request;
                InitProducerIdRequestData requestData = request.data();
//                data.
//                request.
                InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                		.setErrorCode(Errors.NONE.code())
                		.setProducerId(requestData.producerId())
                		.setProducerEpoch(requestData.producerEpoch());
                InitProducerIdResponse response= new InitProducerIdResponse(responseData);
                responseToSend(response, apiKey, requestHeader.toResponseHeader(), version);
            	break;
            }
            case METADATA: {
                if (inFlightRequestCount > 0) return delayRequest(requestAndSize, requestHeader);
                MetadataRequest metadataRequest = (MetadataRequest) requestAndSize.request;
                MetadataRequestData data = metadataRequest.data();

                List<MetadataResponseData.MetadataResponsePartition> partitionList = new ArrayList<>();
                for(int n = 0; n < partitionsPerTopic; n++) {
                    partitionList.add(new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(n).setErrorCode(Errors.NONE.code()).setLeaderEpoch(1).setLeaderId(0)
                        .setReplicaNodes(Arrays.asList(0)).setIsrNodes(Arrays.asList(0))
                        .setOfflineReplicas(Collections.emptyList()));
                }
                
                MetadataResponseData.MetadataResponseTopicCollection topics = new MetadataResponseData.MetadataResponseTopicCollection();
                for (MetadataRequestData.MetadataRequestTopic topic : data.topics()) {
                    MetadataResponseData.MetadataResponseTopic topicMetadata = new MetadataResponseData.MetadataResponseTopic()
                            .setName(topic.name()).setErrorCode(Errors.NONE.code()).setPartitions(partitionList)
                            .setIsInternal(false);
                    topics.add(topicMetadata);
                }
                MetadataResponse metadataResponse = new MetadataResponse(
                        new MetadataResponseData().setThrottleTimeMs(0).setBrokers(listenPort.brokers())
                                .setClusterId(listenPort.clusterId()).setControllerId(0).setTopics(topics),
                        version);
                responseToSend(metadataResponse, apiKey, requestHeader.toResponseHeader(), version);
                break;
            }
            case PRODUCE: {
                ProduceRequest produceRequest = (ProduceRequest) requestAndSize.request;
                ProduceRequestData produceRequestData = produceRequest.data();
                TopicProduceDataCollection produceTopicData = produceRequestData.topicData();

                // We should not get no topics, and do not want to deal with it
                if (produceTopicData.isEmpty()) {
                    throw new InvalidRequestException("No topics in PRODUCE request");
                }
                
                ProduceResponseData.TopicProduceResponseCollection topicResponseCollection =
                    new ProduceResponseData.TopicProduceResponseCollection(produceTopicData.size());

                ProduceResponseData produceResponseData = new ProduceResponseData()
                    .setThrottleTimeMs(0)
                    .setResponses(topicResponseCollection);
                
                ProduceResponse produceResponse = new ProduceResponse(produceResponseData);
                    
                Iterator<ProduceRequestData.TopicProduceData> topicProduceDataIt = produceTopicData.iterator();
                while (topicProduceDataIt.hasNext()) {
                    ProduceRequestData.TopicProduceData topicProduceData = topicProduceDataIt.next();

                    String topicName = topicProduceData.name();
                    List<PartitionProduceResponse> partitionProduceResponses = new ArrayList<>();
                    ProduceResponseData.TopicProduceResponse topicProduceResponse = (new ProduceResponseData.TopicProduceResponse())
                        .setName(topicName)
                        .setPartitionResponses(partitionProduceResponses);
                    topicResponseCollection.add(topicProduceResponse);

                    Iterator<ProduceRequestData.PartitionProduceData> partitionDataIt = topicProduceData.partitionData().iterator();
                    while (partitionDataIt.hasNext()) {
                        ProduceRequestData.PartitionProduceData partitionData = partitionDataIt.next();
                        
                        int partitionId = partitionData.index();
                        ProduceResponseData.PartitionProduceResponse partitionResponse = (new ProduceResponseData.PartitionProduceResponse())
                            .setIndex(partitionId)
                            .setBaseOffset(-1)
                            .setLogAppendTimeMs(-1)
                            .setLogStartOffset(-1);
                        partitionProduceResponses.add(partitionResponse);

                        int batchCount = 0;
                        int recordCount = 0;
                        MemoryRecords records = (MemoryRecords) partitionData.records();
                        
                        Iterator<MutableRecordBatch> batchIt = records.batchIterator();
                        while (batchIt.hasNext()) {
                            MutableRecordBatch batch = batchIt.next();

                            batchCount++;
                            BufferSupplier.GrowableBufferSupplier supplier = new BufferSupplier.GrowableBufferSupplier();
                            
                            CloseableIterator<Record> recordIt = batch.streamingIterator(supplier);
                            while (recordIt.hasNext()) {
                                Record record = recordIt.next();

                                recordCount++;
                                final byte[] payload;
                                if (record.hasValue()) {
                                    payload = new byte[record.value().remaining()];
                                    record.value().get(payload);
                                } else {
                                    payload = new byte[0];
                                }
                                final byte[] key;
                                if (record.hasKey()) {
                                    key = new byte[record.key().remaining()];
                                    record.key().get(key);
                                } else {
                                    key = null;
                                }
                                boolean lastInRequest = !(recordIt.hasNext() || batchIt.hasNext() || partitionDataIt.hasNext() || topicProduceDataIt.hasNext());
                                
                                final ProduceAckState produceAckState = new ProduceAckState(this, 
                                    produceResponse, partitionResponse,
                                    requestHeader, lastInRequest);
                                
                                inFlightRequestCount++;
                                
                                session.publish(topicName, partitionId, payload, key, produceAckState);
                            }
                        }
                        // We do not want to deal with no records for a topic
                        if (batchCount == 0) {
                            throw new InvalidRequestException("No records in PRODUCE request, topic: " + topicName);
                        }
                        log.trace("Published PRODUCE request: {} records in {} batches.", recordCount, batchCount);
                    }
                }
                break;
            }
            default: {
                log.error("Unhanded request type: " + apiKey.toString());
                break;
            }
        }
		return true;
	}

	// Logic taken from readFrom() in kafka.common.network.NetworkReceive.java
	// only call this from Reactor thread
    // We exit after a message to make sure that we do not keep looping if there is 
    // lots of data to read, BUT we do not exit if bytes buffered in the transport layer
    // due to use of SSL since otherwise we may not wake up again on a read event
    // We also exit if we are told to wait (e.g. authentication request), but later we will be 
    // forced back into this routine even without a read event when we are ready to proceed further.
	void readFromChannel() {
        boolean gotMessage = false;
		try {
			while (transportLayer.hasBytesBuffered() ||
                   (!gotMessage && transportLayer.selectionKey().isReadable())) {
				if (!transportLayer.ready()) {
					transportLayer.handshake();
					if (!transportLayer.ready())
						return;
				}
				if (size.hasRemaining()) {
					int bytesRead = transportLayer.read(size);
					if (bytesRead < 0) {
						close("Channel closed by far end");
						return;
					}
					if (!size.hasRemaining()) {
						// We have the full size of the message
						size.rewind();
						int receiveSize = size.getInt();
						if (receiveSize < 0) {
							close("Invalid receive (size = " + receiveSize + ")");
							return;
						}
						requestedBufferSize = receiveSize; // may be 0 for some payloads (SASL)
						if (receiveSize == 0) {
							buffer = EMPTY_BUFFER;
						}
					} else
						return;
				}
				if (buffer == null && requestedBufferSize != -1) { // we know the size we want but haven't been able to
																   // allocate it yet
					byte[] bytes = new byte[requestedBufferSize];
					buffer = ByteBuffer.wrap(bytes);
				}
				if (buffer != null) {
					int bytesRead = transportLayer.read(buffer);
					if (bytesRead < 0) {
						close("Channel closed by far end");
						return;
					}
					// see if we have the entire message read
					if (!buffer.hasRemaining()) {
						size.clear();
						buffer.rewind();
                        gotMessage = true;
						try {
							final boolean keepReading = parseRequest(buffer);
							buffer = null;
							if (!keepReading)
								return; // do not want to read any more messages (e.g. could be blocked on
										// authentication)
						} catch (Exception e) {
							close("Request parse did not work: " + e.toString());
							buffer = null;
							return;
						}
					}
				} else {
					return;
                }
			}
		} catch (Exception e) {
			close("Channel read error: " + e);
			return;
		}
	}

	// only call this from Reactor thread
	// Writes as much buffered data as possible
	void writeToChannel() {
		try {
			do {
				if (!transportLayer.ready()) {
					transportLayer.handshake();
					if (!transportLayer.ready())
						return;
				}
				Send send = sendQueue.peek();
				if (send != null) {
					send.writeTo(transportLayer);
					if (!send.completed())
						break;
					sendQueue.remove();
				} else {
					transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
					break;
				}
			} while (true);
		} catch (Exception e) {
			close("Channel write error: " + e);
		}
	}

	// Only call this from Reactor thread
	void close(String reason) {
        // Avoid logging if we have already closed (otherwise may get many close logs to due unable to send PRODUCE response)
        if (proxySasl != null) {
            log.info("Cleaning up channel (remote " + transportLayer.socketChannel().socket().getRemoteSocketAddress()
                    + ") due to " + reason);
        }
		listenPort.removeChannel(this);
		if (session != null) {
			session.removeChannel(this);
			session = null;
		}
		proxySasl = null;
		if (transportLayer != null) {
			try {
				transportLayer.selectionKey().cancel();
				transportLayer.close();
			} catch (IOException e) {
				log.error("Exception during channel close: " + e);
			}
		}
		sendQueue.clear();
	}

	// only call this from Reactor thread
	void handleWorkEntry(ProxyReactor.WorkEntry workEntry) {
		// Almost all of the work is ProduceAckState so check for that first
		if (workEntry instanceof ProduceAckState) {
            inFlightRequestCount--;
			produceResponseProcesssing.handleProduceAckState((ProduceAckState) workEntry);
		} else if (workEntry instanceof AuthorizationResult) {
            inFlightRequestCount--;
			AuthorizationResult authResult = (AuthorizationResult) workEntry;
			final boolean worked = authResult.getWorked();
			authorizationResult(authResult.getRequestHeader(), worked);
            if (!worked) return; // if did not work then we are done as channel will be closed
		} else if (workEntry instanceof Close) {
			final Close closeReq = (Close) workEntry;
			close(closeReq.getReason());
            return;
		} else {
			log.error("Unknown work entry type");
            return;
		}
        
        // If there are no more in-flight requests and we had blocked reading from the socket, then we 
        // are ready to read again. We need to immediately read from the channel as we may have buffered
        // bytes in the SSL transport layer even if the socket has nothing in it for reading. Otherwise,
        // the channel may not wake up from select for a read event even though bytes are buffered above the 
        // socket for reading.
        if ((inFlightRequestCount == 0) && (savedRequestAndSize != null)) {
            final RequestAndSize requestAndSize = savedRequestAndSize;
            final RequestHeader requestHeader = savedRequestHeader;
            savedRequestAndSize = null;
            savedRequestHeader = null;
            try {
                handleRequest(requestAndSize, requestHeader);
                transportLayer.addInterestOps(SelectionKey.OP_READ);
                readFromChannel();
            } catch (Exception e) {
			    close("Channel read error during read re-enable: " + e);
            }
        }
	}

}
