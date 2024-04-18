package org.apache.kafka.solace.kafkaproxy;

/*
 * Copyright 2021 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */


import java.util.Properties;
import java.util.Map;
import java.lang.Object;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ProxyPubSubPlusClient {
    private static final Logger log = LoggerFactory.getLogger(ProxyPubSubPlusClient.class);
    private static ProxyPubSubPlusClient client = new ProxyPubSubPlusClient();
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor(); // TEMPORARY until async connect available
    private ProxyConfig proxyConfig;
    private Map<ByteArrayWrapper, ProxyPubSubPlusSession> map;
    
    private final class ByteArrayWrapper
    {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data)
        {
            this.data = data;
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ByteArrayWrapper))
            {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper)other).data);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(data);
        }
    }

    
    private ProxyPubSubPlusClient() {
        map = new HashMap<ByteArrayWrapper, ProxyPubSubPlusSession>();
    }
    
    public static ProxyPubSubPlusClient getInstance() { return client; }
    
    public static ExecutorService getExecutorService() { return executorService; }
    
    public static void close() { executorService.shutdownNow(); }        
    
    public void configure(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }
    
    // This is inefficient but we do not have very many session entries
    public void removeSession(ProxyPubSubPlusSession session) {
        synchronized(map) {
            ByteArrayWrapper key = null;
            for (Map.Entry<ByteArrayWrapper, ProxyPubSubPlusSession> entry : map.entrySet()) {
                if (entry.getValue() == session) {
                    key = entry.getKey();
                    break;
                }
            }
            if (key != null) {
                map.remove(key);
            }
            log.debug("New session count after session remove: " + map.size());
        }
    }
    
    public ProxyPubSubPlusSession connect(
                ProxyChannel.AuthorizationResult authResult, 
                byte[] username, byte[] password) {
        byte[] hostName = authResult.getProxyChannel().getHostName().getBytes();
        byte[] toHash = new byte[username.length + password.length + hostName.length];
        int toHashIndex = 0;
        for (int i = 0; i < username.length; i++) {
            toHash[toHashIndex++] = username[i];
        }
        for (int i = 0; i < password.length; i++) {
            toHash[toHashIndex++] = password[i];
        }
        for (int i = 0; i < hostName.length; i++) {
            toHash[toHashIndex++] = hostName[i];
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            ByteArrayWrapper hashWrapper = new ByteArrayWrapper(md.digest(toHash));
            ProxyPubSubPlusSession session;
            synchronized (map) {
                if (map.containsKey(hashWrapper)) {
                    session = map.get(hashWrapper);
                    session.addChannel(authResult.getProxyChannel());
                } else {
                    try {
                        session =  new ProxyPubSubPlusSession(
                                            proxyConfig, authResult.getProxyChannel(),
                                            username, password);
                    } catch (Exception e) {
                        throw new SaslAuthenticationException("Authentication failed: could not establish session to Solace broker: " + e);
                    }
                    map.put(hashWrapper, session);
                }
            }
            // wipe out the pre-hashed information since it contains a password
            Arrays.fill(toHash, (byte)0);
            try {
                session.connect(authResult);
            } catch (Exception e) {
                session.removeChannel(authResult.getProxyChannel());
                throw new SaslAuthenticationException("Authentication failed: could not connect session: " + e);
            }
            return session;
        } catch (NoSuchAlgorithmException e) {
            throw new SaslAuthenticationException("Authentication failed: could not hash authentication information: " + e);
        }
    }
            
}

