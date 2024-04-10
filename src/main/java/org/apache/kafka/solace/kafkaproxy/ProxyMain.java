package org.apache.kafka.solace.kafkaproxy;

/*
 * Copyright 2021 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

import java.util.Properties;
import java.util.UUID;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

public class ProxyMain {
    
    private static final Logger log = initializeLogger();
    private final String clusterId;
    
    public ProxyMain() {
        UUID uuid = UUID.randomUUID();
        this.clusterId = uuid.toString();
        log.debug("Cluster id: " + this.clusterId);
        log.trace("Trace logging enabled.");
    }
    
     private void startup(String args[]) {
        
        if (args.length <= 0) {
            log.warn("No properties file specified on command line");
            return;
        }
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(args[0])) {
            props.load(input);
        } catch (IOException ex) {
            log.warn("Could not load properties file: " + ex);
            return;
        }
        
        ProxyPubSubPlusClient.getInstance().configure(props);
        
        try {
            final ProxyReactor proxyReactor = new ProxyReactor(new ProxyConfig(props), clusterId);
            proxyReactor.start();
            proxyReactor.join();
        } catch (Exception e) {
            log.warn(e.toString());
        }
        log.info("Proxy no longer running");
    }
  
     /**
     * @param args the command line arguments
     */
     public static void main(String[] args) {
        ProxyMain m = new ProxyMain();
        m.startup(args);
    }

    public static Logger initializeLogger() {
        System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
        System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "yyyy-MM-dd' 'HH:mm:ss.SSS");
        return LoggerFactory.getLogger(ProxyMain.class);
    }
    
}
