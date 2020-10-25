/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANYc KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package com.dcsquare.blog;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.NetworkInterface;

import com.mongodb.client.MongoCollection;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import java.net.UnknownHostException;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDatabase;
import org.bson.*;

/**
 * @author Dominik Obermaier
 */
public class SubscribeCallBackMongoDB implements MqttCallback {

    private static final Logger log = LoggerFactory.getLogger(WildcardSubscriber.class);

    private static final long RECONNECT_INTERVAL = TimeUnit.SECONDS.toMillis(10);

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/sys?user=root&password=password";

    //private static final String SQL_INSERT = "INSERT INTO `Messages` (`message`,`topic`,`quality_of_service`) VALUES (?,?,?)";


    private final MqttClient mqttClient;

    private PreparedStatement statement;

    private MongoCollection<Document> collection;
    //private DBObject document;

    public SubscribeCallBackMongoDB(final MqttClient mqttClient) {
        this.mqttClient = mqttClient;

        try {
            MongoClient mongoClient = new MongoClient("localhost", 27017);
            // Creating Credentials
            MongoCredential credential;
            credential = MongoCredential.createCredential("saie", "sensor",
                    "password".toCharArray());
            MongoDatabase database = mongoClient.getDatabase("sensor");
            database.createCollection("temperature");
            collection = database.getCollection("temperature");



            //mongoClient.close();
        } catch(Exception e)
        {
            System.out.println(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        try {
            log.info("Connection lost. Trying to reconnect");

            //Let's try to reconnect
            mqttClient.connect();

        } catch (MqttException e) {
            log.error("", e);
            try {
                Thread.sleep(RECONNECT_INTERVAL);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
            connectionLost(e);
        }
    }

    @Override
    public void messageArrived(MqttTopic topic, MqttMessage message) throws Exception {
        log.info("Message arrived with QoS {} on Topic {}. Message size: {} bytes", message.getQos(), topic.getName(), message.getPayload().length);

        try {

            String temp = new String(message.getPayload());
            Document document = new Document("Temp", temp)
                    .append("Topic", topic.getName())
                    .append("Quality", message.getQos());

            //Inserting document into the collection
            collection.insertOne(document);

        } catch (Exception e) {
            System.out.println(topic.getName() + " " + message.getPayload());
            log.error("Error while inserting", e);
        }

    }

    @Override
    public void deliveryComplete(MqttDeliveryToken token) {
        //no-op
    }
}
