package com.mongodb.kafka.connect.source.decrypt;

import org.bson.BsonDocument;

import com.mongodb.annotations.NotThreadSafe;

import com.mongodb.kafka.connect.source.Configurable;

@NotThreadSafe
public interface PropertyDecrypter extends Configurable {
  BsonDocument decryptProperties(BsonDocument document);
}
