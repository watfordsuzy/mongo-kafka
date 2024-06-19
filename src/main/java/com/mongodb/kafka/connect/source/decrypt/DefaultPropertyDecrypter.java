package com.mongodb.kafka.connect.source.decrypt;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.CRYPT_PROPERTY_MAP_CONFIG;
import static com.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static com.mongodb.kafka.connect.util.ConfigHelper.documentFromString;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.ClientEncryptionConfig;
import com.mongodb.kafka.connect.util.ConnectConfigException;

@NotThreadSafe
public class DefaultPropertyDecrypter implements PropertyDecrypter {
  private Map<String, String> simplePairs;
  private MongoClientSettings mongoClientSettings;
  private ClientEncryption clientEncryption;

  public DefaultPropertyDecrypter(MongoClientSettings mongoClientSettings) {
    this.mongoClientSettings = assertNotNull(mongoClientSettings);
  }

  @Override
  public void configure(final MongoSourceConfig config) {
    final ConfigExceptionSupplier configExceptionSupplier =
        message ->
            new ConnectConfigException(
                CRYPT_PROPERTY_MAP_CONFIG, config.getString(CRYPT_PROPERTY_MAP_CONFIG), message);

    Document propertyMap =
        documentFromString(config.getString(CRYPT_PROPERTY_MAP_CONFIG)).orElse(new Document());
    simplePairs = new HashMap<>();
    for (Entry<String, Object> pair : propertyMap.entrySet()) {
      String fieldPath = pair.getKey();
      Object value = pair.getValue();
      if (!(value instanceof String)) {
        throw configExceptionSupplier.apply("All values must be strings");
      }
      String decryptedPropertyName = (String) pair.getValue();
      simplePairs.put(fieldPath, decryptedPropertyName);
    }

    ClientEncryptionConfig clientEncryptionConfig =
        assertNotNull(config.getClientEncryptionConfig());
    clientEncryption =
        ClientEncryptions.create(
            clientEncryptionConfig.buildClientEncryptionSettings(mongoClientSettings));
  }

  @Override
  public BsonDocument decryptProperties(BsonDocument document) {
    for (Entry<String, String> pair : simplePairs.entrySet()) {
      String fieldPath = pair.getKey();
      String decryptedFieldPath = pair.getValue();

      BsonValue bsonValue = fieldLookup(fieldPath, document).orElse(new BsonNull());
      if (bsonValue.isBinary()) {
        BsonValue decrypted = clientEncryption.decrypt(bsonValue.asBinary());
        fieldSet(decryptedFieldPath, document, decrypted);
      }
    }
    return document;
  }

  private static void fieldSet(
      final String fieldPath, final BsonDocument document, final BsonValue value) {
    if (document.containsKey(fieldPath)) {
      document.put(fieldPath, value);
    } else if (fieldPath.contains(".") && !fieldPath.endsWith(".")) {
      String subDocumentName = fieldPath.substring(0, fieldPath.indexOf("."));
      String subDocumentFieldName = fieldPath.substring(fieldPath.indexOf(".") + 1);
      if (document.isDocument(subDocumentName)) {
        fieldSet(subDocumentFieldName, document.getDocument(subDocumentName), value);
      }
    }
  }

  @FunctionalInterface
  private interface ConfigExceptionSupplier extends Function<String, ConnectConfigException> {}
}
