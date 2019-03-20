package com.wepay.kafka.connect.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads Google credentials from different sources.
 */
public class GoogleCredentialsReader {
    private static final Logger logger = LoggerFactory.getLogger(GoogleCredentialsReader.class);

    /**
     * Returns {@link GoogleCredentials} defined by a service account credentials file or
     * user credentials file in JSON format.
     * @param filename the name of the file.
     * @return the credential defined by the file.
     * @throws BigQueryConnectException if the credential cannot be created from the file.
     */
    public static GoogleCredentials fromJsonFile(String filename) {
        logger.debug("Attempting to open credentials file {}", filename);
        try (InputStream stream = new FileInputStream(filename)) {
            return GoogleCredentials.fromStream(stream);
        } catch (IOException err) {
            throw new BigQueryConnectException("Failed to read credentials from file", err);
        }
    }

    /**
     * Returns {@link GoogleCredentials} defined by a service account credentials or
     * user credentials in JSON format.
     * @param jsonString the string with the credentials.
     * @return the credential defined by the JSON string.
     * @throws BigQueryConnectException if the credential cannot be created from the string.
     */
    public static GoogleCredentials fromJsonString(String jsonString) {
        try (InputStream stream = new ByteArrayInputStream(
            jsonString.getBytes(StandardCharsets.UTF_8))) {
            return GoogleCredentials.fromStream(stream);
        } catch (IOException err) {
            throw new BigQueryConnectException("Failed to read credentials from JSON string", err);
        }
    }
}
