package com.wepay.kafka.connect.bigquery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class GoogleCredentialUtil {
  private static final Logger logger = LoggerFactory.getLogger(GoogleCredentialUtil.class);

  private static final String GOOGLE_API_URL = "https://www.googleapis.com/auth/cloud-platform";
  private static final String GOOGLE_ACCOUNT_TYPE = "type";
  private static final String PROJECT_ID = "project_id";
  private static final String PRIVATE_KEY_ID = "private_key_id";
  private static final String PRIVATE_KEY = "private_key";
  private static final String CLIENT_EMAIL = "client_email";
  private static final String CLIENT_ID = "client_id";

  public static GoogleCredentials getCredentials(String fileName) {
    logger.debug("Attempting to open file {} for service account json key", fileName);
    try {
      if (isValidJsonFile(fileName)) {
        return GoogleCredentials
                .fromStream(new FileInputStream(fileName));
      } else {
        return buildCredentialsFromProperties(fileName);
      }
    } catch (IOException e) {
      throw new BigQueryConnectException("No valid credentials source was provided.");
    }
  }

  private static String readAllBytesFromFile(String filePath) throws IOException {
    String content = "";
    try {
      content = new String(Files.readAllBytes(
              Paths.get(filePath)), StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      throw new IOException(
              "Unable to read contents of credentials file at: " + filePath, e);
    }
    return content;
  }

  private static boolean isValidJsonFile(String pathToFile) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      mapper.readTree(readAllBytesFromFile(pathToFile));
      return true;
    } catch (IOException e) {
      logger.warn("Input file is not valid json", e);
    }
    return false;
  }

  private static GoogleCredentials buildCredentialsFromProperties(
          String pathToCredentials
  ) throws IOException {
    try (FileInputStream credentialsStream = new FileInputStream(pathToCredentials)) {
      Properties prop = new Properties();
      prop.load(credentialsStream);
      Set<String> credentialsFields = new HashSet<>();
      credentialsFields.add(GOOGLE_ACCOUNT_TYPE);
      credentialsFields.add(PROJECT_ID);
      credentialsFields.add(PRIVATE_KEY_ID);
      credentialsFields.add(PRIVATE_KEY);
      credentialsFields.add(CLIENT_EMAIL);
      credentialsFields.add(CLIENT_ID);
      Map<String, String> valueMap = new HashMap<>();
      prop.forEach((k,v) -> valueMap.put((String)k, (String)v));
      Map<String, String> credentialsMap = new HashMap<>();
      for (Map.Entry<String,String> entry : valueMap.entrySet()) {
        String key = entry.getKey().replaceAll("\"", "").replaceAll(",", "");
        if (credentialsFields.contains(key)) {
          String val = entry.getValue().replaceAll("\"", "").replaceAll(",", "");
          credentialsMap.put(key, val);
        }
      }
      String jsonString = new ObjectMapper().writeValueAsString(credentialsMap);
      InputStream is = new ByteArrayInputStream(
              jsonString.getBytes(Charset.forName(StandardCharsets.UTF_8.name())));
      return GoogleCredentials.fromStream(is)
              .createScoped(Collections.singletonList(GOOGLE_API_URL));
    } catch (IOException e) {
      logger.error("Unable to build credentials from properties file", e);
      throw e;
    }
  }

}
