package com.wepay.kafka.connect.bigquery.utils;

import java.util.Map;
import java.util.stream.Collectors;

public class FieldNameSanitizer {

  private static String sanitizeName(String name) {
    name = name.replaceAll("^[^a-zA-Z_]+", "");
    return name.replaceAll("[^a-zA-Z0-9_]", "_");
  }


  // Big Query specifies field name must begin with a letter or underscore and can only contain
  // letters, numbers, and underscores
  public static Map<String,Object> replaceInvalidKeys(Map<String, Object> map) {

    return map.entrySet().stream().collect(Collectors.toMap(
            (entry) -> {
              return sanitizeName((String) (entry.getKey()));
            },
            (entry) -> {
              if (entry.getValue() instanceof Map) {
                return replaceInvalidKeys((Map) entry.getValue());
              }
              return entry.getValue();

            }
    ));
  }
}
