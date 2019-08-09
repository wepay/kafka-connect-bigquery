package com.wepay.kafka.connect.bigquery.utils;

import java.util.Map;
import java.util.stream.Collectors;

public class FieldNameSanitizer {

  // replace all non-letter, non-digit characters with underscore
  // append underscore in front of name if it does not begin with letter or underscore
  private static String sanitizeName(String name) {
    name = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (name.matches("^[^a-zA-Z_]+.*")) {
      name = "_" + name;
    }
    return name;
  }


  // Big Query specifies field name must begin with a letter or underscore and can only contain
  // letters, numbers, and underscores
  // Note that a.b and a/b will have the same value after sanitization
  // which will cause Duplicate key Exception
  public static Map<String,Object> replaceInvalidKeys(Map<String, Object> map) {

    return map.entrySet().stream().collect(Collectors.toMap(
            (entry) -> {
              return sanitizeName(entry.getKey());
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
