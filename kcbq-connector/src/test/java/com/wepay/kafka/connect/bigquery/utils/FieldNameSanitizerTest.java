package com.wepay.kafka.connect.bigquery.utils;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldNameSanitizerTest {
  HashMap testMap;


  @Test
  public void testInvalidSymbol() {
    testMap =
            new HashMap<String, HashMap<?, ?>>() {{
              put("A.1", new HashMap<String, Object>() {{
                put("_B1", 1);
                put("B.2", "hello.B-2");
              }});
              put("A-2", new HashMap<String, Object>() {{
                put("=/B.3", "hello B3");
                put("B./4", "hello B4");
              }});
            }};

    testMap = (HashMap)FieldNameSanitizer.replaceInvalidKeys(testMap);
    assertTrue(testMap.containsKey("A_1"));
    assertTrue(testMap.containsKey("A_2"));
    assertTrue(((HashMap)(testMap.get("A_1"))).containsKey("B_2"));
    assertTrue(((HashMap)(testMap.get("A_1"))).containsKey("_B1"));

    assertEquals(((HashMap)(testMap.get("A_1"))).get("B_2"), "hello.B-2");
    assertEquals(((HashMap)(testMap.get("A_1"))).get("_B1"), 1);

    assertTrue(((HashMap)(testMap.get("A_2"))).containsKey("B_3"));
    assertTrue(((HashMap)(testMap.get("A_2"))).containsKey("B__4"));

    assertEquals(((HashMap)(testMap.get("A_2"))).get("B_3"), "hello B3");
    assertEquals(((HashMap)(testMap.get("A_2"))).get("B__4"), "hello B4");

  }

}
