package com.wepay.kafka.connect.bigquery.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldNameSanitizerTest {
  HashMap testMap;

  @Before
  public void setUp() {
    testMap =
        new HashMap<String, HashMap<?, ?>>() {{
          put("A.1", new HashMap<String, Object>() {{
            put("_B1", 1);
            put("B.2", "hello.B-2");
          }});
          put("A-2", new HashMap<String, Object>() {{
            put("=/B.3", "hello B3");
            put("B./4", "hello B4");
            put("2A/", "hello B5");
            put("3A/", "hello B6");
          }});
        }};
  }

  @Test
  public void testInvalidSymbol() {
    testMap = (HashMap)FieldNameSanitizer.replaceInvalidKeys(testMap);
    assertTrue(testMap.containsKey("A_1"));
    assertTrue(testMap.containsKey("A_2"));
    assertTrue(((HashMap)(testMap.get("A_1"))).containsKey("B_2"));
    assertTrue(((HashMap)(testMap.get("A_1"))).containsKey("_B1"));

    assertEquals(((HashMap)(testMap.get("A_1"))).get("B_2"), "hello.B-2");
    assertEquals(((HashMap)(testMap.get("A_1"))).get("_B1"), 1);

    assertTrue(((HashMap)(testMap.get("A_2"))).containsKey("__B_3"));
    assertTrue(((HashMap)(testMap.get("A_2"))).containsKey("B__4"));

    assertEquals(((HashMap)(testMap.get("A_2"))).get("__B_3"), "hello B3");
    assertEquals(((HashMap)(testMap.get("A_2"))).get("B__4"), "hello B4");

    assertEquals(((HashMap)(testMap.get("A_2"))).get("_2A_"), "hello B5");
    assertEquals(((HashMap)(testMap.get("A_2"))).get("_3A_"), "hello B6");
  }
}
