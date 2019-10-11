package edu.cornell.library.folioimpl.objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReferenceData {

  // PUBLIC METHODS

  /*
   * Retrieve the complete data set (up to 4000) items from OKAPI, and build a
   * reference map to identify UUIDs based on key values. The UUIDs are assumed to
   * be the "id" field in the data set, and the provided key field is the field
   * that will be used to find the UUID values. This currently only works with top
   * level key fields, and those where the key field values are strings. If other
   * use cases arise, this can be expanded.
   */
  public ReferenceData(OkapiClient okapi, String endPoint, String keyField) throws IOException {
    String json = okapi.query(endPoint , null, 4000);
    this.dataMap = getMapFromJson(json, keyField);

  }

  /*
   * Get the UUID for the given key value. If the key value isn't populated,
   * return default or null.
   */
  public String getUuid(String keyValue) {
    if (this.defaultKey == null || this.dataMap.containsKey(keyValue))
      return this.dataMap.get(keyValue);
    return this.dataMap.get(this.defaultKey);
  }

  /*
   * Get the UUID for the given key value. If key value isn't populated, return
   * null.
   */
  public String getStrictUuid(String keyValue) {
    return this.dataMap.get(keyValue);
  }

  /*
   * Set default key value to use when invalid key is requested. Throws
   * IllegalArgumentException if invalid.
   */
  public void setDefault(String defaultKey) throws IllegalArgumentException {
    if (defaultKey != null && !this.dataMap.containsKey(defaultKey))
      throw new IllegalArgumentException("Default key \"" + defaultKey + "\" is not a valid key.");
    this.defaultKey = defaultKey;
  }

  public void writeMapToStdout() {
    for (Entry<String, String> e : this.dataMap.entrySet())
      System.out.printf("%s => %s\n", e.getKey(), e.getValue());
  }

  // PRIVATE VALUES AND METHODS

  private final Map<String, String> dataMap;
  private static ObjectMapper mapper = new ObjectMapper();
  private String defaultKey = null;

  private static Map<String, String> getMapFromJson(String json, String keyField)
      throws JsonParseException, JsonMappingException, IOException {
    Map<String, Object> rawData = mapper.readValue(json, Map.class);
    Map<String, String> processedMap = new HashMap<>();
    for (String mainKey : rawData.keySet()) {

      if (mainKey.equals("totalRecords"))
        continue;
      Object mainValue = rawData.get(mainKey);
      if (!mainValue.getClass().equals(ArrayList.class))
        continue;

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> entries = (ArrayList<Map<String, Object>>) mainValue;
      for (Map<String, Object> entry : entries)
        processedMap.put((String) entry.get(keyField), (String) entry.get("id"));
    }
    return processedMap;
  }

}
