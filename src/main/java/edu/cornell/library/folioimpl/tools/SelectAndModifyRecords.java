package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SelectAndModifyRecords {

  public static void run( OkapiClient okapi, String endPoint, String query , ModificationLogic mod )
      throws IOException {

    int maxDataSetSize = 15000;

    // Pull values from source system
    String sourceJson = okapi.query(endPoint, query, maxDataSetSize);
    if ( maxDataSetSize < (Integer)mapper.readValue(sourceJson,Map.class).get("totalRecords") )
      throw new UnsupportedOperationException(
          "Method available for endpoints of no more than "+maxDataSetSize+" matching records.");
    List<Map<String,Object>> sourceData = OkapiClient.resultsToList(sourceJson);

    for ( Map<String,Object> record : sourceData ) {

      // If modification changes record, push modified record back.
      if ( mod.modify( record )) {
        okapi.put(endPoint,record);
        System.out.println("Updated: "+endPoint+' '+mapper.writeValueAsString(record));
      }
    }

  }

  private static ObjectMapper mapper = new ObjectMapper();
}
