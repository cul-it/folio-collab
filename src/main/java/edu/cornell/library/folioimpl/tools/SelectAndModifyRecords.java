package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.folioimpl.interfaces.MatchingLogic;
import edu.cornell.library.folioimpl.interfaces.ModificationLogic;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class SelectAndModifyRecords {
  
  private OkapiClient okapi;
  private String endPoint;
  private String query;
  private MatchingLogic match;
  private ModificationLogic mod;

  public static void run(OkapiClient okapi,String endPoint,String query,MatchingLogic match,ModificationLogic mod)
      throws IOException {

    int maxDataSetSize = 15000;

    // Pull values from source system
    String sourceJson = okapi.query(endPoint, query, maxDataSetSize);
    if ( maxDataSetSize < (Integer)mapper.readValue(sourceJson,Map.class).get("totalRecords") )
      throw new UnsupportedOperationException(
          "Method available for endpoints of no more than "+maxDataSetSize+" matching records.");
    List<Map<String,Object>> sourceData = OkapiClient.resultsToList(sourceJson);
    System.out.printf("%d records matching Okapi query.\n", sourceData.size());
    if ( sourceData.size() == 0 )
    { System.out.println("No changes made"); return; }

    List<Map<String,Object>> filteredSourceData;
    if ( match == null )
      filteredSourceData = sourceData;
    else {
      filteredSourceData = new ArrayList<>();
      for ( Map<String,Object> record : sourceData )
        if ( match.matches(record) )
          filteredSourceData.add(record);
    }
    System.out.printf("%d records matching record filter rules.\n", filteredSourceData.size());
    if ( filteredSourceData.size() == 0 )
    { System.out.println("No changes made"); return; }


    int updated = 0;
    for ( Map<String,Object> record : filteredSourceData ) {

      // If modification changes record, push modified record back.
      if ( mod.modify( record )) {
        okapi.put(endPoint,record);
        System.out.println("Updated: "+endPoint+' '+mapper.writeValueAsString(record));
        updated++;
      } else
        System.out.println("Not updated: "+endPoint+' '+mapper.writeValueAsString(record));
    }
    System.out.printf("%d records updated.", updated);
  }

  private static ObjectMapper mapper = new ObjectMapper();
}
