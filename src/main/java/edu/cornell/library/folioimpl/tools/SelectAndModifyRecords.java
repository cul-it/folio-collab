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

  public void execute() throws IOException {
    doTheThing( this.okapi, this.endPoint, this.query, this.match, this.mod);
  }

  public static class Builder {

    private OkapiClient okapi = null;
    private String endPoint = null;
    private String query = null;
    private MatchingLogic match = null;
    private ModificationLogic mod = null;

    public Builder setOkapiClient( OkapiClient okapi ) {
      this.okapi = okapi;
      return this;
    }

    public Builder setEndPoint( String endPoint ) {
      this.endPoint = endPoint;
      return this;
    }

    public Builder setModificationLogic( ModificationLogic mod ) {
      this.mod = mod;
      return this;
    }

    public Builder setQuery( String query ) {
      this.query = query;
      return this;
    }

    public Builder setMatchingLogic( MatchingLogic match ) {
      this.match = match;
      return this;
    }

    public SelectAndModifyRecords build() throws IllegalArgumentException {
      if ( this.okapi == null || this.endPoint == null || this.mod == null )
        throw new IllegalArgumentException("OkapiClient, end point, and ModificationLogic must all be specified.");
      return new SelectAndModifyRecords( this.okapi, this.endPoint, this.query, this.match, this.mod );
    }

  }

  SelectAndModifyRecords(OkapiClient okapi,String endPoint,String query,MatchingLogic match,ModificationLogic mod) {
    this.okapi = okapi;
    this.endPoint = endPoint;
    this.query = query;
    this.match = match;
    this.mod = mod;
  }

  private static void doTheThing(
      OkapiClient okapi,String endPoint,String query,MatchingLogic match,ModificationLogic mod)
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
