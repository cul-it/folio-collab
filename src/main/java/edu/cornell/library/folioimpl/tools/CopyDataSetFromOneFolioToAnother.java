package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CopyDataSetFromOneFolioToAnother {

  private OkapiClient sourceOkapi = null;
  private OkapiClient destOkapi = null;
  private String endPoint = null;
  private boolean deleteUnmatched = true;

  private ModificationLogic mod = null;
  private Dependency dependencyFilter = null;
  private String whiteListField = null;
  private List<String> whiteList = null;
  private Set<CreateRecord> satellites = null;


  public static void copy( OkapiClient okapi1, OkapiClient okapi2, String endPoint )
      throws IOException {
    doTheThing( okapi1, okapi2, endPoint, true, null, null, null, null, null );
  }

  public void execute() throws IOException {
    doTheThing(
        this.sourceOkapi, this.destOkapi, this.endPoint, this.deleteUnmatched,
        this.mod, this.dependencyFilter, this.whiteListField, this.whiteList, this.satellites );
  }

  CopyDataSetFromOneFolioToAnother(
      OkapiClient sourceOkapi, OkapiClient destOkapi, String endPoint, boolean deleteUnmatched, ModificationLogic mod,
      Dependency dependency, String whiteListField, List<String> whiteList, Set<CreateRecord> satellites) {
    this.sourceOkapi = sourceOkapi;
    this.destOkapi = destOkapi;
    this.endPoint = endPoint;
    this.mod = mod;
    this.deleteUnmatched = deleteUnmatched;
    this.dependencyFilter = dependency;
    this.whiteListField = whiteListField;
    this.whiteList = whiteList;
    this.satellites = satellites;
  }

  public static class Builder {

    private OkapiClient sourceOkapi = null;
    private OkapiClient destOkapi = null;
    private String endPoint = null;
    private boolean deleteUnmatched = true;

    private ModificationLogic mod = null;
    private Dependency dependencyFilter = null;
    private String whiteListField = null;
    private List<String> whiteList = null;
    private Set<CreateRecord> satellites = null;

    public Builder setSourceOkapi( OkapiClient sourceOkapi ) {
      this.sourceOkapi = sourceOkapi;
      return this;
    }

    public Builder setDestOkapi( OkapiClient destOkapi ) {
      this.destOkapi = destOkapi;
      return this;
    }

    public Builder setEndPoint( String endPoint ) {
      this.endPoint = endPoint;
      return this;
    }

    public Builder setDeleteUnmatched( boolean deleteUnmatched ) {
      this.deleteUnmatched = deleteUnmatched;
      return this;
    }

    public Builder setDependencyFilter( Dependency dependencyFilter ) {
      this.dependencyFilter = dependencyFilter;
      return this;
    }

    public Builder setWhiteListFilter( String whiteListField, List<String> whiteList ) {
      this.whiteListField = whiteListField;
      this.whiteList = whiteList;
      return this;
    }

    public Builder setSatellites( Set<CreateRecord> satellites ) {
      this.satellites = satellites;
      return this;
    }

    public Builder addSatellite( CreateRecord satellite ) {
      if ( this.satellites == null ) this.satellites = new HashSet<>();
      this.satellites.add(satellite);
      return this;
    }

    public Builder setModificationLogic( ModificationLogic modificationLogic ) {
      this.mod = modificationLogic;
      return this;
    }

    public CopyDataSetFromOneFolioToAnother build() throws IllegalArgumentException {
      return new CopyDataSetFromOneFolioToAnother(this.sourceOkapi,this.destOkapi,this.endPoint,this.deleteUnmatched,
          this.mod,this.dependencyFilter,this.whiteListField,this.whiteList,this.satellites);
    }

  }

  private static void doTheThing(
      OkapiClient sourceOkapi, OkapiClient destOkapi, String endPoint, boolean deleteUnmatched, ModificationLogic mod,
      Dependency dependency, String whiteListField, List<String> whiteList, Set<CreateRecord> satellites )
      throws IOException {

    int maxDataSetSize = 15000;

    // Pull values from source system
    String sourceJson = sourceOkapi.query(endPoint, null, maxDataSetSize);
    if ( maxDataSetSize < (Integer)mapper.readValue(sourceJson,Map.class).get("totalRecords") )
      throw new UnsupportedOperationException(
          "Method available for endpoints of no more than "+maxDataSetSize+" records.");
    List<Map<String,Object>> sourceData = OkapiClient.resultsToList(sourceJson);

    // Modify source records if appropriate
    if ( mod != null )
      for ( Map<String,Object> sourceRecord : sourceData )
        mod.modify(sourceRecord);

    // Pull values from target system for comparison
    Map<String,Map<String,Object>> dataOnTarget = destOkapi.queryAsMap(endPoint, null, maxDataSetSize);

    Map<String,Boolean> dependenciesPresent = new HashMap<>();

    // Update (if necessary) source records onto target system
    for ( Map<String,Object> record : sourceData ) {
      String id = (String) record.get("id");

      // If dependency filter set, check dependency is present
      if ( dependency != null ) {
        if ( ! record.containsKey(dependency.foreignKey) )
          continue;
        String foreignKey = (String) record.get(dependency.foreignKey);
        if ( dependenciesPresent.containsKey(foreignKey) ) {
          if ( ! dependenciesPresent.get(foreignKey) ) continue;
        } else try {
          sourceOkapi.getRecord(dependency.endPoint,foreignKey);
          dependenciesPresent.put(foreignKey,true);
        } catch (@SuppressWarnings("unused") NoSuchObjectException e ) {
          dependenciesPresent.put(foreignKey,false);
          continue;
        }
      }

      // If white list filtering, check list
      if ( whiteListField != null && whiteList != null ) {
        if ( ! record.containsKey(whiteListField) )
          continue;
        String whiteListFieldValue = (String) record.get(whiteListField);
        if ( ! whiteList.contains(whiteListFieldValue) )
          continue;
      }

      //If new record. Add it.
      if ( ! dataOnTarget.containsKey(id) ) {
        destOkapi.post(endPoint,mapper.writeValueAsString(record));
        System.out.println("Added: "+endPoint+' '+mapper.writeValueAsString(record));
        if ( satellites != null )
          for (CreateRecord cr : satellites) {
            Map<String,Object> satRecord = cr.buildRecord(record);
            destOkapi.post(cr.getEndPoint(),mapper.writeValueAsString(satRecord));
          }
        continue;
      }

      // Remove create and modify info for comparison's sake. (This data will be ignored if sent to Okapi.) 
      record.remove("metadata");
      dataOnTarget.get(id).remove("metadata");

      // If unchanged record. Leave it alone.
      if ( Objects.equals(record, dataOnTarget.get(id)) ) {
        dataOnTarget.remove(id);
        System.out.println("Already correct: "+endPoint+' '+mapper.writeValueAsString(record));

      // Else changed record. Update it.
      } else {
        dataOnTarget.remove(id);
        destOkapi.put(endPoint,record);
        System.out.println("Updated: "+endPoint+' '+mapper.writeValueAsString(record));
      }
    }

    // Delete target system records not reflected in source system
    if ( deleteUnmatched )
      for (String key : dataOnTarget.keySet()) {
        System.out.printf("deleting %s/%s %s\n",endPoint,key,mapper.writeValueAsString(dataOnTarget.get(key)));
        destOkapi.delete(endPoint, key);
      }
  }


  private static ObjectMapper mapper = new ObjectMapper();
}
