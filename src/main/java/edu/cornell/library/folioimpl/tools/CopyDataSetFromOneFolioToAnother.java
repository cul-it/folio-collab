package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.io.InputStream;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CopyDataSetFromOneFolioToAnother {

  public static void main(String[] args) throws IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi31 =   new OkapiClient( prop.getProperty("url31dmg"), prop.getProperty("token31dmg") );
    OkapiClient okapi31sb = new OkapiClient( prop.getProperty("url31sb"),  prop.getProperty("token31sb")  );
    OkapiClient okapi32dmg =new OkapiClient( prop.getProperty("url32dmg"), prop.getProperty("token32dmg") );
    OkapiClient okapi32sb = new OkapiClient( prop.getProperty("url32sb"),  prop.getProperty("token32sb")  );

    OkapiClient from = okapi31;
    OkapiClient to   = okapi32sb;

//    okapi31.deleteAll("/holdings-storage/holdings", true);
//    okapi31sb.deleteAll("/organizations-storage/organizations", true);
//    okapi31sb.deleteAll("/organizations-storage/contacts", true);
//    new CopyDataSetFromOneFolioToAnother(okapi21, okapi31sb, "/organizations-storage/organizations");


    to.deleteAll("/service-points-users", true);
    to.deleteAll("/locations", true);
    to.deleteAll("/location-units/libraries", true);
    to.deleteAll("/location-units/campuses", true);
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/institutions");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/campuses");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/libraries");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/locations");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/service-points");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/material-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/loan-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/groups");

  }

  public static void copy( OkapiClient okapi1, OkapiClient okapi2, String endPoint )
      throws IOException {
    doTheThing( okapi1, okapi2, endPoint, null, null, null, null );
  }

  public static void dependencyFilteredCopy(
      OkapiClient okapi1, OkapiClient okapi2, String endPoint, Dependency filter ) throws IOException {
    doTheThing( okapi1, okapi2, endPoint, null, filter, null, null );
  }

  public static void modifiedCopy( OkapiClient okapi1, OkapiClient okapi2, String endPoint, ModificationLogic mod )
      throws IOException {
    doTheThing( okapi1, okapi2, endPoint, mod, null, null, null );
  }

  public static void dependencyFilteredAndModifiedCopy(
      OkapiClient okapi1, OkapiClient okapi2, String endPoint, Dependency filter, ModificationLogic mod )
          throws IOException {
    doTheThing( okapi1, okapi2, endPoint, mod, filter, null, null );
  }

  public static void whiteListFilteredCopy(
      OkapiClient okapi1, OkapiClient okapi2, String endPoint, String whiteListField, ArrayList<String> whiteList )
      throws IOException {
    doTheThing( okapi1, okapi2, endPoint, null, null, whiteListField, whiteList );
  }

  public static void whiteListFilteredAndModifiedCopy(
      OkapiClient okapi1, OkapiClient okapi2, String endPoint,
      String whiteListField, ArrayList<String> whiteList, ModificationLogic mod )
      throws IOException {
    doTheThing( okapi1, okapi2, endPoint, mod, null, whiteListField, whiteList );
  }

  private static void doTheThing(
      OkapiClient okapi1, OkapiClient okapi2, String endPoint, ModificationLogic mod,
      Dependency dependency, String whiteListField, ArrayList<String> whiteList )
      throws IOException {

    int maxDataSetSize = 15000;

    // Pull values from source system
    String sourceJson = okapi1.query(endPoint, null, maxDataSetSize);
    if ( maxDataSetSize < (Integer)mapper.readValue(sourceJson,Map.class).get("totalRecords") )
      throw new UnsupportedOperationException(
          "Method available for endpoints of no more than "+maxDataSetSize+" records.");
    List<Map<String,Object>> sourceData = OkapiClient.resultsToList(sourceJson);

    // Modify source records if appropriate
    if ( mod != null )
      for ( Map<String,Object> sourceRecord : sourceData )
        mod.modify(sourceRecord);

    // Pull values from target system for comparison
    Map<String,Map<String,Object>> dataOnTarget = okapi2.queryAsMap(endPoint, null, maxDataSetSize);

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
          okapi1.getRecord(dependency.endPoint,foreignKey);
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
        okapi2.post(endPoint,mapper.writeValueAsString(record));
        System.out.println("Added: "+endPoint+' '+mapper.writeValueAsString(record));
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
        okapi2.put(endPoint,record);
        System.out.println("Updated: "+endPoint+' '+mapper.writeValueAsString(record));
      }
    }

    // Delete target system records not reflected in source system
    for (String key : dataOnTarget.keySet()) {
      System.out.printf("deleting %s/%s %s\n",endPoint,key,mapper.writeValueAsString(dataOnTarget.get(key)));
      okapi2.delete(endPoint, key);
    }
  }

  private static ObjectMapper mapper = new ObjectMapper();
}
