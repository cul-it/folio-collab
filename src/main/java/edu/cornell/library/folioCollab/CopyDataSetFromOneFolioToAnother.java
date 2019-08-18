package edu.cornell.library.folioCollab;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Base64.Decoder;
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

    OkapiClient okapi21 =   new OkapiClient( prop.getProperty("url21dmg"), prop.getProperty("token21dmg") );
    OkapiClient okapi22 =   new OkapiClient( prop.getProperty("url22sb"),  prop.getProperty("token22sb")  );
    OkapiClient okapi31 =   new OkapiClient( prop.getProperty("url31dmg"), prop.getProperty("token31dmg") );
    OkapiClient okapi31sb = new OkapiClient( prop.getProperty("url31sb"),  prop.getProperty("token31sb")  );

    OkapiClient from = okapi31;
    OkapiClient to   = okapi31sb;

    okapi31.deleteAll("/holdings-storage/holdings", true);
//    okapi31sb.deleteAll("/organizations-storage/organizations", true);
//    okapi31sb.deleteAll("/organizations-storage/contacts", true);
//    new CopyDataSetFromOneFolioToAnother(okapi21, okapi31sb, "/organizations-storage/organizations");

    /*
    to.deleteAll("/service-points-users", true);
    to.deleteAll("/locations", true);
    to.deleteAll("/location-units/libraries", true);
    to.deleteAll("/location-units/campuses", true);
    new CopyDataSetFromOneFolioToAnother(from, to, "/location-units/institutions");
    new CopyDataSetFromOneFolioToAnother(from, to, "/location-units/campuses");
    new CopyDataSetFromOneFolioToAnother(from, to, "/location-units/libraries");
    new CopyDataSetFromOneFolioToAnother(from, to, "/locations");
    new CopyDataSetFromOneFolioToAnother(from, to, "/service-points");
    new CopyDataSetFromOneFolioToAnother(from, to, "/material-types");
    new CopyDataSetFromOneFolioToAnother(from, to, "/loan-types");
    new CopyDataSetFromOneFolioToAnother(from, to, "/groups");
*/
  }

  public CopyDataSetFromOneFolioToAnother( OkapiClient okapi1, OkapiClient okapi2, String endPoint )
      throws IOException {

    int maxDataSetSize = 15000;

    // Pull values from source system
    String sourceJson = okapi1.query(endPoint, null, maxDataSetSize);
    sourceJson.replaceAll("\"types\":\\[\\],", "");
    if ( maxDataSetSize < (Integer)mapper.readValue(sourceJson,Map.class).get("totalRecords") )
      throw new UnsupportedOperationException(
          "Method available for endpoints of no more than "+maxDataSetSize+" records.");
    List<Map<String,Object>> sourceData = OkapiClient.resultsToList(sourceJson);

    // Pull values from target system for comparison
    Map<String,Map<String,Object>> dataOnTarget = okapi2.queryAsMap(endPoint, null, maxDataSetSize);

    // Update (if necessary) source records onto target system
    // TODO delete record update metadata before comparison to prevent false difference detection
    for ( Map<String,Object> record : sourceData ) {
      String id = (String) record.get("id");

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
