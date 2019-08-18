package edu.cornell.library.folioCollab;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

public class GetHoldingsForInstances {

  public static void main(String[] args) throws IOException, SQLException, NumberFormatException, XMLStreamException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    String instanceEndPoint = "/instance-storage/instances";

    OkapiClient okapi = new OkapiClient(prop.getProperty("url31sb"),prop.getProperty("token31sb"));

    ReferenceData identifierTypes = new ReferenceData(okapi, "/identifier-types", "name");
    String localId = identifierTypes.getUuid("Local Identifier");
    String sysCtrlNum = identifierTypes.getUuid("System control number");

    List<String> skippedInstances = new ArrayList<>();
    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass")) ) {

      List<Map<String,Object>> instances = okapi.queryAsList(instanceEndPoint,"id==* sortBy metadata.updatedDate",null);
      String modDateCursor = processInstanceBatch(voyager, okapi, instances, localId, sysCtrlNum, skippedInstances);

//      String modDateCursor = "2019-08-14T00:25:52.702+0000";
      while (modDateCursor != null) {

        instances = okapi.queryAsList(instanceEndPoint,
            String.format("metadata.updatedDate > \"%s\" sortBy metadata.updatedDate",modDateCursor),null);
        modDateCursor = processInstanceBatch(voyager, okapi, instances, localId, sysCtrlNum, skippedInstances);
      }
    }
    System.out.println(skippedInstances.size()+ " instances where no holdings were loaded.");
    System.out.println(String.join(", ", skippedInstances));
  }

  @SuppressWarnings("unchecked")
  private static String processInstanceBatch(
      Connection voyager, OkapiClient okapi, List<Map<String,Object>> instances, String localId, String sysCtrlNum, List<String> skippedInstances)
      throws IOException, NumberFormatException, SQLException, XMLStreamException {

    String lastModDate = null;
    for (Object instance : instances) {
      Map<String, Object> i = (HashMap<String, Object>) instance;

      Map<String, Object> metadata = (HashMap<String, Object>) i.get("metadata");
      lastModDate = (String) metadata.get("updatedDate");

      List<Object> identifiers = (ArrayList<Object>) i.get("identifiers");
      String title = ((String) i.get("title"));
      String bibId = null;
      for (Object identifier : identifiers) {
        Map<String, String> id = (HashMap<String, String>) identifier;

        // bib id location in Joe's instance stubs
        if (id.get("identifierTypeId").equals(localId))
          bibId = id.get("value");

        // bib id location in instance generated from SRS q3.1
        if (id.get("identifierTypeId").equals(sysCtrlNum))
          if ( integerP.matcher(id.get("value")).matches() )
            if ( isActuallyTheBibId( voyager, title, id.get("value")))
              bibId = id.get("value");
      }

      if (bibId == null) {
        System.out.println("bib id not identified for instance "+i.get("id"));
        skippedInstances.add((String)i.get("id"));
        continue;
      }

      List<Holding> holdings = Holdings.getHoldingsForBibRecord(
          voyager, okapi, Integer.valueOf(bibId), (String)i.get("id"));
//      System.out.println("id=" + i.get("id") + "; bibid=" + bibId + "; holdingCount=" + holdings.size());
      for (Holding h : holdings) {
//        System.out.println("\t" + h.toString());
//        String response = okapi.delete("/holdings-storage/holdings", h.getId());
//        System.out.println(response);
        String response = okapi.post("/holdings-storage/holdings", h.toString());
        System.out.println(response);
      }
//      System.exit(0);
    }
    return lastModDate;
  }

  private static boolean isActuallyTheBibId(Connection voyager, String title, String candidateBibId)
      throws SQLException {

    if ( bibTextQuery == null )
      bibTextQuery = voyager.prepareStatement("SELECT title FROM bib_text WHERE bib_id = ?");
    bibTextQuery.setInt(1, Integer.valueOf(candidateBibId));
    title = title.trim().replaceAll("\\\\", "").replaceAll("\\s+"," ");
    if (title.length() > 255) title = title.substring(0, 255);
    try ( ResultSet rs = bibTextQuery.executeQuery() ) {
      while ( rs.next() ) {
        String dbTitle = new String(rs.getBytes(1),StandardCharsets.UTF_8).trim().replaceAll("\\s+"," ");
        if ( dbTitle.equals(title) || dbTitle.equals(title+"."))
          return true;
        if ( title.length() - dbTitle.length() < 10 
            && ( title.contains(dbTitle.substring(1,dbTitle.length()-1))))
//                || (title+".").endsWith(dbTitle.substring(1))))
          return true;
        System.out.println("Title mismatch:"+title+" <> "+dbTitle+" ("+candidateBibId+")");
        System.out.println(dbTitle.length());
      }
    }
    return false;
  }
  static PreparedStatement bibTextQuery = null;
  static Pattern integerP = Pattern.compile("^\\d+$");



}
