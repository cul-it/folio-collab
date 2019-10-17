package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import edu.cornell.library.folioimpl.objects.MarcRecord;
import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.tools.DownloadMarc;

public class CompareSRSJsonWithRawMarcAndOriginalMarc {

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient(prop.getProperty("url32dmg"),prop.getProperty("token32dmg"));
    String srsEndPoint = "/source-storage/records";

    List<Map<String,Object>> records = okapi.queryAsList(srsEndPoint,"id==* sortBy id",20);
    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass")) ) {
      while (records.size() > 0) {
        String idCursor = processRecords(records, voyager);
        records = okapi.queryAsList(srsEndPoint,String.format("id > \"%s\" sortBy id",idCursor),20);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static String processRecords(List<Map<String, Object>> records, Connection voyager)
      throws SQLException, IOException {
    String lastId = null;
    RECORD: for (Map<String,Object> record : records) {
      lastId = (String)record.get("id");
      Map<String,Object> rawRecord = (Map<String, Object>) record.get("rawRecord");
      MarcRecord marcFromRaw = new MarcRecord( ((String)rawRecord.get("content")).getBytes(StandardCharsets.UTF_8) );
      Integer bibId = null;
      for ( MarcRecord.ControlField f : marcFromRaw.controlFields )
        if ( f.tag.equals("001") ) {
          try {
            bibId = Integer.valueOf(f.value);
          } catch ( NumberFormatException e ) {
            System.out.println("No bib found for "+f.value+".\n");
            continue RECORD;
          }
          break;
        }
      if ( bibId != null ) {
        MarcRecord marcFromVoyager = DownloadMarc.get(voyager, MarcRecord.RecordType.BIBLIOGRAPHIC, bibId);
        if ( marcFromVoyager == null ) {
          System.out.println("No bib found for "+bibId+".\n");
          continue RECORD;
        }
        String importDifferences = marcFromVoyager.differences(marcFromRaw);
        if ( ! importDifferences.isEmpty() ) {
          System.out.printf("%d => %s\n",bibId,lastId);
          System.out.println(importDifferences);
        }
        marcFromRaw = new MarcRecord( ((String)rawRecord.get("content")).getBytes(StandardCharsets.UTF_8) );
      }
      Map<String,Object> parsedRecord = (Map<String, Object>) record.get("parsedRecord");
      MarcRecord marcFromJson = new MarcRecord( (Map<String, Object>) parsedRecord.get("content"));
      String differences = marcFromRaw.differences(marcFromJson);
      if ( ! differences.isEmpty() ) {
        System.out.println(lastId);
        System.out.println(differences);
      }
    }
    return lastId;
  }


}
