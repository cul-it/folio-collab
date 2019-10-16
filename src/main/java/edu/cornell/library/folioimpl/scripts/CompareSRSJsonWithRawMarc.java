package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import edu.cornell.library.folioimpl.objects.MarcRecord;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class CompareSRSJsonWithRawMarc {

  public static void main(String[] args) throws IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient(prop.getProperty("url32dmg"),prop.getProperty("token32dmg"));
    String srsEndPoint = "/source-storage/records";

    List<Map<String,Object>> records = okapi.queryAsList(srsEndPoint,"id==* sortBy id",20);
    while (records.size() > 0) {
      String idCursor = processRecords(records);
      records = okapi.queryAsList(srsEndPoint,String.format("id > \"%s\" sortBy id",idCursor),20);
    }
  }

  @SuppressWarnings("unchecked")
  private static String processRecords(List<Map<String, Object>> records) {
    String lastId = null;
    for (Map<String,Object> record : records) {
      lastId = (String)record.get("id");
      Map<String,Object> rawRecord = (Map<String, Object>) record.get("rawRecord");
      MarcRecord marcFromRaw = new MarcRecord( ((String)rawRecord.get("content")).getBytes(StandardCharsets.UTF_8) );
      Map<String,Object> parsedRecord = (Map<String, Object>) record.get("parsedRecord");
      MarcRecord marcFromJson = new MarcRecord( (Map<String, Object>) parsedRecord.get("content"));
      System.out.println(lastId);
      compareRecords( marcFromRaw, marcFromJson );
    }
    return lastId;
  }

  private static void compareRecords(MarcRecord a, MarcRecord b) {
    if ( ! Objects.equals(a.leader, b.leader) )
      System.out.printf("- %s\n+ %s\n",a.leader,b.leader);
//    if ( a.controlFields.size() != b.controlFields.size() )
//      System.out.printf("number of control fields changed %d -> %d\n",a.controlFields.size(),b.controlFields.size());
    Set<MarcRecord.ControlField> matchingControlFieldsA = new HashSet<>();
    Set<MarcRecord.ControlField> matchingControlFieldsB = new HashSet<>();
    FIELD: for ( MarcRecord.ControlField f : a.controlFields )
      for ( MarcRecord.ControlField g : b.controlFields ) 
        if ( f.tag.equals(g.tag) && f.value.equals(g.value) ) {
          matchingControlFieldsA.add(f);
          matchingControlFieldsB.add(g);
          continue FIELD;
        }
    for ( MarcRecord.ControlField f : matchingControlFieldsA ) a.controlFields.remove(f);
    for ( MarcRecord.ControlField g : matchingControlFieldsB ) b.controlFields.remove(g);
    for ( MarcRecord.ControlField f : a.controlFields ) System.out.printf("- %s %s\n", f.tag, f.value);
    for ( MarcRecord.ControlField g : b.controlFields ) System.out.printf("+ %s %s\n", g.tag, g.value);

//    if ( a.dataFields.size() != b.dataFields.size() )
//      System.out.printf("number of data fields changed %d -> %d\n",a.dataFields.size(),b.dataFields.size());
    Set<MarcRecord.DataField> matchingDataFieldsA = new HashSet<>();
    Set<MarcRecord.DataField> matchingDataFieldsB = new HashSet<>();
    FIELD: for ( MarcRecord.DataField f : a.dataFields )
      for ( MarcRecord.DataField g : b.dataFields ) 
        if ( f.toString().equals(g.toString()) ) {
          matchingDataFieldsA.add(f);
          matchingDataFieldsB.add(g);
          continue FIELD;
        } 
    for ( MarcRecord.DataField f : matchingDataFieldsA ) a.dataFields.remove(f);
    for ( MarcRecord.DataField g : matchingDataFieldsB ) b.dataFields.remove(g);
    for ( MarcRecord.DataField f : a.dataFields ) System.out.printf("- %s\n", f.toString());
    for ( MarcRecord.DataField g : b.dataFields ) System.out.printf("+ %s\n", g.toString());
    System.out.print("\n");

  }
}
