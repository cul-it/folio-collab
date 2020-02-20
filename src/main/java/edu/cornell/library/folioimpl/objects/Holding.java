package edu.cornell.library.folioimpl.objects;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.folioimpl.tools.DownloadMarc;

public class Holding {

  public MarcRecord marc;
  public Map<String, Object> holding = new HashMap<>();
  public String locationCode = null;
  private static ObjectMapper mapper = new ObjectMapper();
  private static ReferenceData holdingNoteTypes = null;
  private static ReferenceData holdingTypes = null;
  private static ReferenceData locations = null;
  private static ReferenceData callNumberTypes = null;

  public Holding(Connection voyager, OkapiClient okapi, Integer recordId) throws SQLException, IOException {

    if (holdingNoteTypes == null)
      holdingNoteTypes = new ReferenceData(okapi, "/holdings-note-types", "name");
    if (holdingTypes == null)
      holdingTypes = new ReferenceData(okapi, "/holdings-types", "name");
    if (locations == null) {
      locations = new ReferenceData(okapi, "/locations", "code");
      locations.setDefault("void");
    }
    if (callNumberTypes == null)
      callNumberTypes = new ReferenceData(okapi, "/call-number-types", "name");
    this.marc = DownloadMarc.get(voyager, MarcRecord.RecordType.HOLDINGS, recordId);
    this.setVoyagerMfhdId(recordId);
    processMarcData();

  }

  public Holding setId(String holdingUuid) {
    this.holding.put("id", holdingUuid);
    return this;
  }

  public String getId() {
    return (String)this.holding.get("id");
  }

  public Holding setVoyagerMfhdId(Integer mfhdId) {
    this.holding.put("formerIds", Arrays.asList(mfhdId.toString()));
    return this;
  }

  public Holding setInstanceId(String instanceId) {
    this.holding.put("instanceId", instanceId);
    return this;
  }

  public Holding setSuppressed(Boolean suppress) {
    this.holding.put("discoverySuppress", suppress);
    return this;
  }

  @Override
  public String toString() {
    String s = null;
    try {
      s = mapper.writeValueAsString(this.holding);
    } catch (JsonProcessingException e) {}
    return s;
  }

  @SuppressWarnings("unchecked")
  private void processMarcData() {
    for (MarcRecord.DataField f : this.marc.dataFields)
      switch (f.tag) {
      case "852":
        List<String> callNumberParts = new ArrayList<>();
        for (MarcRecord.Subfield sf : f.subfields)
          switch (sf.code) {
          case 'b':
            this.holding.put("holdingsTypeId", (sf.value.contains("serv,remo"))
                ? holdingTypes.getUuid("Electronic")
                : holdingTypes.getUuid("Physical"));
            this.locationCode = sf.value;
            this.holding.put("permanentLocationId", locations.getUuid(sf.value.trim()));
            break;
          case 'k':
            this.holding.put("callNumberPrefix", sf.value);
            break;
          case 'h':
          case 'i':
            callNumberParts.add(sf.value);
            break;
          case 'm':
            this.holding.put("callNumberSuffix", sf.value);
            break;
          case 'l':
            this.holding.put("shelvingTitle", sf.value);
            break;
          case 'x':
          case 'z':
            addHoldingNote(holdingNoteTypes.getUuid("Note"), sf.value, sf.code.equals('x'));
            break;
          }
        if (!callNumberParts.isEmpty()) {
          this.holding.put("callNumber", String.join(" ", callNumberParts));
          switch (f.ind1) {
          case '0':
            this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Library of Congress classification"));
            break;
          case '1':
            this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Dewey Decimal classification"));
            break;
          case '2':
            this.holding.put("callNumberTypeId",
                callNumberTypes.getUuid("National Library of Medicine classification"));
            break;
          case '3':
            this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Superintendent of Documents classification"));
            break;
          case '4':
            this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Shelving control number"));
            break;
          case '5':
            this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Title"));
            break;
          case '7':
            for (MarcRecord.Subfield sf : f.subfields)
              if (sf.code.equals('7') && sf.value.equals("lcc"))
                this.holding.put("callNumberTypeId", callNumberTypes.getUuid("Library of Congress classification"));
            break;
          }
        }
        break;

      case "866":
      case "867":
      case "868":
        Map<String, String> holdingDescription = new HashMap<>();
        for (MarcRecord.Subfield sf : f.subfields)
          if (sf.code.equals('a'))
            holdingDescription.put("statement", sf.value);
          else if (sf.code.equals('z'))
            holdingDescription.put("note", sf.value);
        if (!holdingDescription.containsKey("statement")) {
          System.out.println("Holdings description field lacks holdings description " + f.toString());
          break;
        }
        String field = (f.tag.equals("866")) ? "holdingsStatements"
            : (f.tag.equals("867")) ? "holdingsStatementsForIndexes" : "holdingsStatementsForSupplements";
        List<Map<String, String>> stmtlist;

        if (this.holding.containsKey(field))
          stmtlist = (List<Map<String, String>>) this.holding.get(field);
        else
          stmtlist = new ArrayList<>();
        stmtlist.add(holdingDescription);
        this.holding.put(field, stmtlist);
        break;
      case "506":
        addHoldingNote(holdingNoteTypes.getUuid("Restriction"), f.concatSubfields("3abcdefgqu"), false);
        break;
      case "561":
        addHoldingNote(holdingNoteTypes.getUuid("Provenance"), f.concatSubfields("3au"), false);
        break;
      case "562":
        addHoldingNote(holdingNoteTypes.getUuid("Copy note"), f.concatSubfields("3abcde"), false);
        break;
      case "563":
        addHoldingNote(holdingNoteTypes.getUuid("Binding"), f.concatSubfields("3au"), false);
        break;
      case "583": // public and non-public note as only part of a note that must be public or
                  // nonpublic. :-P
        /* Examples with both m2522212 b2085444 m4773819 b4219530 m6731034 b5782601 */
        addHoldingNote(holdingNoteTypes.getUuid("Action note"), f.concatSubfields("3abcdefhijklnouxz"),
            false);
        break;
      case "843":
        addHoldingNote(holdingNoteTypes.getUuid("Reproduction"), f.concatSubfields("3abcdefmn"), false);
        break;
      default:
//        System.out.println(f.toString());
      }
  }

  @SuppressWarnings("unchecked")
  private void addHoldingNote(String typeUuid, String noteText, boolean staffOnly) {
    Map<String, Object> note = new HashMap<>();
    note.put("holdingsNoteTypeId", typeUuid);
    note.put("note", noteText);
    note.put("staffOnly", staffOnly);
    List<Map<String, Object>> notes;
    if (this.holding.containsKey("notes"))
      notes = (List<Map<String, Object>>) this.holding.get("notes");
    else
      notes = new ArrayList<>();
    notes.add(note);
    this.holding.put("notes", notes);
  }
}
