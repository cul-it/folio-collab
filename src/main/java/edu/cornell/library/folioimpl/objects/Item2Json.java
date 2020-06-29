package edu.cornell.library.folioimpl.objects;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Item2Json {

  public Map<Integer, String> itemTypeHash;

  // initialized by 'hand-coding' from design documents
  // preliminary to uuid lookup
  public static Map<String, String> loanTypeHash = new HashMap<>();
  static {
    loanTypeHash.put("1dayloan", "Circulating");
    loanTypeHash.put("1dayres", "Reserves");
    loanTypeHash.put("1hrloan", "Reserves");
    loanTypeHash.put("1hrres", "Reserves");
    loanTypeHash.put("1wkloan", "Circulating");
    loanTypeHash.put("1wkres", "Reserves");
    loanTypeHash.put("2dayloan", "Reserves");
    loanTypeHash.put("2dayres", "Reserves");
    loanTypeHash.put("2hrloan", "Reserves");
    loanTypeHash.put("2hrres", "Reserves");
    loanTypeHash.put("2wkloan", "Circulating");
    loanTypeHash.put("3dayloan", "Circulating");
    loanTypeHash.put("3dayres", "Reserves");
    loanTypeHash.put("3hrloan", "Circulating");
    loanTypeHash.put("3hrres", "Circulating");
    loanTypeHash.put("4hrloan", "Circulating");
    loanTypeHash.put("4hrres", "Reserves");
    loanTypeHash.put("8hrres", "Circulating");
    loanTypeHash.put("archivmanu", "Non-circulating");
    loanTypeHash.put("book", "Circulating");
    loanTypeHash.put("computfile", "Circulating");
    loanTypeHash.put("equipment", "Circulating");
    loanTypeHash.put("keys", "Circulating");
    loanTypeHash.put("laptop", "Circulating");
    loanTypeHash.put("map", "Non-circulating");
    loanTypeHash.put("maps", "Non-circulating");
    loanTypeHash.put("microform", "Circulating");
    loanTypeHash.put("music", "Circulating");
    loanTypeHash.put("newbook", "Circulating");
    loanTypeHash.put("newspaper", "Circulating");
    loanTypeHash.put("nocirc", "Non-circulating");
    loanTypeHash.put("periodical", "Circulating");
    loanTypeHash.put("permres", "Reserves");
    loanTypeHash.put("serial", "Circulating");
    loanTypeHash.put("soundrec", "Circulating");
    loanTypeHash.put("specloan", "Circulating");
    loanTypeHash.put("umbrella", "Circulating");
    loanTypeHash.put("unbound", "Circulating");
    loanTypeHash.put("visual", "Circulating");
  }

  public enum FolioStatus {
    AVAIL("Available"),      PICKUP("Awaiting pickup"),   CHECKEDOUT("Checked out"),
    PROCESS("In process"),   TRANSIT("In transit"),       MISSING("Missing"),
    ORDER("On order"),       PAGED("Paged"),              WITHDRAWN("Withdrawn"),
    LOST("Declared lost"),   CLAIMED("Claimed returned");
    final private String value;
    private FolioStatus(String value) { this.value = value; }
    @Override public String toString() { return this.value; }
  }

  public enum VoyagerStatus {
    NIL ("not used do not remove 0-th entry",null),
    NC  ("Not Charged",           FolioStatus.AVAIL),
/*  C   ("Charged",               FolioStatus.CHECKEDOUT),
    R   ("Renewed",               FolioStatus.CHECKEDOUT),
    O   ("Overdue",               FolioStatus.CHECKEDOUT),
    RR  ("Recall Request",        FolioStatus.CHECKEDOUT),*/
    C   ("Charged",               FolioStatus.AVAIL),
    R   ("Renewed",               FolioStatus.AVAIL),
    O   ("Overdue",               FolioStatus.AVAIL),
    RR  ("Recall Request",        FolioStatus.AVAIL),
    HR  ("Hold Request",          FolioStatus.PAGED),
    OH  ("On Hold",               FolioStatus.PICKUP),
    IT  ("In Transit",            FolioStatus.TRANSIT),
    ITD ("In Transit Discharged", FolioStatus.TRANSIT),
    ITOH("In Transit On Hold",    FolioStatus.TRANSIT),
    DC  ("Discharged",            FolioStatus.AVAIL),
    M   ("Missing",               FolioStatus.MISSING),
    LLA ("Lost--Library Applied", FolioStatus.LOST),
    LSA ("Lost--System Applied",  FolioStatus.LOST),
    CRET("Claims Returned",       FolioStatus.CLAIMED),
    D   ("Damaged",               FolioStatus.AVAIL),
    W   ("Withdrawn",             FolioStatus.WITHDRAWN),
/*  AB  ("At Bindery",            FolioStatus.CHECKEDOUT),
    CATR("Cataloging Review",     FolioStatus.CHECKEDOUT),
    CRCR("Circulation Review",    FolioStatus.CHECKEDOUT),*/
    AB  ("At Bindery",            FolioStatus.AVAIL),
    CATR("Cataloging Review",     FolioStatus.AVAIL),
    CRCR("Circulation Review",    FolioStatus.AVAIL),
    S   ("Scheduled",             FolioStatus.PAGED),
    IP  ("In Process",            FolioStatus.PROCESS),
    CSR ("Call Slip Request",     FolioStatus.PAGED),
    SLR ("Short Loan Request",    FolioStatus.PAGED),
    RSR ("Remote Storage Request",FolioStatus.PAGED);
    final private String value;
    final private FolioStatus folioStatus;
    private VoyagerStatus(String value, FolioStatus folioStatus) {
      this.value = value; this.folioStatus = folioStatus;
    }
    @Override public String toString() {
      if (this.folioStatus != null) return this.folioStatus.toString();
      return this.value;
    }
  }
  VoyagerStatus[] voyagerStatuses = VoyagerStatus.values();

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_EMPTY);
  }


  final ReferenceData itemNoteTypes;
  final ReferenceData materialTypes;
  final ReferenceData loanTypes;
  final ReferenceData locations;
  final ReferenceData itemDamagedStatuses;
  public Item2Json ( Connection voyager, OkapiClient okapi ) throws IOException, SQLException {

    this.itemNoteTypes = new ReferenceData(okapi, "/item-note-types", "name");
    this.materialTypes = new ReferenceData(okapi, "/material-types", "name");
    this.loanTypes = new ReferenceData(okapi, "/loan-types", "name");
    this.locations = new ReferenceData(okapi, "/locations", "code");
    this.itemDamagedStatuses = new ReferenceData(okapi, "/item-damaged-statuses","name");
    this.itemTypeHash = getItemTypes( voyager );

  }

  public List<Item> getItemsForMfhdId(Integer mfhd, Connection voyager) throws SQLException {

    String query = "";
    query = "select distinct ";
    query += "   item.item_id, jrm424.item_id_uuid(item.item_id) uuid, ";
    query += "   jrm424.matype(item.item_id) mt, jrm424.mfhd_uuid(mfhd_item.mfhd_id) mfhd_uuid, ";
    query += "   jrm424.bar_code(item.item_id) item_barcode, mfhd_item.item_enum, ";
    query += "   mfhd_item.chron, item.copy_number, ";
    query += "   item.pieces, jrm424.mrlo(item_status.item_id) comp_status, ";
    query += "   item.item_type_id, item.temp_item_type_id, ";
    query += "   jrm424.location_code(item.perm_location) perm_location, ";
    query += "   jrm424.location_code(item.temp_location) temp_location, ";
    query += "   item_note.item_note, damaged_status.item_status_date damaged_date ";
    query += "from";
    query += "    item, mfhd_item, item_status,";
    query += "    item_note, item_barcode, item_status damaged_status ";
    query += "where";
    query += " item.item_id = mfhd_item.item_id (+) and ";
    query += " item.item_id = item_status.item_id (+) and ";
    query += " item.item_id = item_note.item_id (+) and ";
    query += " item.item_id = item_barcode.item_id  (+) and ";
    query += " item.item_id = damaged_status.item_id (+) and ";
    query += " damaged_status.item_status (+) = 16 and ";
    query += " mfhd_item.mfhd_id = " + mfhd + " order by item.item_id ";

    System.out.println(query);
    try ( Statement stmt = voyager.createStatement();
        ResultSet result = stmt.executeQuery(query)) {
      return processResultSet(result );
      
    }
  }


  public Item getItemById(Integer itemId, Connection voyager) throws SQLException {

    String query = "";
    query = "select distinct ";
    query += "   item.item_id, jrm424.item_id_uuid(item.item_id) uuid, ";
    query += "   jrm424.matype(item.item_id) mt, mfhd_item.mfhd_id, ";
    query += "   jrm424.bar_code(item.item_id) item_barcode, mfhd_item.item_enum, ";
    query += "   mfhd_item.chron, item.copy_number, ";
    query += "   item.pieces, jrm424.mrlo(item_status.item_id) comp_status, ";
    query += "   item.item_type_id, item.temp_item_type_id, ";
    query += "   jrm424.location_code(item.perm_location) perm_location, ";
    query += "   jrm424.location_code(item.temp_location) temp_location, ";
    query += "   item_note.item_note, damaged_status.item_status_date damaged_date ";
    query += "from";
    query += "    item, mfhd_item, item_status,";
    query += "    item_note, item_barcode, item_status damaged_status ";
    query += "where";
    query += " item.item_id = "+ itemId + " and ";
    query += " item.item_id = mfhd_item.item_id (+) and ";
    query += " item.item_id = item_status.item_id (+) and ";
    query += " item.item_id = item_note.item_id (+) and ";
    query += " item.item_id = item_barcode.item_id  (+) and ";
    query += " item.item_id = damaged_status.item_id (+) and ";
    query += " damaged_status.item_status (+) = 16";

    try ( Statement stmt = voyager.createStatement();
        ResultSet result = stmt.executeQuery(query)) {
      List<Item> items = processResultSet(result );
      if ( items.isEmpty() )
        return null;
      return items.get(0);
      
    }
  }

  public List<Item> processResultSet(ResultSet results ) throws SQLException {
    List<Item> items = new ArrayList<>();

    while (results.next()) {

      Item i = new Item();

      i.hrid = String.valueOf(results.getInt("item_id"));
      i.id = results.getString("uuid");
      String mt = results.getString("mt");
      if ( mt == null || mt.equals("unknown") ) {
        i.materialTypeId = this.materialTypes.getUuid("unspecified");
      } else
        i.materialTypeId = this.materialTypes.getUuid(mt);
      try {
        results.findColumn("mfhd_uuid");
        i.holdingsRecordId = results.getString("mfhd_uuid");
      } catch (SQLException e ) {
        i.mfhdId = results.getString("mfhd_id");
      }
      i.barcode = results.getString("item_barcode");
      i.enumeration = results.getString("item_enum");
      i.chronology = results.getString("chron");
      i.copyNumber = results.getString("copy_number");
      i.numberOfPieces = results.getInt("pieces");

      String comp_status = results.getString("comp_status");
      if (comp_status != null) {
        String[] statusParts = comp_status.split("\\|");
        i.status.put("name", this.voyagerStatuses[Integer.parseInt(statusParts[1])].toString() );
        if (!"<null>".equals(statusParts[2]))
          i.status.put("date", statusParts[2]);
      }

      Timestamp damagedDate = results.getTimestamp("damaged_date");
      if ( damagedDate != null ) {
        i.itemDamagedStatusId = this.itemDamagedStatuses.getUuid("Damaged");
        i.itemDamagedStatusDate =
            damagedDate.toInstant().atZone(ZoneId.of("America/New_York")).toString().substring(0, 19);
      }

      String permType = this.itemTypeHash.get(results.getInt("item_type_id"));
      if (permType != null && loanTypeHash.containsKey(permType))
        i.permanentLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(permType));

      String tempType = this.itemTypeHash.get(results.getInt("temp_item_type_id"));
      if (tempType != null && loanTypeHash.containsKey(tempType))
        i.temporaryLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(tempType));

      String permLoc = this.locations.getUuid(results.getString("perm_location"));
      if (permLoc == null)
        permLoc = this.locations.getUuid("void");
      i.permanentLocationId = permLoc;

      String tempLocCode = results.getString("temp_location");
      if (tempLocCode != null) {
        String tempLoc = this.locations.getUuid(tempLocCode);
        if (tempLoc == null)
          tempLoc = this.locations.getUuid("void");
        i.temporaryLocationId = tempLoc;
      }

      String note = results.getString("item_note");
      if (note != null) {
        Map<String, String> noteMap = new HashMap<>();
        // "8d0a5eca-25de-4391-81a9-236eeefdd20b"
        noteMap.put("itemNoteTypeId", this.itemNoteTypes.getUuid("Note"));
        noteMap.put("note", note);
        noteMap.put("staffOnly", "false");
        i.notes.add(noteMap);
      }

      items.add(i);
    }

    return items;
  }

  public class Item {
    public String id = null;
    public String hrid = null;
    public List<String> formerIds = new ArrayList<>();
    public String materialTypeId = null;
    public String mfhdId = null;
    public String holdingsRecordId = null;
    public String barcode = null;
    public String enumeration = null;
    public String chronology = null;
    public String copyNumber = null;
    public Integer numberOfPieces = null;
    public Map<String,String> status = new HashMap<>();
    public String permanentLoanTypeId = null;
    public String temporaryLoanTypeId = null;
    public String permanentLocationId = null;
    public String temporaryLocationId = null;
    public String itemDamagedStatusId = null;
    public String itemDamagedStatusDate = null;
    public List<Map<String,String>> notes = new ArrayList<>();
    public String getId() { return this.id; }

    @Override
    public String toString() {
      String s = null;
      try {
        s = mapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {}
      return s;
    }

  }

  //get and make hash from item_type tbl
  /**
   * 
   * @param con
   * @throws SQLException
   */
  public Map<Integer, String> getItemTypes(Connection con) throws SQLException {
    Map<Integer, String> res = new HashMap<>();
    try ( Statement stmt = con.createStatement() ) {
      String q = "SELECT ITEM_TYPE_ID, ITEM_TYPE_NAME FROM ITEM_TYPE ORDER BY ITEM_TYPE_ID";
      try ( ResultSet rs = stmt.executeQuery(q) ) {
        while (rs.next()) {
          Integer id = rs.getInt("ITEM_TYPE_ID");
          String idn = rs.getString("ITEM_TYPE_NAME");
          res.put(id, idn);
        }
      }
    }
    return res;
  }

}
