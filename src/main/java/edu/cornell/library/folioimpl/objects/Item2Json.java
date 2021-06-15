package edu.cornell.library.folioimpl.objects;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Item2Json {

  public Map<Integer, String> itemTypeHash;

  // initialized by 'hand-coding' from design documents
  // preliminary to uuid lookup
  public static Map<String, String> loanTypeHash = new HashMap<>();
  static {
    loanTypeHash.put("1dayloan", "1 day loan");
    loanTypeHash.put("1dayres", "1 day loan");
    loanTypeHash.put("1hrloan", "2 hour loan");
    loanTypeHash.put("1hrres", "2 hour loan");
    loanTypeHash.put("1wkloan", "7 day loan");
    loanTypeHash.put("1wkres", "7 day loan");
    loanTypeHash.put("2dayloan", "2 day loan");
    loanTypeHash.put("2dayres", "2 day loan");
    loanTypeHash.put("2hrloan", "2 hour loan");
    loanTypeHash.put("2hrres", "2 hour loan");
    loanTypeHash.put("2wkloan", "14 day loan");
    loanTypeHash.put("3dayloan", "3 day loan");
    loanTypeHash.put("3dayres", "3 day loan");
    loanTypeHash.put("3hrloan", "3 hour loan");
    loanTypeHash.put("3hrres", "3 hour loan");
    loanTypeHash.put("4hrloan", "4 hour loan");
    loanTypeHash.put("4hrres", "4 hour loan");
    loanTypeHash.put("8hrres", "8 hour loan");
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
    loanTypeHash.put("newbook", "Non-circulating");
    loanTypeHash.put("newspaper", "Circulating");
    loanTypeHash.put("nocirc", "Non-circulating");
    loanTypeHash.put("periodical", "Circulating");
    loanTypeHash.put("permres", "Non-circulating");
    loanTypeHash.put("serial", "Circulating");
    loanTypeHash.put("soundrec", "Circulating");
    loanTypeHash.put("specloan", "Circulating");
    loanTypeHash.put("umbrella", "Circulating");
    loanTypeHash.put("unbound", "Circulating");
    loanTypeHash.put("visual", "Circulating");
  }

  public static Map<Integer, MaterialType> vItemType2fMaterialType = new HashMap<>();
  static {
    vItemType2fMaterialType.put(  8, MaterialType.ARCHIVE);
    vItemType2fMaterialType.put(  3, MaterialType.BOOK);
    vItemType2fMaterialType.put(  4, MaterialType.FILE);
    vItemType2fMaterialType.put( 35, MaterialType.EQUIP);
    vItemType2fMaterialType.put( 37, MaterialType.EQUIP); //MaterialType.KEYS);
    vItemType2fMaterialType.put( 36, MaterialType.EQUIP); //MaterialType.LAPTOP);
    vItemType2fMaterialType.put(  6, MaterialType.MAP);
    vItemType2fMaterialType.put( 21, MaterialType.MAP);
    vItemType2fMaterialType.put( 19, MaterialType.MICRO);
    vItemType2fMaterialType.put(  5, MaterialType.SCORE);//Voyager: music
    vItemType2fMaterialType.put( 25, MaterialType.BOOK);
    vItemType2fMaterialType.put( 20, MaterialType.NEWS);
    vItemType2fMaterialType.put( 15, MaterialType.PERIODICAL);
    vItemType2fMaterialType.put(  2, MaterialType.SERIAL);
    vItemType2fMaterialType.put( 18, MaterialType.SOUND);//Voyager: soundrec
    vItemType2fMaterialType.put( 40, MaterialType.EQUIP); //MaterialType.UMBRELLA);
    vItemType2fMaterialType.put( 39, MaterialType.UNBOUND);
    vItemType2fMaterialType.put(  7, MaterialType.VISUAL);
}

  public static Map<String, MaterialType> bibFormat2fMaterialType = new HashMap<>();
  static {
    bibFormat2fMaterialType.put("aa", MaterialType.BOOK);
    bibFormat2fMaterialType.put("ab", MaterialType.BOOK);
    bibFormat2fMaterialType.put("ac", MaterialType.BOOK);
    bibFormat2fMaterialType.put("ad", MaterialType.BOOK);
    bibFormat2fMaterialType.put("am", MaterialType.BOOK);
    bibFormat2fMaterialType.put("as", MaterialType.SERIAL);
    bibFormat2fMaterialType.put("ai", MaterialType.TEXT);
    bibFormat2fMaterialType.put("ca", MaterialType.SCORE);
    bibFormat2fMaterialType.put("cb", MaterialType.SCORE);
    bibFormat2fMaterialType.put("cc", MaterialType.SCORE);
    bibFormat2fMaterialType.put("cd", MaterialType.SCORE);
    bibFormat2fMaterialType.put("cm", MaterialType.SCORE);
    bibFormat2fMaterialType.put("cs", MaterialType.SCORE);
    bibFormat2fMaterialType.put("da", MaterialType.SCORE);
    bibFormat2fMaterialType.put("db", MaterialType.SCORE);
    bibFormat2fMaterialType.put("dc", MaterialType.SCORE);
    bibFormat2fMaterialType.put("dd", MaterialType.SCORE);
    bibFormat2fMaterialType.put("dm", MaterialType.SCORE);
    bibFormat2fMaterialType.put("ds", MaterialType.SCORE);
    bibFormat2fMaterialType.put("ea", MaterialType.MAP);
    bibFormat2fMaterialType.put("eb", MaterialType.MAP);
    bibFormat2fMaterialType.put("ec", MaterialType.MAP);
    bibFormat2fMaterialType.put("ed", MaterialType.MAP);
    bibFormat2fMaterialType.put("em", MaterialType.MAP);
    bibFormat2fMaterialType.put("es", MaterialType.MAP);
    bibFormat2fMaterialType.put("fa", MaterialType.MAP);
    bibFormat2fMaterialType.put("fb", MaterialType.MAP);
    bibFormat2fMaterialType.put("fc", MaterialType.MAP);
    bibFormat2fMaterialType.put("fd", MaterialType.MAP);
    bibFormat2fMaterialType.put("fm", MaterialType.MAP);
    bibFormat2fMaterialType.put("fs", MaterialType.MAP);
    bibFormat2fMaterialType.put("ga", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("gb", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("gc", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("gd", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("gm", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("gs", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("ia", MaterialType.SOUND);
    bibFormat2fMaterialType.put("ib", MaterialType.SOUND);
    bibFormat2fMaterialType.put("ic", MaterialType.SOUND);
    bibFormat2fMaterialType.put("id", MaterialType.SOUND);
    bibFormat2fMaterialType.put("im", MaterialType.SOUND);
    bibFormat2fMaterialType.put("is", MaterialType.SOUND);
    bibFormat2fMaterialType.put("ja", MaterialType.SOUND);
    bibFormat2fMaterialType.put("jb", MaterialType.SOUND);
    bibFormat2fMaterialType.put("jc", MaterialType.SOUND);
    bibFormat2fMaterialType.put("jd", MaterialType.SOUND);
    bibFormat2fMaterialType.put("jm", MaterialType.SOUND);
    bibFormat2fMaterialType.put("js", MaterialType.SOUND);
    bibFormat2fMaterialType.put("ka", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("kb", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("kc", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("kd", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("km", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("ks", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("ma", MaterialType.FILE);
    bibFormat2fMaterialType.put("mb", MaterialType.FILE);
    bibFormat2fMaterialType.put("mc", MaterialType.FILE);
    bibFormat2fMaterialType.put("md", MaterialType.FILE);
    bibFormat2fMaterialType.put("mm", MaterialType.FILE);
    bibFormat2fMaterialType.put("ms", MaterialType.FILE);
    bibFormat2fMaterialType.put("oa", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("ob", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("oc", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("od", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("om", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("os", MaterialType.VISUAL);
    bibFormat2fMaterialType.put("pa", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("pb", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("pc", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("pd", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("pm", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("ps", MaterialType.ARCHIVE);
    bibFormat2fMaterialType.put("ra", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("rb", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("rc", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("rd", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("rm", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("rs", MaterialType.OBJECT);
    bibFormat2fMaterialType.put("ta", MaterialType.BOOK);
    bibFormat2fMaterialType.put("tb", MaterialType.BOOK);
    bibFormat2fMaterialType.put("tc", MaterialType.BOOK);
    bibFormat2fMaterialType.put("td", MaterialType.BOOK);
    bibFormat2fMaterialType.put("tm", MaterialType.BOOK);
    bibFormat2fMaterialType.put("ts", MaterialType.SERIAL);
  }


  private enum MaterialType {
    ARCHIVE("Archivman"),
    BOOK("Book"),
    FILE("Computfile"),
    EQUIP("Equipment"), // needed but unused? This can be cleaned up
    MAP("Map"),
    MICRO("Microform"),
    NEWS("Newspaper"),
    OBJECT("Object"),
    PERIODICAL("Periodical"),
    SCORE("Music (score)"),
    SERIAL("Serial"),
    SOUND("Soundrec"),
    TEXT("Textual resource"),
    UNBOUND("Unbound"),
    VISUAL("Visual"),

    XXX("Unspecified");

    final private String value;
    private MaterialType(String value) { this.value = value; }
    @Override public String toString() { return this.value; }
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
    AB  ("At Bindery",            FolioStatus.AVAIL),
    CATR("Cataloging Review",     FolioStatus.AVAIL),
    CRCR("Circulation Review",    FolioStatus.AVAIL),
    S   ("Scheduled",             FolioStatus.PAGED),
    IP  ("In Process",            FolioStatus.PROCESS),
    CSR ("Call Slip Request",     FolioStatus.PAGED),
    SLR ("Short Loan Request",    FolioStatus.PAGED),
    RSR ("Remote Storage Request",FolioStatus.PAGED);
    final String value;
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
  final VoyagerLocations voyLocations;
  final ReferenceData itemDamagedStatuses;
  final Map<Integer,Map<VoyagerStatus,Timestamp>> circulationNotes;
  final Map<Integer,Map<FolioStatus,Timestamp>> mappedUnavailStatuses;
  final Set<Integer> undatedAvailStatuses;
  public Item2Json ( Connection voyager, OkapiClient okapi ) throws IOException, SQLException {

    this.itemNoteTypes = new ReferenceData(okapi, "/item-note-types", "name");
    this.materialTypes = new ReferenceData(okapi, "/material-types", "name");
    this.loanTypes = new ReferenceData(okapi, "/loan-types", "name");
    this.locations = new ReferenceData(okapi, "/locations", "code");
    this.itemDamagedStatuses = new ReferenceData(okapi, "/item-damaged-statuses","name");
    this.voyLocations = new VoyagerLocations( voyager );
    this.itemTypeHash = getItemTypes( voyager );

    validateMaterialTypes();

    try ( Statement stmt = voyager.createStatement()) {
      stmt.setFetchSize(100_000);
      try ( ResultSet rs = stmt.executeQuery("SELECT * FROM item_status WHERE item_status IN (19,20)") ) {
        this.circulationNotes = new HashMap<>();
        while ( rs.next() ) {
          int itemId = rs.getInt("item_id");
          VoyagerStatus status = (rs.getInt("item_status")==19)?VoyagerStatus.CATR:VoyagerStatus.CRCR;
          if ( ! this.circulationNotes.containsKey(itemId)) this.circulationNotes.put(itemId, new HashMap<>());
          this.circulationNotes.get(itemId).put(status, rs.getTimestamp("item_status_date"));
        }}
      try ( ResultSet rs = stmt.executeQuery("SELECT * FROM item_status WHERE item_status IN (12,15,17)") ) {
        this.mappedUnavailStatuses = new HashMap<>();
        while ( rs.next() ) {
          int itemId = rs.getInt("item_id");
          FolioStatus status =
              (rs.getInt("item_status")==12)?FolioStatus.MISSING:
                (rs.getInt("item_status")==15)?FolioStatus.CLAIMED:FolioStatus.WITHDRAWN;
          if ( ! this.mappedUnavailStatuses.containsKey(itemId)) this.mappedUnavailStatuses.put(itemId, new HashMap<>());
          this.mappedUnavailStatuses.get(itemId).put(status, rs.getTimestamp("item_status_date"));
        }}
      try ( ResultSet rs = stmt.executeQuery(
          "SELECT item_id FROM item_status WHERE item_status = 1 and item_status_date is null") ) {
        this.undatedAvailStatuses = new HashSet<>();
        while ( rs.next() ) this.undatedAvailStatuses.add( rs.getInt("item_id") );
      }
    }

  }


  private void validateMaterialTypes() {
    for (MaterialType mt : MaterialType.values()) {
      String uuid = this.materialTypes.getUuid(mt.toString());
      if ( uuid == null || uuid.isEmpty() ) {
        System.out.printf("Material type '%s' not present in Folio.\n",mt.toString());
        System.exit(1);
      }
    }
  }


  public List<Item> getItemsForMfhdId(Integer mfhdId, Connection voyager) throws SQLException {

    String query = "";
    query = "select distinct ";
    query += "   item.item_id, item.item_type_id, bib_text.bib_format, mfhd_item.mfhd_id, ";
    query += "   item_barcode, mfhd_item.item_enum, ";
    query += "   mfhd_item.chron, item.copy_number, ";
    query += "   item.pieces, avail_status.item_status_date avail_date, ";
    query += "   item.item_type_id, item.temp_item_type_id, item.create_operator_id, ";
    query += "   perm_location, temp_location, ";
    query += "   item_note.item_note, damaged_status.item_status_date damaged_date ";
    query += "from";
    query += "    item, bib_item, bib_text, mfhd_item, item_status avail_status,";
    query += "    item_note, item_barcode, item_status damaged_status ";
    query += "where";
    query += " mfhd_item.mfhd_id = "+ mfhdId + " and ";
    query += " item.item_id = bib_item.item_id and ";
    query += " bib_text.bib_id = bib_item.bib_id and ";
    query += " item.item_id = mfhd_item.item_id (+) and ";
    query += " item.item_id = avail_status.item_id (+) and ";
    query += " avail_status.item_status (+) in ( 1, 11 ) and ";
    query += " item.item_id = item_note.item_id (+) and ";
    query += " item.item_id = item_barcode.item_id  (+) and ";
    query += " item_barcode.barcode_status (+) = 1 and ";
    query += " item.item_id = damaged_status.item_id (+) and ";
    query += " damaged_status.item_status (+) = 16";

    try ( Statement stmt = voyager.createStatement();
        ResultSet result = stmt.executeQuery(query)) {
      List<Item> items = processResultSet(result );
      if ( items.isEmpty() )
        return null;
      return items;
      
    }
  }

  public PreparedStatement prepareItemByIdQuery ( Connection voyager ) throws SQLException {

      String query = "";
      query = "select distinct ";
      query += "   item.item_id, item.item_type_id, bib_text.bib_format, mfhd_item.mfhd_id, ";
      query += "   item_barcode, mfhd_item.item_enum, ";
      query += "   mfhd_item.chron, item.copy_number, ";
      query += "   mfhd_item.freetext, item.spine_label, mfhd_item.caption, ";
      query += "   item.pieces, avail_status.item_status_date avail_date, ";
      query += "   item.item_type_id, item.temp_item_type_id, item.create_operator_id, ";
      query += "   perm_location, temp_location, ";
      query += "   item_note.item_note, damaged_status.item_status_date damaged_date ";
      query += "from";
      query += "    item, bib_item, bib_text, mfhd_item, item_status avail_status,";
      query += "    item_note, item_barcode, item_status damaged_status ";
      query += "where";
      query += " item.item_id = ? and ";
      query += " item.item_id = bib_item.item_id and ";
      query += " bib_text.bib_id = bib_item.bib_id and ";
      query += " item.item_id = mfhd_item.item_id (+) and ";
      query += " item.item_id = avail_status.item_id (+) and ";
      query += " avail_status.item_status (+) in ( 1, 11 ) and ";
      query += " item.item_id = item_note.item_id (+) and ";
      query += " item.item_id = item_barcode.item_id  (+) and ";
      query += " item_barcode.barcode_status (+) = 1 and ";
      query += " item.item_id = damaged_status.item_id (+) and ";
      query += " damaged_status.item_status (+) = 16";
      return voyager.prepareStatement(query);
  }

  public Item getItemById(Integer itemId, PreparedStatement itemByIdQuery) throws SQLException {

    itemByIdQuery.setInt(1, itemId);
    try (ResultSet result = itemByIdQuery.executeQuery()) {
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
      int itemId = Integer.valueOf(i.hrid);
      int itemTypeId = results.getInt("item_type_id");
      MaterialType mType = null;
      if ( vItemType2fMaterialType.containsKey(itemTypeId) ) {
        mType = vItemType2fMaterialType.get(itemTypeId);
      } else {
        String bibFormat = results.getString("bib_format");
        if ( bibFormat != null && bibFormat2fMaterialType.containsKey(bibFormat) ) {
          mType = bibFormat2fMaterialType.get(bibFormat);
        } else {
          mType = MaterialType.XXX;
        }
      }

      // Skip laptops, keys, umbrellas & other equipment entirely
      if (mType.equals(MaterialType.EQUIP)) continue;

      i.materialTypeId = this.materialTypes.getUuid(mType.toString());
      try {
        results.findColumn("mfhd_uuid");
        i.holdingsRecordId = results.getString("mfhd_uuid");
      } catch (SQLException e ) {
        i.mfhdId = results.getString("mfhd_id");
      }
      i.barcode = results.getString("item_barcode");

      i.copyNumber = results.getString("copy_number");
      i.numberOfPieces = results.getInt("pieces");

      if ( this.mappedUnavailStatuses.containsKey(itemId) ) {
        // Look for three unavailable statuses in their Voyager priority order
        for ( FolioStatus s : Arrays.asList(FolioStatus.MISSING, FolioStatus.WITHDRAWN, FolioStatus.CLAIMED))
          if ( this.mappedUnavailStatuses.get(itemId).containsKey(s) ) {
            i.status.put("name", s.toString());
            i.status.put("date", this.mappedUnavailStatuses.get(itemId).get(s).toInstant().toString());
            break;
          }
      } else {
        i.status.put("name", "Available");
        if ( results.getString("avail_date") != null )
          i.status.put("date", results.getTimestamp("avail_date").toInstant().toString());
        else if ( ! this.undatedAvailStatuses.contains(itemId) ) 
          i.status.put("date", "1978-04-06T03:21:00Z");
      }

      Timestamp damagedDate = results.getTimestamp("damaged_date");
      if ( damagedDate != null ) {
        i.itemDamagedStatusId = this.itemDamagedStatuses.getUuid("Damaged");
        i.itemDamagedStatusDate = damagedDate.toInstant().toString();
      }

      String createOperator = results.getString("create_operator_id");
      if (createOperator != null && createOperator.equals("bd02"))
        i.permanentLoanTypeId = this.loanTypes.getUuid("BD LOAN"); 
      else
      {

        String permType = this.itemTypeHash.get(results.getInt("item_type_id"));
        if (permType != null && loanTypeHash.containsKey(permType))
          i.permanentLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(permType));

      }

      String tempType = this.itemTypeHash.get(results.getInt("temp_item_type_id"));
      if (tempType != null && loanTypeHash.containsKey(tempType))
        i.temporaryLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(tempType));

      boolean isRMC = false;
      VoyagerLocations.Location vLoc = this.voyLocations.getByNumber( results.getInt("perm_location") );
      if ( vLoc != null ) {
        i.permanentLocationId = this.locations.getUuid( vLoc.code );
        isRMC = (vLoc.code.startsWith("rmc"));
      }

      vLoc = this.voyLocations.getByNumber( results.getInt("temp_location") );
      if ( vLoc != null ) {
        i.temporaryLocationId = this.locations.getUuid( vLoc.code );
      }

      i.enumeration = results.getString("item_enum");
      String chron = results.getString("chron");
      if ( chron != null ) {
        if (isRMC && chron.equalsIgnoreCase("RESTRICTED"))
          addNote(i,chron,(this.itemNoteTypes.getUuid("Restrictions")));
        else
          i.chronology = chron;
      }

      String note = results.getString("item_note");
      if (note != null)
        addNote(i,note,this.itemNoteTypes.getUuid("Note"));

      String freeText = results.getString("freetext");
      if ( freeText != null && ! freeText.isEmpty() )
        addNote(i,freeText,(isRMC)?
            this.itemNoteTypes.getUuid("ArchivesSpace Top Container"):this.itemNoteTypes.getUuid("Note"));
      String spine = results.getString("spine_label");
      if ( spine != null && ! spine.isEmpty() )
        addNote(i,spine,(isRMC)?this.itemNoteTypes.getUuid("Vault location"):this.itemNoteTypes.getUuid("Note"));
      String caption = results.getString("caption");
      if ( caption != null && ! caption.isEmpty() )
        addNote(i,caption,this.itemNoteTypes.getUuid("Note"));

      if (this.circulationNotes.containsKey(itemId))
        addCirculationNotes(i,this.circulationNotes.get(itemId));

      items.add(i);
    }
    return items;
  }

  private static void addCirculationNotes(Item i, Map<VoyagerStatus, Timestamp> map) {
    for (Entry<VoyagerStatus,Timestamp> e : map.entrySet()) {
      Map<String,Object> note = new HashMap<>();
      note.put("noteType", "Check in");
      note.put("staffOnly", true);
      if ( e.getValue() == null )
        note.put("note", e.getKey().value);
      else {
        note.put("note", String.format("%s (%s)", e.getKey().value,e.getValue().toLocalDateTime().format(formatter)));
        note.put("date", e.getValue().toInstant().toString());
      }
      i.circulationNotes.add(note);
    }
  }

  private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT);


  private static void addNote(Item i, String note, String uuid) {
    Map<String, String> noteMap = new HashMap<>();
    noteMap.put("itemNoteTypeId", uuid);
    noteMap.put("note", note);
    noteMap.put("staffOnly", "true");
    i.notes.add(noteMap);
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
    public List<Map<String,Object>> circulationNotes = new ArrayList<>();
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
