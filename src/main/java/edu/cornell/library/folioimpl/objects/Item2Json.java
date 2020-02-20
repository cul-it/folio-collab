package edu.cornell.library.folioimpl.objects;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Item2Json {

  // essentially the 'columns' names/aliases used in the main query
  static String[] selectTerms = { "item_id", "mfhd_uuid", "item_barcode", "item_enum", "chron", "copy_number", "pieces",
      "comp_status", "item_type_id", "temp_item_type_id", "perm_location", "temp_location", "item_note" };

  static public String selectClause = String.join(", ", selectTerms);

  // initialized by SQL query in getItemTypes() call
  // item_type_id -> item_type_name
  public Map<String, String> itemTypeHash;

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

  private static ObjectMapper mapper = new ObjectMapper();

  final ReferenceData itemNoteTypes;
  final ReferenceData materialTypes;
  final ReferenceData loanTypes;
  final ReferenceData locations;
  public Item2Json ( Connection voyager, OkapiClient okapi ) throws IOException, SQLException {

    this.itemNoteTypes = new ReferenceData(okapi, "/item-note-types", "name");
    this.materialTypes = new ReferenceData(okapi, "/material-types", "name");
    this.loanTypes = new ReferenceData(okapi, "/loan-types", "name");
    this.locations = new ReferenceData(okapi, "/locations", "code");
    this.itemTypeHash = getItemTypes( voyager );

  }

  public List<Item> getItemsForMfhdId(Integer mfhd, Connection voyager) throws SQLException {

    String query = "";
    query = "select distinct ";
    // TBD
    // v----- need this pl/sql function
    query += "   item.item_id, jrm424.mfhd_uuid(mfhd_item.mfhd_id) mfhd_uuid, ";
    // query += " item.item_id, mfhd_item.mfhd_id, ";
    // query += " item_barcode.item_barcode, mfhd_item.item_enum, ";
    query += "   jrm424.bar_code(item.item_id) item_barcode, mfhd_item.item_enum, ";
    query += "   mfhd_item.chron, item.copy_number, ";
    query += "   item.pieces, jrm424.mrlo(item_status.item_id) comp_status, ";
    query += "   item.item_type_id, item.temp_item_type_id, ";
    query += "   jrm424.location_code(item.perm_location) perm_location, ";
    query += "   jrm424.location_code(item.temp_location) temp_location, ";
    query += "   item_note.item_note ";
    query += "from";
    query += "    item, mfhd_item, item_status,";
    query += "    item_note, item_barcode ";
    query += "where";
    query += " item.item_id = mfhd_item.item_id (+) and ";
    query += " item.item_id = item_status.item_id (+) and ";
    query += " item.item_id = item_note.item_id (+) and ";
    query += " item.item_id = item_barcode.item_id  (+) and ";
    query += " mfhd_item.mfhd_id = " + mfhd + " order by item.item_id ";

    ArrayList<String> res = readItemRecords(voyager, query);

    ArrayList<String> columns = new ArrayList<>();
    columns = new ArrayList<>(Arrays.asList(selectClause.split(", ")));
    int numberOfItems = res.size() / columns.size();
    if ( numberOfItems == 0 )
      return new ArrayList<>();

    return processResultSet(res, columns, voyager );
  }

  public class Item { //TODO
    public String id = null;
    public List<String> formerIds = new ArrayList<>();
    public String materialTypeId = null;
    public String holdingsRecordId = null;
    public String barcode = null;
    public String enumeration = null;
    public String chronology = null;
    public List<String> copyNumbers = new ArrayList<>();
    public String numberOfPieces = null;
    public Map<String,String> status = new HashMap<>();
    public String permanentLoanTypeId = null;
    public String temporaryLoanTypeId = null;
    public String permanentLocationId = null;
    public String temporaryLocationId = null;
    public List<Map<String,String>> notes = new ArrayList<>();
    public String getId() { return id; }

    @Override
    public String toString() {
      String s = null;
      try {
        s = mapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {}
      return s;
    }

  }

  /**
   * @param query
   * 
   * @return result set as 1 dimensional ArrayList<String> will address it as a 2D
   *         array
   */
  public static ArrayList<String> readItemRecords(Connection con, String query)
      throws SQLException {

    ArrayList<String> res = new ArrayList<>();
    // collect column names
    ArrayList<String> columns = new ArrayList<>();
    columns = new ArrayList<>(Arrays.asList(selectClause.split(", ")));

    try ( Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query) ) {

      ResultSetMetaData rsmd = rs.getMetaData();
      System.out.println(rsmd.getColumnCount());

      String cval = ""; // result for column
      while (rs.next()) {
        for (int n = 0; n < columns.size(); n++) {
          String c = columns.get(n);

          // accumulate row's columns
          // google how-to-write-a-utf-8-file-with-java
          // >>>---> part of UTF-8 fix <---<<<
          /*
           * if(n < 13) { cval = rs.getString(columns.get(n)); } else { byte[] b =
           * rs.getBytes(n); if(b == null) { cval = null; } else { cval = new
           * String(b,StandardCharsets.UTF_8); } }
           */
          cval = rs.getString(c);
          res.add(cval);
        }
      }
      // prevents java.sql.SQLException: ORA-01000: maximum open cursors exceeded
      rs.close();

      // con.close();
      return res;
    }
  }

  /**
   * 
   * @param res
   * @param columns
   */
  public List<Item> processResultSet(ArrayList<String> res, ArrayList<String> columns, Connection con )
      throws SQLException {
    List<Item> items = new ArrayList<>();
    int N = res.size() / columns.size(); // number of rows

    // treat res as a 2 dimensional array
    // N rows x Csize columns
    int Csize = columns.size();

    for (int n = 0; n < N; n++) {

      Item i = new Item();

      String item_id = res.get(n * Csize);

      boolean barCodeFlag = false;
      String result = "";
      for (int m = 0; m < Csize; m++) {
        // m is column number
        if (!result.equals(""))
          result += ",";

        // k is where column value is in res array
        int k = Csize * n + m;
        String r = columns.get(m);
        String colval = res.get(k);

        result += colval;

        if (m == 0) {
          i.id = getItemUuid(con, colval);
          i.formerIds.add(colval);

          // do material type here as well
          i.materialTypeId = this.materialTypes.getUuid(getMaterialTypeId(con, colval));
        } else if (m == 1) {
          i.holdingsRecordId = colval;
        } else if (m == 2) {
          i.barcode = colval;
          if (colval == null) {
            barCodeFlag = true;
            // System.exit(1);
          }
        } else if (m == 3) {
          i.enumeration = colval;
        } else if (m == 4) {
          i.chronology = colval;
        } else if (m == 5) {
          i.copyNumbers.add(colval);
        } else if (m == 6) {
          i.numberOfPieces = colval;
        } else if (m == 7) {
          String[] parts = colval.split("\\|");
          i.status.put("name", statusStrings.get(Integer.parseInt(parts[1])));
          if (!"<null>".equals(parts[2]))
            i.status.put("date", parts[2]);
        } else if (m == 8) {
          String v = this.itemTypeHash.get(colval);
          if (v != null)
            i.permanentLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(v));
        } else if (m == 9) {
          String v = this.itemTypeHash.get(colval);
          if (v != null)
            i.temporaryLoanTypeId = this.loanTypes.getUuid(loanTypeHash.get(v));
        } else if (m == 10) {
          String v = this.locations.getUuid(colval);
          if (v == null)
            v = this.locations.getUuid("void");
          i.permanentLocationId = v;
        } else if (m == 11) {
          if (colval != null) {
            String v = this.locations.getUuid(colval);
            if (v == null)
              v = this.locations.getUuid("void");
            i.temporaryLocationId = v;
          }
        } else if (m == 12) {
          if (colval != null) {
            Map<String, String> note = new HashMap<>();
            // "8d0a5eca-25de-4391-81a9-236eeefdd20b"
            note.put("itemNoteTypeId", this.itemNoteTypes.getUuid("Note"));
            note.put("note", colval);
            note.put("staffOnly", "false");
            i.notes.add(note);
          }
        } else {
          // catch all
          System.out.println("Item value not handled "+r.toLowerCase());
          System.exit(1);
        }

      }
      if (barCodeFlag) {
        System.out.println("Empty item - no barcode status 1 " + item_id);

      }
      System.out.println(i.toString());
      System.out.println("result = " + result);
      System.out.flush();
      items.add(i);
    }

    return items;
  }


  private static List<String> statusStrings = Arrays.asList("not used do not remove 0-th entry", "Not Charged",
      "Charged", "Renewed", "Overdue", "Recall Request", "Hold Request", "On Hold", "In Transit",
      "In Transit Discharged", "In Transit On Hold", "Discharged", "Missing", "Lost--Library Applied",
      "Lost--System Applied", "Claims Returned", "Damaged", "Withdrawn", "At Bindery", "Cataloging Review",
      "Circulation Review", "Scheduled", "In Process", "Call Slip Request", "Short Loan Request",
      "Remote Storage Request");

  //get and make hash from item_type tbl
  /**
   * 
   * @param con
   * @throws SQLException
   */
  public Map<String, String> getItemTypes(Connection con) throws SQLException {
    Map<String, String> res = new HashMap<>();
    try ( Statement stmt = con.createStatement() ) {
      String q = "SELECT ITEM_TYPE_ID, ITEM_TYPE_NAME FROM ITEM_TYPE ORDER BY ITEM_TYPE_ID";
      try ( ResultSet rs = stmt.executeQuery(q) ) {
        while (rs.next()) {
          String id = rs.getString("ITEM_TYPE_ID");
          String idn = rs.getString("ITEM_TYPE_NAME");
          res.put(id, idn);
        }
      }
    }
    return res;
  }

  /**
   * 
   * @param con
   * @param iid
   * @return materialTypeId
   * @throws SQLException
   */
  public static String getMaterialTypeId(Connection con, String iid) throws SQLException {
    String materialTypeId = "";
    String q = "select jrm424.matype(" + iid + ") mt from dual";

    try ( Statement stmt = con.createStatement();
          ResultSet rs = stmt.executeQuery(q) ) {
      while ( rs.next() )  materialTypeId = rs.getString("mt");
    }

    return materialTypeId;
  }

  public static String getItemUuid(Connection con, String iid) throws SQLException {
    String res = "";
    String q = "select jrm424.item_id_uuid(" + iid + ") uuid from dual";

    try ( Statement stmt = con.createStatement();
          ResultSet rs = stmt.executeQuery(q) ) {
      rs.next();
      res = rs.getString("uuid");
    }
    if (res.startsWith("dup_")) {
      // verrry rare ...
      // 1 billion calls/second for a year yields 50% chance of dup
      System.err.println("DUPLICATE UUID in getItemUuid " + iid + " -> " + res);
      System.exit(1);
    }
    return res;
  }

}
