package edu.cornell.library.folioimpl.objects;

import java.util.*;
import java.sql.*;
import java.io.BufferedReader;
import org.apache.commons.cli.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.json.*;

public class Item2Json {
  static int Jmade = 0;
  static boolean range = true;
  static boolean verb = false;
  static boolean uuid = true;
  static String dbhost = "database.library.cornell.edu";
  static String dbport = "1521";
  static String dbuser = "";
  static String dbpw = "";
  static String sid = "VGER";
  static String jdir = ""; // json file directory

  // should get these files from folio before run
  // only need to get once - in principal
  //
  static String mapath = "c:/Users/Jrm424/Desktop/Tokens2UUIDs/MaterialTypes.json";
  static String mtypes = "mtypes";
  static String ltpath = "c:/Users/Jrm424/Desktop/Tokens2UUIDs/LoanTypes.json";
  static String ltypes = "loantypes";
  static String itemnotepath = "c:/Users/Jrm424/Desktop/Tokens2UUIDs/ItemNoteTypes.json";
  static String itemno = "itemNoteTypes";
  static String locpath = "c:/Users/Jrm424/Desktop/Tokens2UUIDs/Locations.json";
  static String locations = "locations";

  static void initDbVarsEtc() throws IOException {
    //
    // java -Dprog.args.path=/my/path/filename -jar Item2Json.jar [args]
    //
    String srcfile = System.getProperty("prog.args.path");

    if (srcfile != null) {
      File SF = new File(srcfile);
      if (SF.exists()) {
        BufferedReader br = new BufferedReader(new FileReader(SF));
        String srcsraw = "";
        while ((srcsraw = br.readLine()) != null) {
          srcsraw = srcsraw.trim();
          if (srcsraw.startsWith("#"))
            continue;
          String[] parts = srcsraw.split("=");
          if ("user".equals(parts[0])) { // required
            dbuser = parts[1].trim();
          } else if ("pw".equals(parts[0])) { // required
            dbpw = parts[1].trim();
          } else if ("host".equals(parts[0])) {
            dbhost = parts[1].trim();
          } else if ("sid".equals(parts[0])) {
            sid = parts[1].trim();
          } else if ("port".equals(parts[0])) {
            dbport = parts[1].trim();
          } else if ("jdir".equals(parts[0])) {
            jdir = parts[1].trim();
          } else if ("mapath".equals(parts[0])) {
            // material types
            mapath = parts[1].trim();
          } else if ("ltpath".equals(parts[0])) {
            // loan types
            ltpath = parts[1].trim();
          } else if ("itemnotepath".equals(parts[0])) {
            // item not types
            itemnotepath = parts[1].trim();
          } else if ("locpath".equals(parts[0])) {
            // item locations
            locpath = parts[1].trim();
          }
        }
        br.close();
      } else {
        System.err.println("Can't find " + srcfile);
        System.exit(1);
      }
    }

  }

  // development
  // jdbc:oracle:thin:@voy-test9-db.library.cornell.edu:1521:VGER
  // jdbc:oracle:thin:@database-test.library.cornell.edu:1521:VGER

  // production
  // jdbc:oracle:thin:@voy-db.library.cornell.edu:1521:VGER
  // jdbc:oracle:thin:@database.library.cornell.edu:1521:VGER

  static String constr = "jdbc:oracle:thin:@" + dbhost + ":" + dbport + ":" + sid;

  static public void updateConnectString() {
    constr = "jdbc:oracle:thin:@" + dbhost + ":" + dbport + ":" + sid;
  }

  // the item IDs in this hash come from the item file(s)
  // the query result item IDs are looked for on this list
  // and filtered out if not found (voyager is an active system)
  static Map<String, String> itemfilter = new HashMap<>();

  // essentially the 'columns' names/aliases used in the main query
  static String[] selectTerms = { "item_id", "mfhd_uuid", "item_barcode", "item_enum", "chron", "copy_number", "pieces",
      "comp_status", "item_type_id", "temp_item_type_id", "perm_location", "temp_location", "item_note" };

  static public String selectClause = "";

  static {
    for (int n = 0; n < selectTerms.length; n++) {
      if (!"".equals(selectClause)) {
        selectClause += ", ";
      }
      selectClause += selectTerms[n];
    }
  }

  static Map<String, String> jsonNames = new HashMap<>();

  static {
    jsonNames.put("item_barcode", "barcode");
    jsonNames.put("item_enum", "enumeration");
    jsonNames.put("chron", "chronology");
    jsonNames.put("pieces", "numberOfPieces");
    /*
     * jsonNames.put("",""); jsonNames.put("", "");
     */
  }

  // initialized by SQL query in getItemTypes() call
  // item_type_id -> item_type_name
  public static Map<String, String> itemTypeHash;

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

  /*
   * static { loanTypeHash.put("1dayloan","Can circulate");
   * loanTypeHash.put("1dayres","Reserves");
   * loanTypeHash.put("1hrloan","Reserves");
   * loanTypeHash.put("1hrres","Reserves");
   * loanTypeHash.put("1wkloan","Can circulate");
   * loanTypeHash.put("1wkres","Reserves");
   * loanTypeHash.put("2dayloan","Reserves");
   * loanTypeHash.put("2dayres","Reserves");
   * loanTypeHash.put("2hrloan","Reserves");
   * loanTypeHash.put("2hrres","Reserves");
   * loanTypeHash.put("2wkloan","Can circulate");
   * loanTypeHash.put("3dayloan","Can circulate");
   * loanTypeHash.put("3dayres","Reserves");
   * loanTypeHash.put("3hrloan","Equipment");
   * loanTypeHash.put("3hrres","Equipment");
   * loanTypeHash.put("4hrloan","Equipment");
   * loanTypeHash.put("4hrres","Reserves");
   * loanTypeHash.put("8hrres","Equipment");
   * loanTypeHash.put("archivmanu","Nocirc");
   * loanTypeHash.put("book","Can circulate");
   * loanTypeHash.put("computfile","Can circulate");
   * loanTypeHash.put("equipment","Equipment");
   * loanTypeHash.put("keys","Equipment"); loanTypeHash.put("laptop","Laptop");
   * loanTypeHash.put("map","Nocirc"); loanTypeHash.put("maps","Nocirc");
   * loanTypeHash.put("microform","Can circulate");
   * loanTypeHash.put("music","Can circulate");
   * loanTypeHash.put("newbook","Can circulate");
   * loanTypeHash.put("newspaper","Can circulate");
   * loanTypeHash.put("nocirc","Nocirc");
   * loanTypeHash.put("periodical","Can circulate");
   * loanTypeHash.put("permres","Reserves");
   * loanTypeHash.put("serial","Can circulate");
   * loanTypeHash.put("soundrec","Can circulate");
   * loanTypeHash.put("specloan","Equipment");
   * loanTypeHash.put("umbrella","Equipment");
   * loanTypeHash.put("unbound","Can circulate");
   * loanTypeHash.put("visual","Can circulate"); }
   */
  // these are the requested item_ids
  public static Set<String> askedToDo = new HashSet<>();
  // these are the item_ids found to do
  public static Set<String> foundToDo = new HashSet<>();

  // first declare calling parameters and options
  static Options defineOptions() {

    Options options = new Options();

    // item_id file if any
    Option opifile = Option.builder("i").longOpt("ifile").required(false).argName("filename").numberOfArgs(1)
        .desc("file listing voyager item ids; one per line").build();
    options.addOption(opifile);

    Option oprng = Option.builder("l").longOpt("list").required(false)
        .desc("make where clause term an ID list vs range of IDs").build();
    options.addOption(oprng);

    // item_id files directory if any
    Option opbfdir = Option.builder("I").longOpt("Ifdir").required(false).argName("directory path").numberOfArgs(1)
        .desc("direcory containing files of voyager item ids").build();
    options.addOption(opbfdir);

    // use item file to filter out item_ids not returned by
    // where clause constructed from file min and max
    // where min(file) <= item_id and item_id <= max(file)
    // and item_id in fileHash
    //
    Option opfilter = Option.builder("f").longOpt("filter").required(false).desc("treat item_id file aa a filter")
        .build();
    options.addOption(opfilter);

    Option opverb = Option.builder("v").longOpt("verbose").required(false).desc("verbose mode").build();
    options.addOption(opverb);

    Option opjdir = Option.builder("j").longOpt("jdir").required(false).argName("directory path").numberOfArgs(1)
        .desc("path to json files out directory").build();
    options.addOption(opjdir);

    Option opjara = Option.builder("a").longOpt("array").required(false).desc("output (to STDOUT) a json array as well")
        .build();
    options.addOption(opjara);

    Option opmv = Option.builder("m").longOpt("move").required(false).desc("move item files to done directory").build();
    options.addOption(opmv);

    Option opnuuid = Option.builder("n").longOpt("nuuid").required(false).desc("non-uuid mode").build();
    options.addOption(opnuuid);

    Option opout = Option.builder("o").longOpt("jsonout").required(false).desc("output mode").build();
    options.addOption(opout);

    Option opnobc = Option.builder("N").longOpt("nbcl").required(false).argName("file path").numberOfArgs(1)
        .desc("path to file listing barcode-rejected items").build();
    options.addOption(opnobc);

    return options;
  }

  static boolean jsonout = false;
  static String noBarCodePath = "";
  static PrintWriter nbcout = null;

  public static void main(String args[]) throws IOException {
    // note current time
    long start = System.nanoTime();

    // set up of command line options
    Options options = defineOptions();

    // no args output
    int argc = args.length;
    if (argc == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Item2Json", options);
      System.exit(0);
    }

    // JAVA -D OPTION INIT
    // init db vars from properties file declared by
    // -Dprog.args.path=/my/path/filename on command line
    //
    initDbVarsEtc();

    // ITEM NOTE TYPE HASH
    // build item note type uuid mapping ... from file
    JSONObject joitmnp = readJsonObject(itemnotepath);
    itemnotetype2uuid = ctrldVocab2Map(joitmnp, itemno, false);

    // MATERIAL TYPE HASH
    // material type UUIDs ... from file
    JSONObject jomatype = readJsonObject(mapath);
    mtype2uuid = ctrldVocab2Map(jomatype, mtypes, false);

    // LOAN TYPE HASH
    // loan types uuid mapping ... from file
    JSONObject joltype = readJsonObject(ltpath);
    ltype2uuid = ctrldVocab2Map(joltype, ltypes, false);

    // LOCATION TYPE HASH
    // location codes to uuid mapping ... from file
    JSONObject jolocs = readJsonObject(locpath);
    loc2uuid = ctrldVocab2Map(jolocs, locations, true);

    // DB CONNECT STRING
    // adjust connect string if needed
    updateConnectString();
    System.err.println("constring = " + constr);

    // COMMAND LINE ARGS
    // figure out command line call
    CommandLineParser parser = new DefaultParser();
    CommandLine line = null;
    try {
      line = parser.parse(options, args);
    } catch (ParseException ex) {
      String reason = ex.getMessage();
      if (argc != 0) {
        System.err.println("Parsing failed.  Reason: " + reason);
        System.exit(1);
      }
    }
    if (line.hasOption("nbcl")) {
      noBarCodePath = line.getOptionValue("nbcl");
      nbcout = new PrintWriter(new FileWriter(noBarCodePath), true);
    }

    verb = line.hasOption("verbose");

    if (line.hasOption("jdir")) { // command line override.
      jdir = line.getOptionValue("jdir");
    }
    File jfd = new File(jdir);
    if (!jfd.exists())
      jfd.mkdir(); // could fail
    // jfd aka jdir still might not exist; so check!

    jsonout = line.hasOption("jsonout") && jfd.exists();

    // collect files to process
    ArrayList<String> itemfiles = new ArrayList<>();

    // indicate where clause should br a list or not
    boolean idlist = line.hasOption("list");
    range = !idlist;

    if (line.hasOption("Idir") && line.hasOption("ifile")) {
      System.err.println("Conflicting directions. --ifile and --Idir don't work together. choose one.");
      System.exit(1);
    }
    if (line.hasOption("Ifdir")) {
      // read Ifdir any add files to 'files' list
      String dpath = line.getOptionValue("Ifdir");
      File[] dir = new File(dpath).listFiles();
      for (File file : dir) {
        if (file.isFile()) {
          itemfiles.add(file.getName());
        }
      }
      File done = new File(dpath + "/done");
      if (!done.exists()) {
        done.mkdir();
      }
      done.setWritable(true);
    } else if (line.hasOption("ifile")) {
      // one file
      itemfiles.add(line.getOptionValue("ifile"));
    } else {
      System.err.println("Nothing to process.");
      System.exit(0);
    }

    // collect column names in an array
    ArrayList<String> columns = new ArrayList<>();
    columns = new ArrayList<String>(Arrays.asList(selectClause.split(", ")));
    int F = 0;
    // connect, query and generate json
    try {
      Connection con = oracleConnect(dbuser, dbpw);

      itemTypeHash = getItemTypes(con);

      int N = 0;
      int S = 0;
      String dpath = line.getOptionValue("Ifdir");
      // for each file
      for (String f : itemfiles) {
        // build where clause term
        String lastWhereTerm = "";

        System.err.println("File " + f);
        /*
         * These files are usually constructed by the getItemIds script from the rows in
         * the voyager item table but in general they should be as follows:
         * 
         * each filename should be of the form i1-i2.txt where i1,i2 are item ids and i1
         * <= i2. moreover the first entry of the file should be i1 and the last i2 with
         * all other entries between i1 and i2. all entries should be found in the
         * voyager item table item_id column.
         * 
         * files should not overlap to avoid wasted processing time
         * 
         * files with a larger number of entries are more efficient than smaller ones
         * 
         * 
         * rangeWhere returns 'i1 <= item.item_id and item.item_id <= i2' with i1 and i2
         * being literal values
         * 
         * otherwise (example) "item_id IN ('123',...) "
         */
        if (range || line.hasOption("Idir"))
          lastWhereTerm = rangeWhere(dpath + "/" + f);
        else
          lastWhereTerm = idListWhere(dpath + "/" + f);

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
        query += lastWhereTerm + " order by item.item_id ";

        ArrayList<String> res = readItemRecords(con, query);

        N += res.size() / columns.size();
        S += processResultSet(res, columns, con);
        F++;
        if (verb)
          System.err.println("Files " + F);
        // when item file finished move to 'done' directory
        // so that it is not reprocessed on a restart after error
        if (line.hasOption("move")) {
          Files.move(Paths.get(dpath + "/" + f), Paths.get(dpath + "/done/" + f), StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        }

      }

      // calc execution time in seconds
      System.err.println(S + " skipped");
      System.err.println(Jmade + " jsons made");
      System.err.println("Number of Item IDs " + itemfilter.size());
      System.err.println("Item Files Processsed " + F);
      long et = System.nanoTime() - start;
      String ext = String.format("%.2f", (float) et / 1000000000.0);
      System.err.println("ext= " + ext + " seconds for " + N + " item ids");

      con.close();
    } catch (Exception e) {
      System.err.println(e);
      System.err.println("Item Files Processsed " + F);
    }

    SetOps();
    if (nbcout != null) {
      nbcout.close();
    }
  }

  /**
   * 
   * @param u
   * @param p
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static Connection oracleConnect(String u, String p) throws SQLException, ClassNotFoundException {
    // load the driver class
    Class.forName("oracle.jdbc.driver.OracleDriver");

    // create the connection object
    Connection con = DriverManager.getConnection(constr, u, p);
    return con;
  }

  /**
   * @param q query
   * 
   * @return result set as 1 dimensional ArrayList<String> will address it as a 2D
   *         array
   */
  public static ArrayList<String> readItemRecords(Connection con, String query)
      throws SQLException, ClassNotFoundException {

    ArrayList<String> res = new ArrayList<>();
    // collect column names
    ArrayList<String> columns = new ArrayList<>();
    columns = new ArrayList<String>(Arrays.asList(selectClause.split(", ")));

    // create the statement object
    Statement stmt = con.createStatement();

    //

    if (verb)
      System.err.println(query);

    ResultSet rs = stmt.executeQuery(query);
    ResultSetMetaData rsmd = rs.getMetaData();
    System.out.println(rsmd.getColumnCount());

    String cval = ""; // result for column
    int N = 0;
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
      N++; // count rows
    }
    // prevents java.sql.SQLException: ORA-01000: maximum open cursors exceeded
    rs.close();
    if (verb)
      System.err.println("res size = " + res.size() + " rows N=" + N);
    // con.close();
    return res;
  }

  /**
   * 
   * @param res
   * @param columns
   * @throws IOException
   */
  public static int processResultSet(ArrayList<String> res, ArrayList<String> columns, Connection con)
      throws SQLException, IOException {
    int N = res.size() / columns.size(); // number of rows
    int Skipped = 0;
    // treat res as a 2 dimensional array
    // N rows x Csize columns
    int Csize = columns.size();
    if (verb)
      System.err.println("res = " + res.size() + " rows N=" + N);

    for (int n = 0; n < N; n++) {

      JSONObject jrow = new JSONObject();

      String item_id = res.get(n * Csize);

      // accumulate the set of item_ids found by query
      foundToDo.add(item_id.trim());

      // was the item id one that was requested?
      // itemfilter is a hash created for the exact purpose
      // of answering this question
      if ((itemfilter.get(item_id.trim()) == null)) {
        Skipped++;
        continue; // skip if not asked for
      }

      // make full json file path name
      String jfpath = jdir + "/item-" + item_id + ".json";
      if (verb) {
        System.err.println(jfpath + " " + item_id + " " + n);
      }

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
          String ituuid = getItemUuid(con, colval);
          jrow.put("id", ituuid);
          JSONArray jaray = new JSONArray();
          jaray.put(colval);
          jrow.put("formerIds", jaray);

          // do material type here as well
          String mt = getMeterialTypeId(con, colval);
          String mtuuid = mtype2uuid.get(mt);
          jrow.put("materialTypeId", mtuuid);
        } else if (m == 1) {
          jrow.put("holdingsRecordId", colval);
        } else if (m == 2) {
          jrow.put("barcode", colval);
          if (colval == null) {
            barCodeFlag = true;
            // System.exit(1);
          }
        } else if (m == 3) {
          jrow.put("enumeration", colval);
        } else if (m == 4) {
          jrow.put("chronology", colval);
        } else if (m == 5) {
          JSONArray jaray = new JSONArray();
          jaray.put(colval);
          jrow.put("copyNumbers", jaray);
        } else if (m == 6) {
          jrow.put("numberOfPieces", colval);
        } else if (m == 7) {
          String[] parts = colval.split("\\|");
          JSONObject jstat = new JSONObject();
          jstat.put("name", statusStrings.get(Integer.parseInt(parts[1])));
          if (!"<null>".equals(parts[2]))
            jstat.put("date", parts[2]);
          jrow.put("status", jstat);
        } else if (m == 8) {
          String v = itemTypeHash.get(colval);
          if (v != null) {
            // translate and lookup
            String loantypeuuid = ltype2uuid.get(loanTypeHash.get(v));
            jrow.put("permanentLoanTypeId", loantypeuuid);
          }
        } else if (m == 9) {
          String v = itemTypeHash.get(colval);
          if (v != null) {
            // translate and lookup
            String loantype = ltype2uuid.get(loanTypeHash.get(v));
            jrow.put("temporaryLoanTypeId", loantype);
          }
        } else if (m == 10) { // really 10
          // if(colval != null) { //most likely drop this test
          String v = loc2uuid.get(colval);
          if (v == null)
            v = loc2uuid.get("void");
          jrow.put("permanentLocationId", v);
          // }
        } else if (m == 11) { // really 11
          if (colval != null) {
            String v = loc2uuid.get(colval);
            if (v == null)
              v = loc2uuid.get("void");
            jrow.put("temporaryLocationId", v);
          }
        } else if (m == 12) {
          if (colval != null) {
            JSONArray jaray = new JSONArray();
            Map<String, String> parts = new HashMap<>();
            String id = itemnotetype2uuid.get("Note");
            // "8d0a5eca-25de-4391-81a9-236eeefdd20b"
            parts.put("itemNoteTypeId", id);
            parts.put("note", colval);
            parts.put("staffOnly", "false");
            jaray.put(parts);
            jrow.put("notes", jaray);
          }
        } else {
          // catch all

          String s = jsonNames.get(r.toLowerCase());
          if (s != null)
            jrow.put(s, colval);
          else
            jrow.put(r.toLowerCase(), colval);
        }

      }
      if (barCodeFlag) {
        System.out.println("Empty item - no barcode status 1 " + item_id);
        if (nbcout != null) {
          System.out.println(jfpath);
          nbcout.println(jfpath);
        }

      }
      System.out.println(jrow.toString(4));
      System.out.println("result = " + result);
      System.out.flush();
      if (jsonout && !barCodeFlag) {
        // prepare to output json to file
        //

        // >>>---> part of UTF-8 fix <---<<<
        Writer jout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jfpath), "UTF-8"));
        jout.write(jrow.toString(4));
        jout.write('\n');
        jout.flush();
        jout.close();
      }
      Jmade++;
    }

    return Skipped;
  }

  /**
   * 
   * @param fpath path to a item id file
   * @return a where clause of the form 'low <= item_id and item_id <= high'
   */
  public static String rangeWhere(String fpath) {
    String where = "";
    int low = 1000000000;
    int hi = -1;
    BufferedReader br = null;
    String itemline = "";
    // populate item filter hash from file and find smallest and largest
    // item_id in file to use as end points of where clause inequalities.
    // For various reasons ORACLE may return item_ids not in the file
    // that are larger than the smallest and smaller than the largest
    // BUT we want only the IDs in the filter file (hash).
    //
    try {
      br = new BufferedReader(new FileReader(fpath));
      while ((itemline = br.readLine()) != null) {
        int nitem = Integer.parseInt(itemline);
        if (nitem <= low)
          low = nitem; // update low
        if (nitem >= hi)
          hi = nitem; // update hi
        itemfilter.put(itemline.trim(), "T");
        askedToDo.add(itemline.trim());
      }
      where = "" + low + " <= item.item_id and item.item_id <= " + hi + "";
      if (verb) {
        System.err.println(where);
      }
    } catch (FileNotFoundException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return where;
  }

  /**
   * 
   * @param fpath
   * @return
   */
  public static String idListWhere(String fpath) {
    String where = ""; // default case; no -i file

    // this case make a where clause of the form
    // "ITEM_ID IN ('123', '456', ... )"
    // not very efficient for ORACLE when the
    // number of item_ids is large.
    BufferedReader br = null;
    String iidline = "";
    String items = "";

    // command line contains: --ifile fpath

    try {
      br = new BufferedReader(new FileReader(fpath));
      while ((iidline = br.readLine()) != null) {
        itemfilter.put(iidline.trim(), "T");
        askedToDo.add(iidline.trim());
        if (!"".equals(items)) {
          items += ",";
        }
        items += "'" + iidline.trim() + "'";
      }
      where = " ITEM_ID IN (" + items + ")";
      if (verb)
        System.err.println(where);
    } catch (FileNotFoundException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    // System.err.println(where);
    return where;
  }

  /**
   * Calculates set differences between the set of itemids asked for (askedToD)
   * and the set of itemids returned (foundToDo) by the Oracle query
   * 
   * foundNotAsked = foundToDo \ askedToDo unexpected itemids created since
   * itemids were collected askedNotFound = askedToDo \ foundToDo itemids that
   * were deleted since itemids were collected
   * 
   */
  static public void SetOps() {
    // do set differences

    // found but not asked for
    Set<String> fna = new HashSet<String>(foundToDo);
    fna.removeAll(askedToDo);

    // asked for but not found
    Set<String> anf = new HashSet<String>(askedToDo);
    anf.removeAll(foundToDo);

    // found but not asked out
    int count = fna.size();
    System.out.println("ITEM_IDs found but not asked for. count " + count);
    Iterator<String> it = fna.iterator();
    if (verb)
      while (it.hasNext())
        System.out.println(it.next());

    // asked but not found out
    count = anf.size();
    System.out.println("ITEM_IDs asked for but not found. count " + count);
    it = anf.iterator();
    if (verb)
      while (it.hasNext())
        System.out.println(it.next());
  }

  private static List<String> statusStrings = Arrays.asList("not used do not remove 0-th entry", "Not Charged",
      "Charged", "Renewed", "Overdue", "Recall Request", "Hold Request", "On Hold", "In Transit",
      "In Transit Discharged", "In Transit On Hold", "Discharged", "Missing", "Lost--Library Applied",
      "Lost--System Applied", "Claims Returned", "Damaged", "Withdrawn", "At Bindery", "Cataloging Review",
      "Circulation Review", "Scheduled", "In Process", "Call Slip Request", "Short Loan Request",
      "Remote Storage Request");

  // get and make hash from item_type tbl
  /**
   * 
   * @param con
   * @return
   * @throws SQLException
   */
  public static Map<String, String> getItemTypes(Connection con) throws SQLException {
    Map<String, String> res = new HashMap<>();
    Statement stmt = con.createStatement();
    String q = "SELECT ITEM_TYPE_ID, ITEM_TYPE_NAME FROM ITEM_TYPE ORDER BY ITEM_TYPE_ID";
    if (verb)
      System.err.println(q);
    ResultSet rs = stmt.executeQuery(q);
    while (rs.next()) {
      String id = rs.getString("ITEM_TYPE_ID");
      String idn = rs.getString("ITEM_TYPE_NAME");
      if (verb)
        System.err.println(id + " -> " + idn);
      res.put(id, idn);
    }
    rs.close();
    return res;
  }

  /**
   * 
   * @param con
   * @param iid
   * @return
   * @throws SQLException
   */
  public static String getMeterialTypeId(Connection con, String iid) throws SQLException {
    String res = "";
    String q = "select jrm424.matype(" + iid + ") mt from dual";
    if (verb)
      System.err.println(q);
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery(q);
    rs.next();
    res = rs.getString("mt");
    rs.close();
    if (verb)
      System.err.println(iid + " -> " + res);

    return res;
  }

  public static String getItemUuid(Connection con, String iid) throws SQLException {
    String res = "";
    String q = "select jrm424.item_id_uuid(" + iid + ") uuid from dual";
    if (verb)
      System.err.println(q);

    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery(q);
    rs.next();
    res = rs.getString("uuid");
    rs.close();
    if (verb)
      System.err.println(iid + " -> " + res);
    if (res.startsWith("dup_")) {
      // verrry rare ...
      // 1 billion calls/second for a year yields 50% chance of dup
      System.err.println("DUPLICATE UUID in getItemUuid " + iid + " -> " + res);
      System.exit(1);
      ;
    }
    return res;
  }

  /**
   * 
   * @param fpath
   * @return
   */
  public static JSONObject readJsonObject(String fpath) {
    String content = "";
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(fpath));
      String in = "";
      while ((in = br.readLine()) != null)
        content += in;

    } catch (FileNotFoundException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println(fpath);
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return new JSONObject(content);
  }

  /**
   * 
   * @param jo
   * @param araname
   * @param cmap
   * @return
   */
  public static Map<String, String> ctrldVocab2Map(JSONObject jo, String araname, boolean cmap) {

    Map<String, String> m = new HashMap<>();

    JSONArray jara = jo.getJSONArray(araname);
    int N = jara.length();

    for (int i = 0; i < N; i++) {
      JSONObject jitem = jara.getJSONObject(i);

      String name = jitem.getString("name");
      String id = jitem.getString("id");

      String source = "";
      if (jitem.has("source"))
        source = jitem.getString("source");

      String code = "";
      if (jitem.has("code"))
        code = jitem.getString("code");

      if (cmap) {
        if (!"".equals(code)) {
          if (verb)
            System.out.println(code + " -> " + id);
          m.put(code, id);
        } // else empty map as output
      } else {
        if (verb)
          System.out.println(name + " -> " + id);
        m.put(name, id);
      }
    }
    return m;
  }

  // gets these files from folio before run
  //
  // initialized from file itemnotepath drawn from folio
  public static Map<String, String> itemnotetype2uuid = new HashMap<>();
  // initialized from file mapath drawn from folio
  public static Map<String, String> mtype2uuid = new HashMap<>();
  // initialized from file ltpath drawn from folio
  public static Map<String, String> ltype2uuid = new HashMap<>();
  // initialized from file locpath drawn from folio
  public static Map<String, String> loc2uuid = new HashMap<>();
}
