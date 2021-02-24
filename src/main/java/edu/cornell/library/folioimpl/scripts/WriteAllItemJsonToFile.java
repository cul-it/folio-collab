package edu.cornell.library.folioimpl.scripts;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.Item2Json;
import edu.cornell.library.folioimpl.objects.Item2Json.Item;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class WriteAllItemJsonToFile {

  static int recordsPerFile = 50_000;

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient( prop.getProperty("urlFTest"), prop.getProperty("tokenFTest"), prop.getProperty("tenantFTest") );

    List<Integer> allItemIds = new ArrayList<>();

    if (prop.containsKey("itemListFile")) {
      System.out.printf("Reading configured item list at '%s'. Remove config argument '%s' to load all items.\n",
          prop.getProperty("itemListFile"), "itemListFile");
      try (FileReader fr = new FileReader(new File(prop.getProperty("itemListFile")));
          BufferedReader in = new BufferedReader(fr)) {
        String itemId;
        while ((itemId = in.readLine()) != null)
          allItemIds.add(Integer.valueOf(itemId));
      } catch (IOException e) {
        System.out.println("Couldn't read configured item list. Remove from configuration to load all items.");
        e.printStackTrace();
        System.exit(1);
      }
    }

    else {

      System.out.println("itemListFile not configured. Loading all item identifiers from Voyager for export.");
      try ( Connection voyager = DriverManager.getConnection(
          prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass"));
          Statement stmt = voyager.createStatement(); ){

        stmt.setFetchSize(100_000);
        try ( ResultSet rs = stmt.executeQuery("select item_id from item order by item_id")) {

          while ( rs.next() ) {
            allItemIds.add(rs.getInt(1));
          }

        }
      }

    }

    List<List<Integer>> splitIdLists = spliterateList(allItemIds);
    System.out.printf("Exporting %d items into %d planned files.\n" , allItemIds.size(), splitIdLists.size());
    Map<String,List<Integer>> idsByOutputFile = new HashMap<>();
    for ( int i = 0; i < splitIdLists.size(); i++ )
      idsByOutputFile.put(String.format("items%03d.json",i+1), splitIdLists.get(i));

    for (Entry<String,List<Integer>> e : idsByOutputFile.entrySet())
      System.out.printf("%s: %d (%d)\n",e.getKey(),e.getValue().get(0),e.getValue().size());


    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass"))) {
      Item2Json itemLoader = new Item2Json(voyager, okapi);
      int totalItems = idsByOutputFile.entrySet().parallelStream()
          .map(entry -> processEntry(entry,prop,itemLoader)).mapToInt(Integer::intValue).sum();
      System.out.println(totalItems);
    }

  }

  private static int processEntry(Entry<String, List<Integer>> entry, Properties prop, Item2Json itemLoader) {
    int exportedItemCount = 0;
    System.out.println("Processing "+entry.getKey());

    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass"));
        BufferedWriter output = Files.newBufferedWriter(Paths.get(entry.getKey()));
        PreparedStatement itemQuery = itemLoader.prepareItemByIdQuery(voyager)) {

      for ( Integer itemId : entry.getValue() ) {
        Item i = itemLoader.getItemById(itemId, itemQuery);
        if ( i == null ) continue;
        output.write(i.toString()+"\n");
        exportedItemCount++;
      }

      output.flush();
      output.close();

    } catch (IOException | SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return exportedItemCount;
  }

  private static List<List<Integer>> spliterateList( List<Integer> ids ) {
    int splitSize = 50_000;
    List<Integer> toSplits = ids;
    List<List<Integer>> splits = new ArrayList<>();
    while ( toSplits.size() > splitSize ) {
        splits.add(toSplits.subList(0, splitSize));
        toSplits = toSplits.subList(splitSize, toSplits.size()-1);
    }
    splits.add(toSplits);
    return splits;
  }


}
