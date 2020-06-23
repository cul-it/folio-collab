package edu.cornell.library.folioimpl.scripts;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.Item2Json;
import edu.cornell.library.folioimpl.objects.Item2Json.Item;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class WriteAllItemJsonToFile {

  static int fileNo = 0;
  static int recordInFileCount = Integer.MAX_VALUE;
  static int recordsPerFile = 50_000;
  static BufferedWriter output = null;

  public static void main(String[] args) throws IOException, SQLException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient( prop.getProperty("urlFTest"), prop.getProperty("tokenFTest"), prop.getProperty("tenantFTest") );

    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass"));
        Statement stmt = voyager.createStatement(); ){

      Item2Json itemLoader = new Item2Json(voyager, okapi);
      stmt.setFetchSize(100_000);

      try ( ResultSet rs = stmt.executeQuery("select item_id from item order by item_id")) {

        while ( rs.next() ) {
          Item i = itemLoader.getItemById(rs.getInt(1), voyager);
          if ( i == null ) continue;
          writeToFile(i);
        }

      }}
    if (output != null) {
      output.flush();
      output.close();
    }
  }

  private static void writeToFile(Item i) throws IOException {
    if ( recordInFileCount >= recordsPerFile ) {
      if (output != null) {
        output.flush();
        output.close();
      }
      output = Files.newBufferedWriter(Paths.get(String.format("items%03d.json", ++fileNo)));
      recordInFileCount = 0;
    }
    output.write(i.toString()+"\n");
    recordInFileCount++;
  }

}
