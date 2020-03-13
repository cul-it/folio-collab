package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.rmi.NoSuchObjectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.Holding;
import edu.cornell.library.folioimpl.objects.Item2Json;
import edu.cornell.library.folioimpl.objects.Item2Json.Item;
import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.tools.Holdings;

public class GetHoldingsForInstances {

  static String instanceEndPoint = "/instance-storage/instances";
  static String holdingEndPoint  = "/holdings-storage/holdings";
  static String itemEndPoint     = "/item-storage/items";
  static Item2Json itemLoader = null;

  public static void main(String[] args) throws IOException, SQLException, NumberFormatException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient( prop.getProperty("url4dmg"), prop.getProperty("token4dmg"), prop.getProperty("tenant4dmg") );
//    OkapiClient okapi = new OkapiClient( prop.getProperty("url4sb"), prop.getProperty("token4sb"), prop.getProperty("tenant4sb") );

    try ( Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"), prop.getProperty("voyagerDBUser"), prop.getProperty("voyagerDBPass")) ) {

      itemLoader = new Item2Json(voyager, okapi);

      List<Map<String,Object>> instances = okapi.queryAsList(instanceEndPoint,"id==* sortBy hrid",null);
      String hridCursor = processInstanceBatch(voyager, okapi, instances);

//      String modDateCursor = "2019-08-14T00:25:52.702+0000";
      while (hridCursor != null) {

        instances = okapi.queryAsList(instanceEndPoint,
            String.format("hrid > \"%s\" sortBy hrid",hridCursor),null);
        hridCursor = processInstanceBatch(voyager, okapi, instances);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static String processInstanceBatch( Connection voyager, OkapiClient okapi,
      List<Map<String,Object>> instances)
      throws NumberFormatException, SQLException, IOException {

    String lastHrid = null;
    for (Object instanceObject : instances) {
      Map<String, Object> instance = (HashMap<String, Object>) instanceObject;
      lastHrid = (String) instance.get("hrid");
      String bibId = ((String) instance.get("hrid"));

      List<Holding> holdings = Holdings.getHoldingsForBibRecord(
          voyager, okapi, Integer.valueOf(bibId), (String)instance.get("id"));
      for (Holding h : holdings) {
        boolean holdingAlreadyInFolio = false;
        try {
          okapi.getRecord(holdingEndPoint, h.getId());
          okapi.put(holdingEndPoint, h.getId(), h.toString());
          holdingAlreadyInFolio = true;
        } catch ( NoSuchObjectException e ) {
          okapi.post(holdingEndPoint, h.toString());
        }
        List<Item> items = itemLoader.getItemsForMfhdId(
            Integer.valueOf((String)h.holding.get("hrid")),voyager);
        for ( Item item : items ) {
          if ( ! holdingAlreadyInFolio ) {
            okapi.post(itemEndPoint, item.toString());
          } else {
            try {
              okapi.getRecord(itemEndPoint, item.getId());
              okapi.put(itemEndPoint, item.getId(), item.toString());
            } catch ( NoSuchObjectException e ) {
              okapi.post(itemEndPoint, item.toString());
            }
          }
        }
      }
    }
    return lastHrid;
  }

}
