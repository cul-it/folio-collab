package edu.cornell.library.folioimpl.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.folioimpl.objects.Item2Json;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class ItemTest {

  static Connection voyager = null;
  static OkapiClient okapi = null;
  static Item2Json item2json = null;

  @BeforeClass
  public static void connect() throws SQLException, IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }
    String url = prop.getProperty("voyagerDBUrl");
    String user = prop.getProperty("voyagerDBUser");
    String pass = prop.getProperty("voyagerDBPass");
    voyager = DriverManager.getConnection(url, user, pass);
    okapi = new OkapiClient(prop.getProperty("url4sb"), prop.getProperty("token4sb"), prop.getProperty("tenant4sb"));
    item2json = new Item2Json( voyager, okapi );
  }


  @Test
  public void damagedItem() throws SQLException {
    List<Item2Json.Item> items = item2json.getItemsForMfhdId(78724, voyager);
    for ( Item2Json.Item i : items ) {
      assertEquals("2007-06-14T19:06:26.000+0000",i.itemDamagedStatusDate);
      /*
       * {"id":"8f0f2b8b-723e-111e-a053-ba1bec844b3a",
       * "hrid":"99418",
       * "materialTypeId":"1a54b431-2e4f-452d-9cae-9cee66c9a892",
       * "holdingsRecordId":"8f0f2b00-851e-111e-a053-ba1bec844b3a",
       * "barcode":"31924005427285",
       * "copyNumbers":["1"],
       * "numberOfPieces":1,
       * "status":{"date":"2014-05-26T05:37:22","name":"Available"},
       * "permanentLoanTypeId":"2b94c631-fca9-4892-a730-03ee529ffe27",
       * "permanentLocationId":"41589a88-8d57-4449-b36e-6ebff7c3b1b9",
       * "itemDamagedStatusId":"54d1dd76-ea33-4bcb-955b-6b29df4f7930",
       * "itemDamagedStatusDate":"2007-06-14T19:06:26.000+0000",
       * "notes":[{"itemNoteTypeId":"8d0a5eca-25de-4391-81a9-236eeefdd20b","note":"damaged","staffOnly":"false"}]}
       */
    }

    items = item2json.getItemsForMfhdId(550428, voyager);
    for ( Item2Json.Item i : items ) {
      assertEquals("2020-03-03T17:50:02.000+0000",i.itemDamagedStatusDate);
      /*
       * {"id":"8f0f2ba0-5d91-111e-a053-ba1bec844b3a",
       * "hrid":"1564194",
       * "materialTypeId":"1a54b431-2e4f-452d-9cae-9cee66c9a892",
       * "holdingsRecordId":"8f0f2b07-cce4-111e-a053-ba1bec844b3a",
       * "barcode":"31924007607173",
       * "copyNumbers":["1"],
       * "numberOfPieces":1,
       * "status":{"date":"2020-03-04T10:16:13","name":"Checked out"},
       * "permanentLoanTypeId":"2b94c631-fca9-4892-a730-03ee529ffe27",
       * "permanentLocationId":"41589a88-8d57-4449-b36e-6ebff7c3b1b9",
       * "itemDamagedStatusId":"54d1dd76-ea33-4bcb-955b-6b29df4f7930",
       * "itemDamagedStatusDate":"2020-03-03T17:50:02.000+0000"}
       */
    }
  }

  @Test
  public void undamagedItem() throws SQLException {
    List<Item2Json.Item> items = item2json.getItemsForMfhdId(3273281, voyager);
    for ( Item2Json.Item i : items ) {
      assertNull(i.itemDamagedStatusDate);
      assertNull(i.itemDamagedStatusId);
      /*
       * {"id":"8f0f2bcd-85a2-111e-a053-ba1bec844b3a",
       * "hrid":"4688546",
       * "materialTypeId":"1a54b431-2e4f-452d-9cae-9cee66c9a892",
       * "holdingsRecordId":"8f0f2b30-f378-111e-a053-ba1bec844b3a",
       * "barcode":"31924073418273",
       * "copyNumbers":["1"],
       * "numberOfPieces":1,
       * "status":{"name":"Available"},
       * "permanentLoanTypeId":"2b94c631-fca9-4892-a730-03ee529ffe27",
       * "permanentLocationId":"47fdee74-8e6f-4b9d-ade5-d043be4e932a",
       * "notes":[{"itemNoteTypeId":"8d0a5eca-25de-4391-81a9-236eeefdd20b",
       *            "note":"recd engr 2/26/96","staffOnly":"false"}]}
       */
    }
  }
}
