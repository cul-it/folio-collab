package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.folioimpl.objects.Holding;
import edu.cornell.library.folioimpl.objects.OkapiClient;

public class HoldingTest {

  static Connection voyager = null;
  static OkapiClient okapi = null;

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
    okapi = new OkapiClient(prop.getProperty("okapiurl21dmgurl"), prop.getProperty("okapiurl21dmgtok"));
  }

  @Test
  public void getHoldings() throws SQLException, IOException, XMLStreamException {
    Holding h = new Holding(voyager, okapi, 2);
    System.out.println(h.marc.toString());
  }

}
