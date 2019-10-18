package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.OkapiClient;

public class EmptySRSandInstanceStore {

  public static void main(String[] args) throws IOException {


    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi = new OkapiClient(prop.getProperty("url32sb"),prop.getProperty("token32sb"));
//    okapi.deleteAll("/holdings-storage/holdings", true);
    okapi.deleteAll("/source-storage/records", "deleted==false", true);
    okapi.deleteAll("/instance-storage/instances", true);
  }

}
