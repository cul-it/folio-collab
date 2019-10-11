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

    OkapiClient okapi31 = new OkapiClient(prop.getProperty("url31dmg"),prop.getProperty("token31dmg"));
    okapi31.deleteAll("/source-storage/records", "deleted==false", true);
    okapi31.deleteAll("/instance-storage/instances", true);
  }

}
