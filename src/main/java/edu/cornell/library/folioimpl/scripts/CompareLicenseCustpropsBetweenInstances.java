package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.tools.CompareFolioEndPoint;

public class CompareLicenseCustpropsBetweenInstances {

  public static void main(String[] args) throws IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi32ermdev =new OkapiClient( prop.getProperty("url32ermdev"), prop.getProperty("token32ermdev") );
    OkapiClient okapi4ermdev =new OkapiClient( prop.getProperty("url4ermdev"), prop.getProperty("token4ermdev"), prop.getProperty("tenant4ermdev") );
    OkapiClient okapi4erm =new OkapiClient( prop.getProperty("url4erm"), prop.getProperty("token4erm"), prop.getProperty("tenant4erm") );
    List<String> ignoreFields = Arrays.asList("id","category->id","category->values[]->id");
    CompareFolioEndPoint.compare(okapi4ermdev, okapi4erm, "/licenses/custprops","name", ignoreFields);

  }
}