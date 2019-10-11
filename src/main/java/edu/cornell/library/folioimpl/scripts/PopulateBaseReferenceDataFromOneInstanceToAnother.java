package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.tools.CopyDataSetFromOneFolioToAnother;

public class PopulateBaseReferenceDataFromOneInstanceToAnother {

  public static void main(String[] args) throws IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi31 =   new OkapiClient( prop.getProperty("url31dmg"), prop.getProperty("token31dmg") );
    OkapiClient okapi31sb = new OkapiClient( prop.getProperty("url31sb"),  prop.getProperty("token31sb")  );
    OkapiClient okapi32dmg =new OkapiClient( prop.getProperty("url32dmg"), prop.getProperty("token32dmg") );
    OkapiClient okapi32sb = new OkapiClient( prop.getProperty("url32sb"),  prop.getProperty("token32sb")  );

    OkapiClient from = okapi31;
    OkapiClient to   = okapi32sb;


    to.deleteAll("/service-points-users", true);
    to.deleteAll("/locations", true);
    to.deleteAll("/location-units/libraries", true);
    to.deleteAll("/location-units/campuses", true);
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/institutions");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/campuses");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/libraries");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/locations");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/service-points");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/material-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/loan-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/groups");

  }

}
