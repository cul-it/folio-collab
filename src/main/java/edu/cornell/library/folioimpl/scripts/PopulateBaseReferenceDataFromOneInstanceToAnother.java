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

    OkapiClient okapi32dmg =new OkapiClient( prop.getProperty("url32dmg"), prop.getProperty("token32dmg") );
    OkapiClient okapi32sb = new OkapiClient( prop.getProperty("url32sb"),  prop.getProperty("token32sb")  );

    OkapiClient from = okapi32dmg;
    OkapiClient to   = okapi32sb;

    to.deleteAll("/locations", true);
    to.deleteAll("/location-units/libraries", true);
    to.deleteAll("/location-units/campuses", true);
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/institutions");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/campuses");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/location-units/libraries");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/locations");
    to.deleteAll("/service-points-users", true);
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/service-points");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/material-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/loan-types");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/loan-policy-storage/loan-policies");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/groups");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/organizations-storage/contacts");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/organizations-storage/organizations");
  }

}
