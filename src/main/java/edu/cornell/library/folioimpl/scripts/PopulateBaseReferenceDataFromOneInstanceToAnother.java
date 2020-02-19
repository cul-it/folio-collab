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

    OkapiClient okapi32ermdev =new OkapiClient( prop.getProperty("url32ermdev"), prop.getProperty("token32ermdev") );
    OkapiClient okapi32erm    =new OkapiClient( prop.getProperty("url32erm"), prop.getProperty("token32erm") );
    OkapiClient okapi4ermdev =new OkapiClient( prop.getProperty("url4ermdev"), prop.getProperty("token4ermdev"), prop.getProperty("tenant4ermdev") );
    OkapiClient okapi4erm =new OkapiClient( prop.getProperty("url4erm"), prop.getProperty("token4erm"), prop.getProperty("tenant4erm") );
    OkapiClient okapi4dmg =new OkapiClient( prop.getProperty("url4dmg"), prop.getProperty("token4dmg"), prop.getProperty("tenant4dmg") );
    OkapiClient okapi4sb =new OkapiClient( prop.getProperty("url4sb"), prop.getProperty("token4sb"), prop.getProperty("tenant4sb") );

    OkapiClient from = okapi32dmg;
    OkapiClient to   = okapi4sb;

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
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/organizations-storage/categories");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/organizations-storage/contacts");
    CopyDataSetFromOneFolioToAnother.copy(from, to, "/organizations-storage/organizations");


//    CopyDataSetFromOneFolioToAnother.copy(from, to, "/perms/permissions");
//    to.deleteAll("/licenses/licenseLinks", true); (copy when source is q4 or later)
//    to.deleteAll("/licenses/licenses", true);
//    for ( int i = 1 ; i < 38; i++)
//    CopyDataSetFromOneFolioToAnother.copy(from, to, "/licenses/licenses", i);
//    CopyDataSetFromOneFolioToAnother.copy(from, to, "/licenses/refdata");
//    CopyDataSetFromOneFolioToAnother.copy(from, to, "/licenses/custprops");
  }

}
