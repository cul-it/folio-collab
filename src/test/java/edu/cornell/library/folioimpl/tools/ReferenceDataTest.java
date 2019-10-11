package edu.cornell.library.folioimpl.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.objects.ReferenceData;

public class ReferenceDataTest {

  static OkapiClient okapi = null;

  @BeforeClass
  public static void connect() throws IOException {
    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }
    okapi = new OkapiClient(prop.getProperty("okapiurl21dmgurl"),prop.getProperty("okapiurl21dmgtok"));
  }

  @Test
  public void classificationTypes() throws IOException {

    ReferenceData callNoTypeUuids = new ReferenceData(okapi, "/classification-types", "name");

    assertEquals("9a60012a-0fcf-4da9-a1d1-148e818c27ad", callNoTypeUuids.getUuid("National Agricultural Library"));

    assertEquals(null, callNoTypeUuids.getUuid("fake"));
  }

  @Test
  public void identifierTypes() throws IOException {

    ReferenceData idTypeUuids = new ReferenceData(okapi, "/identifier-types", "name");
    idTypeUuids.writeMapToStdout();

  }

  @Test
  public void locations() throws IOException {

    ReferenceData locUuids = new ReferenceData(okapi, "/locations", "code");
    locUuids.setDefault("void");

    assertEquals("11290651-9ff2-4231-a2a6-e6fe62a2357a", locUuids.getUuid("afr"));

    assertEquals(locUuids.getUuid("void"), locUuids.getUuid("fake"));

    assertEquals(null, locUuids.getStrictUuid("fake"));

  }

}
