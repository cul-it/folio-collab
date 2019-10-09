package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// curl -i -w '\n' -X GET -H 'X-Okapi-Tenant: diku' -H "X-Okapi-Token: $OKAPITOKEN" "$OKAPIURL/location-units/libraries"
// curl -i -w '\n' -X PUT -H 'Content-Type: application/json' -H 'X-Okapi-Tenant: diku' -H "X-Okapi-Token: $OKAPITOKEN" "$OKAPIURL/location-units/libraries/54a50776-0bd4-41b2-867c-815ffc721313" -d '{"id" : "54a50776-0bd4-41b2-867c-815ffc721313","name" : "Olin Library", "code" : "OL","campusId" : "23d95983-8e2b-4a3a-8d89-71a90b2efacc"}'
// curl -i -w '\n' -X POST -H 'Content-Type: application/json' -H 'X-Okapi-Tenant: diku' -H "X-Okapi-Token: $OKAPITOKEN" "$OKAPIURL/location-units/libraries" -d '{"name" : "Veterinary Library", "code" : "VET","campusId" : "23d95983-8e2b-4a3a-8d89-71a90b2efacc"}'
public class Libraries {

  private final static String ithacaCampus = "23d95983-8e2b-4a3a-8d89-71a90b2efacc";
  private final static String onlineCampus = "470ff1dd-937a-4195-bf9e-06bcfcd135df";

  public Set<Library> loadLibraries(String sourceFile) throws JsonParseException, JsonMappingException, IOException {

    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(sourceFile)) {
      ObjectMapper mapper = new ObjectMapper();
      Set<Library> libraries = mapper.readValue(convertStreamToString(in).replaceAll("(?m)^#.*$", ""),
          new TypeReference<LinkedHashSet<Library>>() {
          });
    }
    return null;
  }

  public class Library {

    final private String libraryName;
    final private int circGroup;
    final private UUID uuid;

    private Library(String libraryName, int circGroup) {
      this.libraryName = libraryName;
      this.circGroup = circGroup;
      this.uuid = UUID.randomUUID();
    }
  }

  private static String convertStreamToString(java.io.InputStream is) {
    String val;
    try (java.util.Scanner s = new java.util.Scanner(is)) {
      s.useDelimiter("\\A");
      val = s.hasNext() ? s.next() : "";
    }
    return val;
  }

}
