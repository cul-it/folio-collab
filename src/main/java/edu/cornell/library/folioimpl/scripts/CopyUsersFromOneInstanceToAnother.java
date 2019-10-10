package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import edu.cornell.library.folioimpl.tools.CopyDataSetFromOneFolioToAnother;
import edu.cornell.library.folioimpl.tools.CreateRecord;
import edu.cornell.library.folioimpl.tools.Dependency;
import edu.cornell.library.folioimpl.tools.ModificationLogic;
import edu.cornell.library.folioimpl.tools.OkapiClient;
import edu.cornell.library.folioimpl.tools.ReferenceData;

public class CopyUsersFromOneInstanceToAnother {

  public static void main(String[] args) throws IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi31sb = new OkapiClient( prop.getProperty("url31sb"),  prop.getProperty("token31sb")  );
    OkapiClient okapi32dmg =new OkapiClient( prop.getProperty("url32dmg"), prop.getProperty("token32dmg") );
    OkapiClient okapi32sb = new OkapiClient( prop.getProperty("url32sb"),  prop.getProperty("token32sb")  );

    OkapiClient from = okapi31sb;
    OkapiClient to = okapi32dmg;

    ReferenceData patronGroups = new ReferenceData( to, "/groups", "group");
    ReferenceData permissions =  new ReferenceData( to, "/perms/permissions", "displayName");

    CopyDataSetFromOneFolioToAnother.Builder users = new CopyDataSetFromOneFolioToAnother.Builder();
    users.setSourceOkapi(from).setDestOkapi(to).setEndPoint("/users").setDeleteUnmatched(false);
    users.setWhiteListFilter("username",Arrays.asList("fbw4","rld244","jrc88","jrm424","nac26"));
    users.setModificationLogic(
        new ModificationLogic() {
          @Override
          public boolean modify(Map<String, Object> user) {
            boolean changed = false;
            if ( ! user.containsKey("barcode" ) )
            { Random r = new Random(); user.put("barcode",r.nextInt(Integer.MAX_VALUE)); changed = true; }
            if ( ! user.containsKey("patronGroup" ) )
            { user.put("patronGroup",patronGroups.getUuid("STAF")); changed = true; }
            return changed;
          }
        });
    users.addSatellite(
        new CreateRecord() {
          @Override public String getEndPoint() { return "/perms/users"; }
          @Override public Map<String, Object> buildRecord(Map<String, Object> user) {
            Map<String, Object> r= new HashMap<>();
            r.put("userId", user.get("id"));
            r.put("permissions", Arrays.asList(permissions.getUuid("super_user")));
            return r;
          }});
    users.addSatellite(
        new CreateRecord() {
          @Override public String getEndPoint() { return "/authn/credentials"; }
          @Override public Map<String, Object> buildRecord(Map<String, Object> user) {
            Map<String, Object> r= new HashMap<>();
            r.put("userId", user.get("id"));
            String userName = (String)user.get("username");
            r.put("username", userName);
            r.put("password", userName);
            return r;
          }});
    users.build().execute();


    CopyDataSetFromOneFolioToAnother.Builder servicePointUsers = new CopyDataSetFromOneFolioToAnother.Builder();
    servicePointUsers.setSourceOkapi(from).setSourceOkapi(to).setEndPoint("/service-points-users");
    servicePointUsers.setDependencyFilter(new Dependency("userId","/users"));
    servicePointUsers.build().execute();
  }

}
