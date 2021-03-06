package edu.cornell.library.folioimpl.scripts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import edu.cornell.library.folioimpl.interfaces.CreateRecord;
import edu.cornell.library.folioimpl.interfaces.ModificationLogic;
import edu.cornell.library.folioimpl.objects.Dependency;
import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.objects.ReferenceData;
import edu.cornell.library.folioimpl.tools.CopyDataSetFromOneFolioToAnother;

public class CopyUsersFromOneInstanceToAnother {

  public static void main(String[] args) throws IOException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")) {
      prop.load(in);
    }

    OkapiClient okapi32dmg =new OkapiClient( prop.getProperty("url32dmg"), prop.getProperty("token32dmg") );
    OkapiClient okapi32sb = new OkapiClient( prop.getProperty("url32sb"),  prop.getProperty("token32sb")  );
    OkapiClient okapi32ermdev = new OkapiClient( prop.getProperty("url32ermdev"), prop.getProperty("token32ermdev")  );
    OkapiClient okapi4ermdev =new OkapiClient( prop.getProperty("url4ermdev"), prop.getProperty("token4ermdev"), prop.getProperty("tenant4ermdev") );
    OkapiClient okapi4erm =new OkapiClient( prop.getProperty("url4erm"), prop.getProperty("token4erm"), prop.getProperty("tenant4erm") );
    OkapiClient okapi4dmg =new OkapiClient( prop.getProperty("url4dmg"), prop.getProperty("token4dmg"), prop.getProperty("tenant4dmg") );
    OkapiClient okapi4sb =new OkapiClient( prop.getProperty("url4sb"), prop.getProperty("token4sb"), prop.getProperty("tenant4sb") );

    OkapiClient from = okapi32sb;
    OkapiClient to = okapi4sb;

    ReferenceData patronGroups = new ReferenceData( to, "/groups", "group");
    ReferenceData permissions =  new ReferenceData( to, "/perms/permissions", "displayName");

    List<String> dmgUserList = Arrays.asList(prop.getProperty("migrationteam").split(","));
    List<String> sbUserList = Arrays.asList(prop.getProperty("allusers").split(","));
/*
    {
    CopyDataSetFromOneFolioToAnother.Builder users = new CopyDataSetFromOneFolioToAnother.Builder();
    users.setSourceOkapi(from).setDestOkapi(to).setEndPoint("/users").setDeleteUnmatched(false);
    users.setWhiteListFilter("patronGroup",Arrays.asList(patronGroups.getUuid("staff")));
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
    users.addSatellite(
        new CreateRecord() {
          @Override public String getEndPoint() { return "/perms/users"; }
          @Override public Map<String, Object> buildRecord(Map<String, Object> user) {
            Map<String, Object> r= new HashMap<>();
            r.put("userId", user.get("id"));
            r.put("permissions", Arrays.asList("660d8f5c-a269-468b-9df2-45f0490251e4"));
            return r;
          }});
    users.build().execute();
    }
    System.exit(0);
*/
    {
    CopyDataSetFromOneFolioToAnother.Builder users = new CopyDataSetFromOneFolioToAnother.Builder();
    users.setSourceOkapi(from).setDestOkapi(to).setEndPoint("/users").setDeleteUnmatched(false);
    users.setWhiteListFilter("username",sbUserList);
    users.setModificationLogic(
        new ModificationLogic() {
          @Override public boolean modify(Map<String, Object> user) {
            boolean changed = false;
            if ( ! user.containsKey("barcode" ) )
            { Random r = new Random(); user.put("barcode",r.nextInt(Integer.MAX_VALUE)); changed = true; }
            if ( ! user.containsKey("patronGroup" ) )
            { user.put("patronGroup",patronGroups.getUuid("STAF")); changed = true; }
            return changed;
          }});
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
    }

    CopyDataSetFromOneFolioToAnother.Builder servicePointUsers = new CopyDataSetFromOneFolioToAnother.Builder();
    servicePointUsers.setSourceOkapi(from).setDestOkapi(to).setEndPoint("/service-points-users");
    servicePointUsers.setDependencyFilter(new Dependency("userId","/users"));
    servicePointUsers.build().execute();

  }

}
