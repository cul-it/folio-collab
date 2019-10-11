package edu.cornell.library.folioimpl.objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OkapiClient {

  private final String url;
  private final String token;

  public OkapiClient( String okapiUrl, String accessToken ) {
    this.url = okapiUrl;
    this.token = accessToken;
    //TODO add test of okapi availability
  }

  public String post( String endPoint, String json ) throws IOException {

    //TODO Improve error detection and handling

    System.out.println( "About to post " +endPoint+ " "+json);
    HttpURLConnection c = commonConnectionSetup(endPoint);
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setDoInput(true);
    OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
    writer.write(json);
    writer.flush();
//      int responseCode = httpConnection.getResponseCode();
//      if (responseCode != 200)
//          throw new IOException(httpConnection.getResponseMessage());
    StringBuilder sb = new StringBuilder();
    try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    return sb.toString();
  }

  public String put( String endPoint, Map<String,Object> object ) throws IOException {
    return put( endPoint, (String)object.get("id"), mapper.writeValueAsString(object) );
  }

  public String put( String endPoint, String uuid, String json ) throws IOException {

    HttpURLConnection c = commonConnectionSetup(endPoint+"/"+uuid);
    c.setRequestMethod("PUT");
    c.setDoOutput(true);
    c.setDoInput(true);
    OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
    writer.write(json);
    writer.flush();
//      int responseCode = httpConnection.getResponseCode();
//      if (responseCode != 200)
//          throw new IOException(httpConnection.getResponseMessage());
    StringBuilder sb = new StringBuilder();
    try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    return sb.toString();
  }

  public String delete( String endPoint, String uuid ) throws IOException {
    StringBuilder sb = delete( endPoint, uuid, new StringBuilder());
    return sb.toString();
  }

  public String deleteAll( String endPoint, boolean verbose ) throws IOException {

    return deleteAll( endPoint, null, verbose );
  }

  public String deleteAll( String endPoint, String notDeletedQuery, boolean verbose ) throws IOException {

    StringBuilder sb = new StringBuilder();

    Map<String,Map<String,Object>> existing = queryAsMap(endPoint,notDeletedQuery, null);

    while ( ! existing.isEmpty() ) {
      for ( String uuid : existing.keySet() ) {
        sb.append("Deleting ").append(endPoint).append('/').append(uuid)
          .append(' ').append(mapper.writeValueAsString(existing.get(uuid))).append('\n');
        HttpURLConnection c = commonConnectionSetup(endPoint+"/"+uuid);
        c.setRequestMethod("DELETE");
        try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
          String line = null;
          while ((line = br.readLine()) != null) {
            if ( ! line.isEmpty() && ! line.trim().isEmpty() )
              sb.append(line).append('\n');
          }
        }
        if ( verbose ) {
          System.out.println(sb.toString());
          sb.setLength(0);
        }
      }
      existing = queryAsMap(endPoint,notDeletedQuery, null);

    }
    return sb.toString();
  }

  public String getRecord( String endPoint, String uuid ) throws IOException {
    HttpURLConnection c = commonConnectionSetup(endPoint+"/"+uuid);
    int responseCode = c.getResponseCode();
    if (responseCode != 200)
      throw new NoSuchObjectException(c.getResponseMessage());

    try (InputStream is = c.getInputStream()) {
      return convertStreamToString(is);
        }
  }

  public List<Map<String,Object>> queryAsList( String endPoint, String query, Integer limit )
      throws IOException {
    return resultsToList( query(endPoint,query,limit) );
  }

  public Map<String,Map<String,Object>> queryAsMap( String endPoint, String query, Integer limit )
      throws IOException {
    return resultsToMap( query(endPoint,query,limit) );
  }

  public String query( String endPoint, String query, Integer limit ) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(endPoint);
    if ( query != null ) {
      sb.append("?query=");
      sb.append(URLEncoder.encode(query,"UTF-8"));
    }
    if ( limit != null ) {
      sb.append((query == null)?"?limit=":"&limit=");
      sb.append(limit);
    }
    HttpURLConnection c = commonConnectionSetup(sb.toString());
    int responseCode = c.getResponseCode();
    if (responseCode != 200)
      throw new IOException(c.getResponseMessage());

    try (InputStream is = c.getInputStream()) {
      return convertStreamToString(is);
    }
  }

  public static Map<String,Map<String,Object>> resultsToMap(String readValue)
      throws JsonParseException, JsonMappingException, IOException {
    Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
    Map<String,Map<String,Object>> dataMap = new HashMap<>();
    for (String mainKey : rawData.keySet()) 
      if ( ! mainKey.equals("totalRecords") && ! mainKey.equals("resultInfo") ) {
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> records = (ArrayList<Map<String,Object>>)rawData.get(mainKey);
        for ( Map<String,Object> record : records )
          dataMap.put((String)record.get("id"), record);
      }
    return dataMap;
  }

  public static List<Map<String,Object>> resultsToList(String readValue)
      throws JsonParseException, JsonMappingException, IOException {
    Map<String, Object> rawData = mapper.readValue(readValue, Map.class);
    for (String mainKey : rawData.keySet()) {
      if ( ! mainKey.equals("totalRecords") && ! mainKey.equals("resultInfo") ) {
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> records = (ArrayList<Map<String,Object>>)rawData.get(mainKey);
        return records;
      }
    }
    return null;
  }


  // END OF PUBLIC UTILITIES

  private HttpURLConnection commonConnectionSetup( String path ) throws IOException {
    URL fullPath = new URL( this.url + path );
    HttpURLConnection c = (HttpURLConnection) fullPath.openConnection();
    c.setRequestProperty("Content-Type", "application/json;charset=utf-8");
    c.setRequestProperty("X-Okapi-Tenant", "diku");
    c.setRequestProperty("X-Okapi-Token", this.token);
    return c;
    
  }

  private StringBuilder delete( String endPoint, String uuid, StringBuilder sb ) throws IOException {
    HttpURLConnection c = commonConnectionSetup(endPoint+"/"+uuid);
    c.setRequestMethod("DELETE");
//      int responseCode = httpConnection.getResponseCode();
//      if (responseCode != 200)
//          throw new IOException(httpConnection.getResponseMessage());
    try ( BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream(), "utf-8")) ) {
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    }
    return sb;
  }
  private static String convertStreamToString(java.io.InputStream is) {
    try (java.util.Scanner s = new java.util.Scanner(is)) {
      s.useDelimiter("\\A");
      return s.hasNext() ? s.next() : "";
    }
  }
  private static ObjectMapper mapper = new ObjectMapper();
}