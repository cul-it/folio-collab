package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.folioimpl.objects.OkapiClient;

public class CompareFolioEndPoint {

  public static void compare(
      OkapiClient o1, OkapiClient o2, String endPoint, String primaryKey, List<String> ignoreFields ) throws IOException {

    List<Map<String,Object>> recs1 = o1.queryAsList(endPoint, null, 15000);
    System.out.println(recs1.size());
    List<Map<String,Object>> recs2 = o2.queryAsList(endPoint, null, 15000);
    System.out.println(recs2.size());

    if ( ignoreFields != null ) {
      for ( Map<String,Object> m1 : recs1 ) removeIgnored(m1,ignoreFields);
      for ( Map<String,Object> m2 : recs2 ) removeIgnored(m2,ignoreFields);
    }

    List<String> matchedPrimaryKeys = new ArrayList<>();
    one: for ( Map<String,Object> m1 : recs1 ) {
      for ( Map<String,Object> m2 : recs2 ) {
        if ( m1.get(primaryKey).equals(m2.get(primaryKey))) {
          matchedPrimaryKeys.add((String)m1.get(primaryKey));
          if ( ! Objects.deepEquals(m1, m2) )
            nestedCompare(m1,m2,(String)m1.get(primaryKey));
          continue one;
        }
      }
      System.out.println("-"+mapper.writeValueAsString(m1));
    }

    for ( Map<String,Object> m2 : recs2 ) {
      if ( matchedPrimaryKeys.contains(m2.get(primaryKey)) )
        continue;
      System.out.println("-"+mapper.writeValueAsString(m2));
    }
  }
  private static void nestedCompare(Object o1, Object o2, String breadcrumb) throws JsonProcessingException {
    if ( o1 instanceof Map ) {
      if ( ! (o2 instanceof Map) ) {
        System.out.printf("-%s: %s\n", breadcrumb, mapper.writeValueAsString(o1));
        System.out.printf("+%s: %s\n", breadcrumb, mapper.writeValueAsString(o2));
        return;
      }
      Map<String,Object> m1 = (Map<String,Object>)o1;
      Map<String,Object> m2 = (Map<String,Object>)o2;
      Set<String> matchedKeys = new HashSet<>();
      for ( String key : m1.keySet() ) {
        if ( m2.containsKey(key) ) {
          matchedKeys.add(key);
          if ( ! Objects.deepEquals(m1.get(key), m2.get(key)) ) {
            nestedCompare( m1.get(key), m2.get(key), breadcrumb+"->"+key);
          }
          continue;
        }
        System.out.printf("-%s->%s: %s\n", breadcrumb, key, m1.get(key));
      }
      for ( String key : m2.keySet() )
        if ( ! matchedKeys.contains(key) )
          System.out.printf("+%s->%s: %s\n", breadcrumb, key, m2.get(key));
      
    }
    if ( o1 instanceof String ) {
      if ( ! Objects.equals(o1,o2) ) {
        System.out.printf("-%s: %s\n", breadcrumb, mapper.writeValueAsString(o1));
        System.out.printf("+%s: %s\n", breadcrumb, mapper.writeValueAsString(o2));
        return;
      }
    }
    if ( o1 instanceof List ) {
      if ( ! (o2 instanceof List) ) {
        System.out.printf("-%s: %s\n", breadcrumb, mapper.writeValueAsString(o1));
        System.out.printf("+%s: %s\n", breadcrumb, mapper.writeValueAsString(o2));
        return;
      }
      Set<String> s1 = new HashSet<>();
      for ( Object obj1 : (List<Object>)o1) s1.add(mapper.writeValueAsString(obj1));
      Set<String> s2 = new HashSet<>();
      for ( Object obj2 : (List<Object>)o2) s2.add(mapper.writeValueAsString(obj2));
      Set<String> matchedValues = new HashSet<>();
      for (String s : s1)
        if ( s2.contains(s) )
          matchedValues.add(s);
        else
          System.out.printf("-%s: %s\n", breadcrumb, s);
      for (String s : s2)
        if ( ! matchedValues.contains(s) )
          System.out.printf("+%s: %s\n", breadcrumb, s);
    }
  }
  private static void removeIgnored(Map<String, Object> obj, List<String> ignoreFields) throws JsonProcessingException {

    for (String field : ignoreFields) {
      int arrInd = field.indexOf("[]");
      int hashInd = field.indexOf("->");
      if ( arrInd == -1 && hashInd == -1 ) {
        obj.remove(field);
        continue;
      }
      drillDown(obj,field,arrInd,hashInd);
    }

  }

  private static void drillDown(Object obj, String field, int arrInd, int hashInd) throws JsonProcessingException {
    if ( obj == null ) return;
    if ( hashInd == -1 && arrInd == -1) {
      ((Map<String,Object>)obj).remove(field);
      return;
    }
    if ( hashInd > -1 ) {
      if ( arrInd == -1 || arrInd > hashInd ) {
        drillDown( ((Map<String,Object>)obj).get(field.substring(0, hashInd)), field.substring(hashInd+2),
            field.substring(hashInd+2).indexOf("[]"), field.substring(hashInd+2).indexOf("->"));
        return;
      }
    }
    if ( arrInd > -1 ) {
      List<Object> objs;
      if ( arrInd > 0 ) {
        Map<String,Object> objHash = (Map<String,Object>)obj;
        String key = field.substring(0, arrInd);
        if ( ! objHash.containsKey(key) )
          return;
        objs = (List<Object>)((objHash).get(field.substring(0, arrInd)));
      } else
        objs = (List<Object>)obj;
      for (Object o : objs)
        drillDown( o, field.substring(arrInd+4), field.substring(arrInd+4).indexOf("[]"),
            field.substring(arrInd+4).indexOf("->"));
    }
  }

  private static ObjectMapper mapper = new ObjectMapper();
}
