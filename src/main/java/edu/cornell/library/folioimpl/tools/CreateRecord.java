package edu.cornell.library.folioimpl.tools;

import java.util.Map;

public interface CreateRecord {

  public String getEndPoint();
  public Map<String,Object> buildRecord( Map<String,Object> parentRecord );
}
