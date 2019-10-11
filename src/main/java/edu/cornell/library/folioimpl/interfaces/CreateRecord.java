package edu.cornell.library.folioimpl.interfaces;

import java.util.Map;

public interface CreateRecord {

  public String getEndPoint();
  public Map<String,Object> buildRecord( Map<String,Object> parentRecord );
}
