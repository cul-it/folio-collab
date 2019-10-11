package edu.cornell.library.folioimpl.objects;

public class Dependency {

  public final String foreignKey;
  public final String endPoint;

  public Dependency ( String foreignKey, String endPoint ) {
    this.foreignKey = foreignKey;
    this.endPoint = endPoint;
  }
}
