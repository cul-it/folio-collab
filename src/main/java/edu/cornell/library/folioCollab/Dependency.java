package edu.cornell.library.folioCollab;

public class Dependency {

  public final String foreignKey;
  public final String endPoint;

  public Dependency ( String foreignKey, String endPoint ) {
    this.foreignKey = foreignKey;
    this.endPoint = endPoint;
  }
}
