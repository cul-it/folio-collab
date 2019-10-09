package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

public class Holdings {

  private static PreparedStatement holdingsForBibPstmt = null;
  private static PreparedStatement uuidForHoldingsPstmt = null;

  public static List<Holding> getHoldingsForBibRecord(
      Connection voyager, OkapiClient okapi, Integer bibId, String instanceUuid)
          throws SQLException, IOException, XMLStreamException {
    List<Holding> holdings = new ArrayList<>();
    if (holdingsForBibPstmt == null)
      holdingsForBibPstmt = voyager.prepareStatement(
          "SELECT mm.mfhd_id , suppress_in_opac" +
           " FROM bib_mfhd bm, mfhd_master mm" +
          " WHERE bib_id = ? AND bm.mfhd_id = mm.mfhd_id");
    holdingsForBibPstmt.setInt(1, bibId);
    try (ResultSet rs = holdingsForBibPstmt.executeQuery()) {
      while (rs.next()) {
        int mfhdId = rs.getInt(1);
        boolean suppressed = rs.getString(2).equals("Y");
        if (uuidForHoldingsPstmt == null)
          uuidForHoldingsPstmt = voyager.prepareStatement(
              "SELECT uuid FROM jrm424.folio_mfhd_uuid WHERE mfhd_id = ?");
        uuidForHoldingsPstmt.setInt(1, mfhdId);
        String holdingUuid = null;
        try (ResultSet rs2 = uuidForHoldingsPstmt.executeQuery()) {
          while (rs2.next())
            holdingUuid = rs2.getString(1);
        }
        holdings.add(
            new Holding(voyager, okapi, mfhdId)
            .setSuppressed(suppressed)
            .setId(holdingUuid)
            .setInstanceId(instanceUuid));
      }
    }
    return holdings;
  }
}
