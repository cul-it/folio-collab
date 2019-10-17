package edu.cornell.library.folioimpl.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.cornell.library.folioimpl.objects.MarcRecord;


public class DownloadMarc {

  public static MarcRecord get(Connection voyager, MarcRecord.RecordType type, Integer recordId)
      throws SQLException, IOException {

    String query;
    if (type.equals(MarcRecord.RecordType.BIBLIOGRAPHIC))
      query = "SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = ? ORDER BY BIB_DATA.SEQNUM";
    else if (type.equals(MarcRecord.RecordType.HOLDINGS))
      query = "SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = ? ORDER BY MFHD_DATA.SEQNUM";
    else
      query = "SELECT * FROM AUTH_DATA WHERE AUTH_DATA.AUTH_ID = ? ORDER BY AUTH_DATA.SEQNUM";

    try (PreparedStatement pstmt = voyager.prepareStatement(query);
        ByteArrayOutputStream bb = new ByteArrayOutputStream()) {
      pstmt.setInt(1, recordId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next())
          bb.write(rs.getBytes("RECORD_SEGMENT"));
      }
      if (bb.size() == 0)
        return null;
      bb.close();
      return new MarcRecord(bb.toByteArray());
    }
  }
}
