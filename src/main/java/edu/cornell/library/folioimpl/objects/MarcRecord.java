package edu.cornell.library.folioimpl.objects;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Objects;
import java.util.TreeSet;

public class MarcRecord {

  public String leader = " ";
  public TreeSet<ControlField> controlFields = new TreeSet<>();
  public TreeSet<DataField> dataFields = new TreeSet<>();

  @SuppressWarnings("unchecked")
  public MarcRecord( Map<String,Object> jsonStructure ) {
    this.leader = (String) jsonStructure.get("leader");
    List<Map<String,Object>> fields = (List<Map<String, Object>>) jsonStructure.get("fields");
    int fieldId = 1;
    for ( Map<String,Object> f : fields )
      for ( Entry<String,Object> field : f.entrySet() ) {
        Object fieldValue = field.getValue();
        if ( fieldValue.getClass().equals(String.class) )
          this.controlFields.add(new ControlField(fieldId++,field.getKey(),(String) fieldValue));
        else {
          Map<String,Object> fieldContent = (Map<String, Object>) fieldValue;
          int subfieldId = 1;
          List<Map<String,Object>> subfields = (List<Map<String,Object>>) fieldContent.get("subfields");
          TreeSet<Subfield> processedSubfields = new TreeSet<>();
          for (Map<String,Object> subfield : subfields) {
            String code = subfield.keySet().iterator().next();
            processedSubfields.add(new Subfield( subfieldId++, code.charAt(0), (String) subfield.get(code) ));
          }
          this.dataFields.add(new DataField(fieldId++,field.getKey(),
              ((String)fieldContent.get("ind1")).charAt(0),
              ((String)fieldContent.get("ind2")).charAt(0),
              processedSubfields
              ));
        }
      }
  }

  public MarcRecord( byte[] rawMarc ) {
    this.leader = new String( Arrays.copyOfRange(rawMarc,0,24), StandardCharsets.UTF_8 );
    int dataBaseAddress = Integer.valueOf(new String( Arrays.copyOfRange(rawMarc,12,17) ));
    byte[] directory = Arrays.copyOfRange(rawMarc, 24, dataBaseAddress);
    byte[] data = Arrays.copyOfRange(rawMarc, dataBaseAddress,rawMarc.length+1);
    int directoryPos = 0;
    int fieldId = 1;
    while (directoryPos < directory.length-1) {
      String tag = new String( Arrays.copyOfRange(directory, directoryPos, directoryPos+3));
      int fieldLength = Integer.valueOf(new String( Arrays.copyOfRange(directory,directoryPos+3,directoryPos+7)));
      int fieldStartPos = Integer.valueOf(new String( Arrays.copyOfRange(directory,directoryPos+7,directoryPos+12)));
      byte[] fieldValue =  Arrays.copyOfRange(data, fieldStartPos, fieldStartPos+fieldLength);
      directoryPos += 12;
      if ( tag.startsWith("00") )
        this.controlFields.add( new ControlField( fieldId++, tag, new String(
            Arrays.copyOfRange(fieldValue,0,fieldValue.length-1),StandardCharsets.UTF_8)));
      else {
        char ind1 = (char)fieldValue[0];
        char ind2 = (char)fieldValue[1];
        TreeSet<Subfield> subfields = new TreeSet<>();
        List<Integer> subfieldSeparatorPositions = new ArrayList<>();
        if (fieldValue[2] != (byte)0x1F)
          subfieldSeparatorPositions.add(1);
        for ( int i = 2; i < fieldValue.length ; i++ )
          if ( fieldValue[i] == (byte)0x1E || fieldValue[i] == (byte)0x1F )
            subfieldSeparatorPositions.add(i);
        for ( int i = 0; i < subfieldSeparatorPositions.size() - 1; i++) {
          int startpos = subfieldSeparatorPositions.get(i)+1;
          int endpos = subfieldSeparatorPositions.get(i+1);
          if (startpos >= endpos) continue;
          subfields.add(new Subfield(startpos,(char)fieldValue[startpos],
              new String( Arrays.copyOfRange(fieldValue, startpos+1, endpos),StandardCharsets.UTF_8)));
        }
        this.dataFields.add(new DataField(fieldId++,tag,ind1,ind2,subfields));
      }
    }
  }


  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.leader).append('\n');
    for (ControlField f : this.controlFields)
      sb.append(f.tag).append(' ').append(f.value).append('\n');
    for (DataField f : this.dataFields) {
      sb.append(f.tag).append(' ').append(f.ind1).append(f.ind2).append(' ');
      for ( Subfield sf : f.subfields )
        sb.append('\u2021').append(sf.code).append(' ').append(sf.value).append(' ');
      sb.deleteCharAt(sb.length()-1);
      sb.append('\n');
    }
    return sb.toString();
  }

  public static enum RecordType {
    BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
  }

  public static class ControlField implements Comparable<ControlField> {

    public int id;
    public String tag;
    public String value;

    public ControlField( int id, String tag, String value ) {
        this.id = id;
        this.tag = tag;
        this.value = value;
    }

    @Override public String toString() { return this.tag+" "+this.value; }

    @Override public int compareTo(final ControlField other) { return Integer.compare(this.id, other.id); }
    @Override public int hashCode() { return Integer.hashCode( this.id ); }
    @Override public boolean equals(final Object o){
        if (this == o) return true;
        if (o == null) return false;
        if (! this.getClass().equals( o.getClass() )) return false;
        ControlField other = (ControlField) o;
        return Objects.equals(this.id, other.id);
    }
}

  public static class DataField implements Comparable<DataField> {

    // Characters to frame Right-to-left text blocks to support display in mixed directional text blocks
    public static String RLE_openRTL = "\u200E\u202B\u200F";//\u200F - strong RTL invis char
    public static String PDF_closeRTL = "\u200F\u202C\u200E"; //\u200E - strong LTR invis char

    public int id;
    public String tag;
    public Character ind1 = ' ';
    public Character ind2 = ' ';
    public TreeSet<Subfield> subfields = new TreeSet<>();

    public Integer linkNumber; //from MARC subfield 6
    public String mainTag = null;

    public DataField(int id, String tag, char ind1, char ind2, TreeSet<Subfield> subfields) {
      this.id = id;
      this.tag = tag;
      this.ind1 = ind1;
      this.ind2 = ind2;
      this.subfields = subfields;
    }
    @Override
    public String toString() {
        return this.toString('\u2021');
    }
    String toString(final Character subfieldSeparator) {
      final StringBuilder sb = new StringBuilder();
      sb.append(this.tag);
      sb.append(" ");
      sb.append(this.ind1);
      sb.append(this.ind2);

      for(final Subfield sf : this.subfields) {
          sb.append(" ");
          sb.append(subfieldSeparator);
          sb.append(sf.code);
          sb.append(" ");
          sb.append(sf.value.trim());
      }
      return sb.toString();
    }
    public String concatSubfields(final String wantedSubfields) {
      final StringBuilder sb = new StringBuilder();
      Boolean first = true;
      Boolean rtl = false;
      for(final Subfield sf : this.subfields) {
          if (sf.code.equals('6'))
              if (sf.value.endsWith("/r"))
                  rtl = true;
          if (! wantedSubfields.contains(sf.code.toString()))
              continue;

          if (first) first = false;
          else sb.append(' ');
          sb.append(sf.value.trim());
      }

      final String val = sb.toString().trim();
      if (rtl && (val.length() > 0)) {
          return RLE_openRTL+val+PDF_closeRTL;
      }
      return val;
      
    }

    @Override public int compareTo(final DataField other) { return Integer.compare(this.id, other.id); }
    @Override public int hashCode() { return Integer.hashCode( this.id ); }
    @Override public boolean equals(final Object o){
        if (this == o) return true;
        if (o == null) return false;
        if (! this.getClass().equals( o.getClass() )) return false;
        DataField other = (DataField) o;
        return Objects.equals(this.id, other.id);
    }

  }

  public static class Subfield implements Comparable<Subfield> {

    public int id;
    public Character code;
    public String value;

    @Override public int compareTo(final Subfield other) { return Integer.compare(this.id, other.id); }
    @Override public int hashCode() { return Integer.hashCode( this.id ); }
    @Override public boolean equals(final Object o){
        if (this == o) return true;
        if (o == null) return false;
        if (! this.getClass().equals( o.getClass() )) return false;
        Subfield other = (Subfield) o;
        return Objects.equals(this.id, other.id);
    }
    public Subfield( int id, char code, String value ) {
      this.id = id;
      this.code = code;
      this.value = value;
    }
  }

}
