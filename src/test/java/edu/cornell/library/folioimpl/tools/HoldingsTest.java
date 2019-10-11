package edu.cornell.library.folioimpl.tools;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.folioimpl.objects.Holding;
import edu.cornell.library.folioimpl.objects.OkapiClient;
import edu.cornell.library.folioimpl.tools.Holdings;

public class HoldingsTest {

	static Connection voyager = null;
	static OkapiClient okapi = null;

	@BeforeClass
	public static void connect() throws SQLException, IOException {
		Properties prop = new Properties();
		try (InputStream in = 
				Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
			prop.load(in);
		}
		String url = prop.getProperty("voyagerDBUrl");
		String user = prop.getProperty("voyagerDBUser");
		String pass = prop.getProperty("voyagerDBPass");
		voyager = DriverManager.getConnection(url,user,pass);
		okapi = new OkapiClient(prop.getProperty("okapiurl21dmgurl"), prop.getProperty("okapiurl21dmgtok"));
	}

	@Test
	public void getBibHoldings() throws SQLException, IOException, XMLStreamException {
		{
			List<Holding> holdings = Holdings.getHoldingsForBibRecord(voyager, okapi, 5140352, "inst-uuid-goes-here");
			for (Holding h : holdings)
				System.out.println(h.toString());
		}
		{
			List<Holding> holdings = Holdings.getHoldingsForBibRecord(voyager, okapi, 6070863, "inst-uuid-goes-here");
			for (Holding h : holdings)
				System.out.println(h.toString());
		}
	}

}
