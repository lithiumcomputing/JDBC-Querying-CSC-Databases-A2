/**
 * @author Jin Jiang Li (1002301671)
 * @date 13 March 2019
 *
 * Assignment 2, CSC343H1S Winter 2019
 * */
import java.sql.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;

public class Assignment2 extends JDBCSubmission {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.postgresql.Driver";  
    static final String DB_URL = "jdbc:postgresql://localhost:5432/csc343h-lijin19";

    //  Database credentials
    static final String USER = "lijin19";
    static final String PASS = "";

	// Database Variables
	static Connection conn = null;
	static Statement stmt = null;

	/**
	 * This program will attempt to make a connection to the CSC343 database.
	 * It will also load the data from ddl.sql.
	 *
	 * @param url Database URL.
	 * @param username Username for the database that corresponds to the url argument.
     * @param password Password for the database.
	 *
	 * @throws SQLException Database SQL exception should the connection fail.
     * */
	public boolean connectDB (String url, String username, String password) {
		try {
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();

			String sqlPreFormatted = new String(Files.readAllBytes(Paths.get("ddl.sql")));
			String sql = "";

			// Tokenize the SQL String
			StringTokenizer st = new StringTokenizer(sqlPreFormatted);
			while (st.hasMoreElements()) {
				String tok = st.nextToken();
				// remove backslashes, since JDBC cannot parse them!
				if (tok.charAt(0) == '\\') {
					tok = tok.substring(1);
				}

				sql += tok + " ";

			}

			stmt.executeUpdate(sql);
			stmt.executeUpdate("SET search_path TO parlgov;");

			return true;
		}

		catch (SQLException se) {
			return false;
		}

		catch (Exception e) {
			return false;
		}
	}

	/**
     * Disconnects the database.
	 *
	 * @throws SQLException Database SQL exception should the disconnect fail.
	 * */
	public boolean disconnectDB () {
		try {
			conn.close();
			return true;
		}

		catch (SQLException se) {
			return false;
		}
	}

	/**
	 * A method that, given a country, returns the list of elections in that
	 * country, in descending order of years, and the cabinets that have formed after that 		 
	 * election and before the next election of the same type.
	 *
	 * @param country The name of the country.
	 * */
	public ElectionCabinetResult electionSequence(String country) {
		try {
			String getAllElectionsForOneCountry = "SELECT country_id, name, election_id, e_date, e_type FROM " +
			"(SELECT country.id AS country_id, country.name FROM country)" +
			" AS cc NATURAL LEFT JOIN (SELECT id as election_id, e_date, e_type FROM election) AS ee" +
			" WHERE name = '" + country + "' ORDER BY e_date DESC;";
			PreparedStatement pstmt = conn.prepareStatement(getAllElectionsForOneCountry);
			ResultSet resultSet = pstmt.executeQuery();
			
			ArrayList<java.sql.Date> electionDates = new ArrayList<java.sql.Date>();
			ArrayList<Integer> electionYears = new ArrayList<Integer>();
			ArrayList<String> electionTypes = new ArrayList<String>();		

			ArrayList<Integer> cabinets = new ArrayList<Integer>();	

			while (resultSet.next()) {
				System.out.println(resultSet.getString(resultSet.findColumn("country_id"))
				+ " " + resultSet.getString(resultSet.findColumn("name"))
				+ " " + resultSet.getString(resultSet.findColumn("election_id"))
				+ " " + Integer.toString(resultSet.getDate(resultSet.findColumn("e_date")).getYear() + 1900)
				+ " " + resultSet.getString(resultSet.findColumn("e_type")));

				// Add to Election Years
				electionDates.add(resultSet.getDate(resultSet.findColumn("e_date")));
				electionYears.add(resultSet.getDate(resultSet.findColumn("e_date")).getYear() + 1900);
				electionTypes.add(resultSet.getString(resultSet.findColumn("e_type")));
			}

			// Get Earliest Election Year by Each Type
			java.sql.Date earliestElectionDateParl = null, earliestElectionDateEuro = null;
			for (int i = electionYears.size()-1;i >= 0;i--) {
				if (earliestElectionDateParl == null && electionTypes.get(i).equals("Parliamentary election"))
					earliestElectionDateParl = electionDates.get(i);
				if (earliestElectionDateEuro == null && electionTypes.get(i).equals("European Parliament"))
					earliestElectionDateEuro = electionDates.get(i);
			}
			//int latestElectionYear = electionYears.get(0);

			String getParlCabinetsFormed = "SELECT * FROM (SELECT country.id as country_id, country.name as" + 
			" country_name FROM country) AS cc NATURAL LEFT JOIN " + 
			"(SELECT country_id, id, start_date, name FROM cabinet) AS cabcab WHERE country_name = '" + country + "'"
			+ " AND start_date > to_date(\'" + Integer.toString(earliestElectionDateParl.getDay()) + " " + 
			Integer.toString(earliestElectionDateParl.getMonth()) +" "+ Integer.toString(earliestElectionDateParl.getYear()+1900)
			+ "\', \'DD MM YYYY\') ORDER BY start_date DESC;"; 

			ResultSet cabinetSet = conn.prepareStatement(getParlCabinetsFormed).executeQuery();

			while (cabinetSet.next()) {
				/*System.out.println(cabinetSet.getString(cabinetSet.findColumn("country_id")) + " "
				+ cabinetSet.getString(cabinetSet.findColumn("country_name")) + " "
				+ cabinetSet.getString(cabinetSet.findColumn("id")) + " "
				+ cabinetSet.getString(cabinetSet.findColumn("start_date")) + " "
				+ cabinetSet.getString(cabinetSet.findColumn("name")) + " ");*/
				cabinets.add(cabinetSet.getDate(cabinetSet.findColumn("start_date")).getYear() + 1900);
			}

			return new ElectionCabinetResult((List<Integer>)electionYears, (List<Integer>)cabinets);
		}

		catch (SQLException se) {
			return null;
		}
	}

	/**
	 * A method that, given a president, returns other presidents that have
	 * similar comments and descriptions in the database.
	 *
	 * @param politician Name of the politician or president.
	 *
	 * @return List of similar politicians.
	 * */
	public List<Integer> findSimilarPoliticians (Integer politicianId, Float threshold) {
		try {
			// Prepare & Execute SQL Query
			String queryMyPolitician = "SELECT id, description, comment FROM "
				+ "politician_president WHERE id = " + Integer.toString(politicianId)
				+ ";\n";
			String queryToFindAllOtherPoliticians = "SELECT id, description, comment FROM "
				+ "politician_president WHERE id <> " + Integer.toString(politicianId)
				+ ";\n";

			PreparedStatement pstmt = conn.prepareStatement(queryToFindAllOtherPoliticians);
			ResultSet otherPoliticians = pstmt.executeQuery();

			// Some lists to keep track of data
			ArrayList<Integer> politicianIDs = new ArrayList<Integer>();
			ArrayList<String> commentConcatDescs = new ArrayList<String>();
			ArrayList<Integer> similarPoliticians = new ArrayList<Integer>(); // by ids

			// Traverse through the queried columns to get all other politician's info.
			while (otherPoliticians.next()) {
				politicianIDs.add(otherPoliticians.getInt(otherPoliticians.findColumn("id")));
				String polComDesc = otherPoliticians.getString(otherPoliticians.findColumn("comment"))
					+ otherPoliticians.getString(otherPoliticians.findColumn("description"));
				commentConcatDescs.add(polComDesc);
			}

			// Get my politician's info
			ResultSet myPolitician = conn.prepareStatement(queryMyPolitician).executeQuery();
			Integer myId;
			String myInfo = "";
			while (myPolitician.next()) {
				myId = myPolitician.getInt(myPolitician.findColumn("id"));
				myInfo = myPolitician.getString(myPolitician.findColumn("comment"))
					+ myPolitician.getString(myPolitician.findColumn("description"));
			}

			for (int index = 0;index < politicianIDs.size();index++) {
				double jacardScore = similarity(myInfo, commentConcatDescs.get(index));
				System.out.println(jacardScore);
				if (jacardScore >= threshold) {
					similarPoliticians.add(politicianIDs.get(index));
				}
			} // end for

			return (List<Integer>)similarPoliticians;
		}

		catch (SQLException se) {
			//se.printStackTrace();
			return null;
		}
	}

	/**
     * This main method will connect to the database, do some queries, and then
	 * it will exit gracefuly.
	 *
 	 * @param args Command line arguments.
	 * */
	public static void main(String[] args) {
		// Register JDBC driver
		try {
		 	Class.forName(JDBC_DRIVER);
			Assignment2 a2 = new Assignment2();
			a2.connectDB(DB_URL,USER,PASS);

			/*ResultSet rs = conn.prepareStatement("SELECT id FROM politician_president;").executeQuery();
			
			Float threshold = (float)0.0001;
			while (rs.next()) {
				Integer pID = rs.getInt(rs.findColumn("id"));
				List<Integer> ll = a2.findSimilarPoliticians(pID, threshold);
				Object [] array = ll.toArray();
			
				if (ll.size() == 0) System.out.println("Empty result");

				else {
					for (int i = 0;i < array.length;i++)
						System.out.print(array[i] + " ");
					System.out.println();
				}
			} // end while loop*/

			a2.disconnectDB();
		}

		catch (ClassNotFoundException ce) {
			// ce.printStackTrace();
		}

		catch (SQLException se) {
			// se.printStackTrace();
		}


	   //System.out.println("Goodbye!");
	} // end main
} // end Assignment2
