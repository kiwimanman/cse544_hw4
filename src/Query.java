import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;
        private Connection customerConn;

	// Canned queries

	// LIKE does a case-insensitive match
	private static final String SEARCH_SQL_BEGIN =
		"SELECT * FROM movie WHERE name LIKE '%";
	private static final String SEARCH_SQL_END = 
		"%' ORDER BY id";

	private static final String DIRECTOR_MID_SQL = "SELECT y.* "
					 + "FROM movie_directors x, directors y "
					 + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement directorMidStatement;

    private static final String ACTOR_MID_SQL = "SELECT DISTINCT actor.* "
            + "FROM actor, casts "
            + "WHERE casts.mid = ? and casts.pid = actor.id";
    private PreparedStatement actorMidStatement;

    private static final String RENTAL_STATUS_STATEMENT =
        "SELECT * FROM rental WHERE status = 1 and movie_id = ?";
    private PreparedStatement rentalStatusStatement;

	private static final String CUSTOMER_LOGIN_SQL = 
		"SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement customerLoginStatement;

	private static final String BEGIN_TRANSACTION_SQL = 
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;
	

	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

    /**********************************************************/
    /* Connection code to SQL Azure. Example code below will connect to the imdb database on Azure
       IMPORTANT NOTE:  You will need to create (and connect to) your new customer database before 
       uncommenting and running the query statements in this file .
     */

	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("videostore.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("videostore.imdb_url");
		jSQLUser	   = configProps.getProperty("videostore.sqlazure_username");
		jSQLPassword = configProps.getProperty("videostore.sqlazure_password");


		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the imdb database */
		conn = DriverManager.getConnection(jSQLUrl, jSQLUser, jSQLPassword);
		conn.setAutoCommit(true); //by default automatically commit after each statement
		//   conn.setTransactionIsolation(...) */

        /* open connections to the customer database */
		customerConn = DriverManager.getConnection(configProps.getProperty("videostore.customer_url"), jSQLUser, jSQLPassword);
		customerConn.setAutoCommit(true); //by default automatically commit after each statement
		//customerConn.setTransactionIsolation(...);

	        
	}

	public void closeConnection() throws Exception {
		conn.close();
		customerConn.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);
        actorMidStatement = conn.prepareStatement(ACTOR_MID_SQL);

        rentalStatusStatement = customerConn.prepareStatement(RENTAL_STATUS_STATEMENT);
		customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
		beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);
	}


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

	public int getRemainingRentals(int cid) throws Exception {
		/* How many movies can she/he still rent?
		   You have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		return (99);
	}

	public String getCustomerName(int cid) throws Exception {
		/* Find the first and last name of the current customer. */
		return ("JoeFirstName" + " " + "JoeLastName");

	}

	public boolean isValidPlan(int planid) throws Exception {
		/* Is planid a valid plan ID?  You have to figure it out */
		return true;
	}

	public boolean isValidMovie(int mid) throws Exception {
		/* is mid a valid movie ID?  You have to figure it out */
		return true;
	}

	private int getRenterID(int mid) throws Exception {
		/* Find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
		return (77);
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */
		int cid;

		customerLoginStatement.clearParameters();
		customerLoginStatement.setString(1,name);
		customerLoginStatement.setString(2,password);
		ResultSet cid_set = customerLoginStatement.executeQuery();
		if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();
		return(cid);
	}

	public void transaction_printPersonalData(int cid) throws Exception {
		/* println the customer's personal data: name, and plan number */
	}

    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_title)
			throws Exception {
		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		/* Interpolate the movie title into the SQL string */
		String searchSql = SEARCH_SQL_BEGIN + movie_title + SEARCH_SQL_END;
		
		Statement searchStatement = conn.createStatement();
		ResultSet movie_set = searchStatement.executeQuery(searchSql);
		while (movie_set.next()) {
			int mid = movie_set.getInt(1);
			System.out.println("ID: " + mid + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3));
			/* do a dependent join with directors */
			directorMidStatement.clearParameters();
			directorMidStatement.setInt(1, mid);
			ResultSet director_set = directorMidStatement.executeQuery();
			while (director_set.next()) {
				System.out.println("\t\tDirector: " + director_set.getString(3)
						+ " " + director_set.getString(2));
			}
			director_set.close();

            /* do a dependent join with actors */
            actorMidStatement.clearParameters();
            actorMidStatement.setInt(1, mid);
            ResultSet actorSet = actorMidStatement.executeQuery();
            while (actorSet.next()) {
                System.out.println("\t\tActors: " + actorSet.getString("fname") + " " + actorSet.getString("lname"));
            }
            actorSet.close();

			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            rentalStatusStatement.clearParameters();
            rentalStatusStatement.setInt(1, mid);
            ResultSet rentalSet = rentalStatusStatement.executeQuery();
            if (rentalSet.next()) {
                if (rentalSet.getInt("customer_id") == cid) {
                    System.out.println("YOU HAVE IT");
                } else {
                    System.out.println("UNAVAILABLE");
                }
            } else {
                System.out.println("AVAILABLE");
            }
            rentalSet.close();
		}
		movie_set.close();
		System.out.println();
	}

	public void transaction_choosePlan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
	}

	public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
	}

	public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */
	}

	public void transaction_return(int cid, int mid) throws Exception {
	    /* return the movie mid by the customer cid */
	}

	public void transaction_fastSearch(int cid, String movie_title)
			throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
		   Answers are sorted by mid.
		   Then merge-joins the three answer sets */
	}


    /* Uncomment helpers below once you've got beginTransactionStatement,
       commitTransactionStatement, and rollbackTransactionStatement setup from
       prepareStatements():
    
       public void beginTransaction() throws Exception {
	    customerConn.setAutoCommit(false);
	    beginTransactionStatement.executeUpdate();	
        }

        public void commitTransaction() throws Exception {
	    commitTransactionStatement.executeUpdate();	
	    customerConn.setAutoCommit(true);
	}
        public void rollbackTransaction() throws Exception {
	    rollbackTransactionStatement.executeUpdate();
	    customerConn.setAutoCommit(true);
	    } 
    */

}
