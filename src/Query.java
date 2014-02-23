/**
 * Keith Stone
 * CSE 544 Winter 2014
 * Homework #4
 */
import java.sql.*;
import java.util.*;

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
	private static final String SEARCH_SQL =
		"SELECT * FROM movie WHERE name LIKE ? ORDER BY id DESC";
    private PreparedStatement movieSearchStatement;

	private static final String DIRECTOR_MID_SQL = "SELECT y.* "
					 + "FROM movie_directors x, directors y "
					 + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement directorMidStatement;

    private static final String DIRECTOR_JOIN_START = "SELECT DISTINCT movie.id, directors.* " +
            "FROM directors " +
            "JOIN movie_directors on directors.id = movie_directors.did " +
            "JOIN movie on movie_directors.mid = movie.id " +
            "WHERE movie.id IN (";
    private static final String DIRECTOR_JOIN_END = ") ORDER BY movie.id DESC";

    private static final String ACTOR_MID_SQL = "SELECT DISTINCT actor.* "
            + "FROM actor, casts "
            + "WHERE casts.mid = ? and casts.pid = actor.id";
    private PreparedStatement actorMidStatement;

    private static final String ACTOR_JOIN_START = "SELECT DISTINCT movie.id, actor.* " +
            "FROM movie " +
            "JOIN casts on casts.mid = movie.id " +
            "JOIN actor on casts.pid = actor.id " +
            "WHERE movie.id in (";
    private static final String ACTOR_JOIN_END = ") ORDER BY movie.id DESC";

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

    private static final String RENTAL_OPEN = "1";
    private static final String RENTAL_CLOSED = "0";

    private static final String LIST_PLANS_SQL = "SELECT * FROM rentalplan ORDER BY monthly_fee ASC";
    private PreparedStatement listPlansStatement;

    private static final String UPDATE_PLAN_SQL =
            "UPDATE customer " +
            "SET plan_id = ? " +
            "WHERE customer.id = ?";
    private PreparedStatement updatePlanStatement;

    private static final String CHECK_PLAN_SQL =
            "SELECT * FROM RentalPlan WHERE id = ?";
    private PreparedStatement checkPlanStatement;

    private static final String PERSONAL_DATA_BEGIN_SQL =
            "SELECT customer.id, fname, lname, count(rental.movie_id) AS rental_count, maximum_rentals " +
            "FROM customer " +
            "LEFT JOIN rental ON customer.id = rental.customer_id AND status = " + RENTAL_OPEN + " " +
            "JOIN rentalplan ON customer.plan_id = rentalplan.id " +
            "WHERE customer.id = ";
    private static final String PERSONAL_DATA_END_SQL =
            " GROUP BY customer.id, fname, lname, maximum_rentals";

    private static final String VALID_MOVIE_SQL =
            "SELECT * FROM Movie where movie.id = ?";
    private  PreparedStatement validMovieStatement;

    private static final String RENT_MOVIE_SQL =
            "INSERT INTO Rental (customer_id, movie_id) VALUES (";

	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

    /**********************************************************/

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
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        /* open connections to the customer database */
		customerConn = DriverManager.getConnection(configProps.getProperty("videostore.customer_url"), jSQLUser, jSQLPassword);
		customerConn.setAutoCommit(true); //by default automatically commit after each statement
		customerConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

	        
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

        movieSearchStatement = conn.prepareStatement(SEARCH_SQL);
		directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);
        actorMidStatement = conn.prepareStatement(ACTOR_MID_SQL);
        validMovieStatement = conn.prepareStatement(VALID_MOVIE_SQL);

        rentalStatusStatement = customerConn.prepareStatement(RENTAL_STATUS_STATEMENT);
		customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
		beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);
        listPlansStatement = customerConn.prepareStatement(LIST_PLANS_SQL);
        updatePlanStatement = customerConn.prepareStatement(UPDATE_PLAN_SQL);
        checkPlanStatement = customerConn.prepareStatement(CHECK_PLAN_SQL);
	}


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

	public int getRemainingRentals(int cid) throws Exception {
		/* How many movies can she/he still rent?
		   You have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
        ResultSet customerSet = customerConn.createStatement().executeQuery(PERSONAL_DATA_BEGIN_SQL + Integer.toString(cid) + PERSONAL_DATA_END_SQL);

        if (customerSet.next()) {
            return customerSet.getInt("maximum_rentals") - customerSet.getInt("rental_count");
        } else {
            return 0;
        }
	}

	public boolean isValidPlan(int plan_id) throws Exception {
        checkPlanStatement.clearParameters();
        checkPlanStatement.setInt(1, plan_id);
        ResultSet planSet = checkPlanStatement.executeQuery();
        boolean result;
        if (planSet.next()) {
            result = true;
        } else {
            result = false;
        }
        planSet.close();
		return result;
	}

	public boolean isValidMovie(int mid) throws Exception {
		validMovieStatement.clearParameters();
        validMovieStatement.setInt(1, mid);
        ResultSet validMovieSet = validMovieStatement.executeQuery();
		boolean result = validMovieSet.next();
        validMovieSet.close();
        return result;
	}

	private void rentMovie(int mid, int cid) throws Exception {
		/* Rent the movie to a customer, transaction correctness enforced elsewhere */
        customerConn.createStatement().executeUpdate(RENT_MOVIE_SQL + Integer.toString(cid) + ',' + Integer.toString(mid) + ')');
	}

    private boolean isMultipleOpenRentals(int mid) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb
                .append("SELECT count(*) as count FROM Rental ")
                .append("WHERE status = ").append(RENTAL_OPEN).append(" ")
                .append("AND movie_id = ").append(Integer.toString(mid));

        ResultSet set = customerConn.createStatement().executeQuery(sb.toString());
        boolean result = set.next() ? set.getInt("count") > 1 : false;
        set.close();
        return result;
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
        ResultSet customerSet = customerConn.createStatement().executeQuery(PERSONAL_DATA_BEGIN_SQL + Integer.toString(cid) + PERSONAL_DATA_END_SQL);

        if (customerSet.next()) {
            StringBuilder sb = new StringBuilder();
            sb
                .append(customerSet.getString("fname"))
                .append(" ")
                .append(customerSet.getString("lname"))
                .append(": ")
                .append("Currently Renting ")
                .append(customerSet.getInt("rental_count"))
                .append(" out of ")
                .append(customerSet.getString("maximum_rentals"))
                .append(" available rentals.");
            System.out.println(sb.toString());
            System.out.println();
        }
        customerSet.close();
	}

    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_title)
			throws Exception {
		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		/* Interpolate the movie title into the SQL string */
		movieSearchStatement.clearParameters();
        movieSearchStatement.setString(1, '%' + movie_title + '%');
		ResultSet movie_set = movieSearchStatement.executeQuery();
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
	    beginTransaction();

        updatePlanStatement.clearParameters();
        updatePlanStatement.setInt(1, pid);
        updatePlanStatement.setInt(2, cid);
        updatePlanStatement.executeUpdate();

        if (isValidPlan(pid) && getRemainingRentals(cid) >= 0) {
            commitTransaction();
            System.out.println("Plan Changed.");
        } else {
            rollbackTransaction();
            System.out.println("Unable to change plan due to too many outstanding rentals.");
        }
	}

	public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
        ResultSet plans = listPlansStatement.executeQuery();
        System.out.println("\tId\tName\tMaximum Rentals\tMonthly Fee");
        while(plans.next()) {
            System.out.println(
                "\t" + Integer.toString(plans.getInt("id")) +
                "\t" + plans.getString("name") +
                "\t" + Integer.toString(plans.getInt("maximum_rentals")) +
                "\t" + Double.toString(plans.getDouble("monthly_fee"))
            );
        }
        plans.close();
	}

	public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */
        if (!isValidMovie(mid)) {
            System.out.println("Invalid movie id. Please try again.");
            return;
        }

        beginTransaction();
        rentMovie(mid, cid);

        // Enforce consistency
        if (getRemainingRentals(cid) < 0) {
            System.out.println("Unable to rent you more movies. Please upgrade your plan or return a title.");
            rollbackTransaction();
        } else if (isMultipleOpenRentals(mid)) {
            System.out.println("Someone has already got it checked out. Sorry.");
            rollbackTransaction();
        } else {
            commitTransaction();
            System.out.println("Enjoy the show!");
        }
	}

	public void transaction_return(int cid, int mid) throws Exception {
	    /* return the movie mid by the customer cid */
        StringBuilder sb = new StringBuilder();
        sb
                .append("UPDATE rental ")
                .append("SET status = ").append(RENTAL_CLOSED).append(" ")
                .append("WHERE customer_id = ").append(Integer.toString(cid)).append(" ")
                .append("AND movie_id = ").append(Integer.toString(mid));
        String query = sb.toString();
        customerConn.createStatement().executeUpdate(query);
	}

	public void transaction_fastSearch(int cid, String movie_title) throws Exception {
        String searchString = '%' + movie_title + '%';

        movieSearchStatement.clearParameters();
        movieSearchStatement.setString(1, searchString);
        ResultSet movieSet = movieSearchStatement.executeQuery();

        // This next part makes me sad but figuring how to get the typing right is annoying in Java.
        // Had to get all the data processing done in one pass of the movies but still have to do another one later...
        List<Integer> movieIds = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        Map<Integer, String> outputMap = new HashMap<>();
        while (movieSet.next()) {
            int movieId = movieSet.getInt(1);
            movieIds.add(movieId);
            sb.append(Integer.toString(movieId));
            sb.append(',');
            outputMap.put(movieId, "NAME: " + movieSet.getString(2) + " YEAR: " + movieSet.getString(3));
        }
        sb.setLength(sb.length() - 1);
        String idTuple = sb.toString();

        // These are super fast when done this way.
        ResultSet actorSet = conn.createStatement().executeQuery(ACTOR_JOIN_START + idTuple + ACTOR_JOIN_END);
        ResultSet directorSet = conn.createStatement().executeQuery(DIRECTOR_JOIN_START + idTuple + DIRECTOR_JOIN_END);

        boolean moreActors = actorSet.next();
        boolean moreDirectors = directorSet.next();

        for (int movie_id : movieIds) {
            // Print movie
            System.out.println("ID: " + movie_id + " " + outputMap.get(movie_id));

            while (moreDirectors && movie_id == directorSet.getInt(1)) {
                // Print director
                System.out.println("\t\tDirector: " + directorSet.getString(4)  + " " + directorSet.getString(3));

                if (!directorSet.next()) {
                    moreDirectors = false;
                }
            }

            while (moreActors && movie_id == actorSet.getInt(1)) {
                // Print actor
                System.out.println("\t\tActors: " + actorSet.getString("fname") + " " + actorSet.getString("lname"));

                if (!actorSet.next()) {
                    moreActors = false;
                }
            }
            System.out.println();
        }
        movieSet.close();
        directorSet.close();
        actorSet.close();
	}

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
}
