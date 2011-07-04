import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MyJob extends Configured implements Tool {

	private static int currentUser;
	private static String timestamp;
	private static Connection conn = null;
	private static final int topN = 5;
	private static List<Movie> movies = null;
	private static List<User> users = null;
	private static List<User> similarUsers = null;
	private static List<Movie> unrratedMovies = null;
	private static MoviesAndRatings votedItemsForUser;
	private static int currentMovieID;

	public MyJob(int userId, String datetime) {
		currentUser = userId;
		timestamp = datetime;
		try {
			String driver = "com.mysql.jdbc.Driver";
			String connectionString = "jdbc:mysql://localhost:3306/MyMoviePilot";
			String username = "root";
			String password = "admin";
			conn = getConnection(driver, connectionString, username, password);
		} catch (ClassNotFoundException ex) {
			System.err.println(ex.getMessage());
		} catch (IllegalAccessException ex) {
			System.err.println(ex.getMessage());
		} catch (InstantiationException ex) {
			System.err.println(ex.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		System.out.println("close");
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// Mapper
	public static class MapClass extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			int newKey = Integer.parseInt(line[0]);
			System.out.println(newKey+" "+line[1]);
			User sim = findUserById(similarUsers, newKey);
			if (sim != null) {
				double intermed = 0;
				try {
					MoviesAndRatings marSim = getVotedItemsForUser(
							sim.getUserID(), timestamp);
					intermed = similarity(currentUser, sim.getUserID(),
							votedItemsForUser, marSim)
							* (getVoteOfUserForItem(sim.getUserID(),
									currentMovieID, marSim));
				}
				catch (Exception ex) {
					System.out.println("!>!>!>!>!>!>!>!"+ex.getMessage());
				} 
				context.write(new LongWritable(newKey), new Text(
						intermed+""));
			}
		}
	}

	// Reducer
	public static class Reduce extends
			Reducer<LongWritable, Text, LongWritable, DoubleWritable> {
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> theIterator = values.iterator();
			double sum = 0;
			while (theIterator.hasNext()) {
				sum += Double.parseDouble(theIterator.next().toString());
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		List<Recommendation> recs = new LinkedList<Recommendation>();
		try {
			movies = getMovies();
			users = getUsers();
			votedItemsForUser = getVotedItemsForUser(currentUser, timestamp);
			unrratedMovies = votedItemsForUser.getUnratedMovies();
			similarUsers = getNsimilarUsers(currentUser, timestamp);
			double k = computeNormalizer(currentUser, similarUsers);
			System.out.println(unrratedMovies.size() + " "
					+ votedItemsForUser.getMovies().size());
			for (Movie movie : unrratedMovies) {
				System.out.println("exec");
				currentMovieID = movie.getMovieId();
				double prediction = getMeanVoteForUser(currentUser,
						votedItemsForUser);
				Configuration conf = getConf();
				conf.addResource(new Path("conf/core-site.xml"));
				conf.addResource(new Path("conf/hdfs-site.xml"));
				conf.reloadConfiguration();
				Job job = new Job(conf, "MyJob");
				job.setJarByClass(MyJob.class);
				Path in = new Path("/home/dragos/userz.csv");
				Path out = new Path("/home/dragos/myOutput");
				FileInputFormat.setInputPaths(job, in);
				FileOutputFormat.setOutputPath(job, out);
				job.setMapperClass(MapClass.class);
				job.setReducerClass(Reduce.class);
				prediction += 1;
				if (prediction > 0)
					recs.add(new Recommendation(currentUser, currentMovieID,
							prediction));
				System.exit(job.waitForCompletion(true) ? 0 : 1);
				System.out.println("done?");
			}
		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyJob(1,
				"2011-07-15 20:00:00"), args);
		System.exit(res);
	}

	public static Connection getConnection(String driver,
			String connectionString, String username, String password)
			throws SQLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		Class.forName(driver).newInstance();
		return (Connection) DriverManager.getConnection(connectionString,
				username, password);
	}

	public static MoviesAndRatings getVotedItemsForUser(int userId,
			String theDay) throws SQLException {
		Statement st = conn.createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		st.setFetchSize(Integer.MIN_VALUE);
		ResultSet films = st
				.executeQuery("SELECT Movies.movieID,Movies.title,Training.value,Training.created_at FROM Movies INNER JOIN Training ON Movies.movieID=Training.movieID WHERE Training.userID="
						+ userId);
		List<Movie> allRated = new LinkedList<Movie>();
		List<Movie> allRatedAtTime = new LinkedList<Movie>();
		List<Integer> ratings = new LinkedList<Integer>();
		while (films.next()) {
			int movieId = films.getInt("movieID");
			String title = films.getString("title");
			String day = films.getString("created_at");
			allRated.add(new Movie(movieId, title));
			if (getDayOfWeek(day).equals(getDayOfWeek(theDay))) {
				allRatedAtTime.add(new Movie(movieId, title));
				ratings.add(films.getInt("value"));
			}
		}
		List<Movie> unrratedMovies = new LinkedList<Movie>();
		List<Movie> allMovies = movies;
		if (allMovies == null)
			allMovies = getMovies();
		for (Movie mo : allMovies) {
			if (!allRated.contains(mo))
				unrratedMovies.add(mo);
		}
		films.close();
		st.close();
		return new MoviesAndRatings(userId, allRatedAtTime, ratings,
				unrratedMovies);
	}

	public static List<User> getUsers() throws SQLException {
		Statement st = conn.createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		st.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = st.executeQuery("SELECT * FROM Users");
		List<User> movies = new LinkedList<User>();
		while (rs.next()) {
			int id = rs.getInt("userID");
			String name = rs.getString("name");
			movies.add(new User(id, name));
		}
		rs.close();
		st.close();
		return movies;
	}

	public static List<Movie> getMovies() throws SQLException {
		Statement st = conn.createStatement(
				java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		st.setFetchSize(Integer.MIN_VALUE);
		ResultSet rs = st.executeQuery("SELECT * FROM Movies");
		List<Movie> movies = new LinkedList<Movie>();
		while (rs.next()) {

			int movieId = rs.getInt("movieID");

			String title = rs.getString("title");
			movies.add(new Movie(movieId, title));
		}
		rs.close();
		st.close();
		return movies;
	}

	public static int getVoteOfUserForItem(int userId, int movieId,
			MoviesAndRatings mar) {
		List<Movie> votedmovies = mar.getMovies();
		List<Integer> ratings = mar.getRatings();
		for (int i = 0; i < votedmovies.size(); i++) {
			if (userId == mar.getUserId()
					&& votedmovies.get(i).getMovieId() == movieId)
				return ratings.get(i);
		}
		return -1;
	}

	public static double getMeanVoteForUser(int userId,
			MoviesAndRatings votedItemsForUser) {
		double result = 0;
		List<Movie> movies = votedItemsForUser.getMovies();
		List<Integer> ratings = votedItemsForUser.getRatings();
		for (int i = 0; i < movies.size(); i++) {
			result += ratings.get(i);
		}
		return result / ratings.size();
	}

	public static double similarity(int userId1, int userId2,
			MoviesAndRatings mar1, MoviesAndRatings mar2) throws SQLException {
		double result = 0;
		List<Movie> list1 = mar1.getMovies();
		List<Movie> list2 = mar2.getMovies();
		if (movies == null)
			movies = getMovies();
		List<Movie> commonMovies = new LinkedList<Movie>();
		for (Movie m1 : list1) {
			for (Movie m2 : list2) {
				if (m1.getMovieId() == m2.getMovieId())
					commonMovies.add(m1);
			}
		}

		double s1 = 0;
		double s2 = 0;
		double s3 = 0;
		for (Movie movie : commonMovies) {
			double v1 = getVoteOfUserForItem(userId1, movie.getMovieId(), mar1);
			double v2 = getVoteOfUserForItem(userId2, movie.getMovieId(), mar2);
			s1 += v1 * v2;

			for (Movie m1 : list1) {
				s2 += Math
						.pow(getVoteOfUserForItem(userId1, m1.getMovieId(),
								mar1), 2);
			}

			for (Movie m2 : list2) {
				s3 += Math
						.pow(getVoteOfUserForItem(userId2, m2.getMovieId(),
								mar2), 2);
			}

		}
		result = s1 / (Math.sqrt(s1) * Math.sqrt(s2));
		return result;
	}

	public static List<User> getNsimilarUsers(int userId, String theDay)
			throws SQLException {
		List<User> result = new LinkedList<User>();
		List<User> zeUsers = users;
		if (zeUsers == null)
			zeUsers = getUsers();
		List<UserAndWeight> usrwgh = new LinkedList<UserAndWeight>();
		MoviesAndRatings mar1 = getVotedItemsForUser(userId, theDay);
		for (int o = 0; o < zeUsers.size(); o++) {
			User u = zeUsers.get(o);
			if (u.getUserID() != userId) {
				MoviesAndRatings mar2 = getVotedItemsForUser(u.getUserID(),
						theDay);
				double si = similarity(userId, u.getUserID(), mar1, mar2);
				if (!Double.isNaN(si))
					usrwgh.add(new UserAndWeight(u, si));
			}
		}

		Collections.sort(usrwgh, new UserWeightComparator());
		if (usrwgh.size() >= topN) {
			for (int i = 0; i < topN; i++) {
				System.out.println(usrwgh.get(i).getUser().getUserID()
						+ " with similarity " + usrwgh.get(i).getWeight());
				result.add(usrwgh.get(i).getUser());
			}
		} else {
			for (int j = 0; j < usrwgh.size(); j++) {
				result.add(usrwgh.get(j).getUser());
			}
		}
		return result;
	}

	public static double computeNormalizer(int userId, List<User> similarUsers)
			throws SQLException {
		System.out.println("in normalizer");
		double result = 0;
		MoviesAndRatings mar1 = getVotedItemsForUser(userId, timestamp);
		for (User user : similarUsers) {
			MoviesAndRatings mar2 = getVotedItemsForUser(user.getUserID(),
					timestamp);
			double cosine = similarity(userId, user.getUserID(), mar1, mar2);
			result += Math.abs(cosine);
		}
		return (1 / result);
	}

	public static String getDayOfWeek(String datetime) {
		String[] firstSplit = datetime.split("[ ]");
		String theDate = firstSplit[0];
		String theTime = firstSplit[1];
		String[] secondSplit = theDate.split("[-]");
		int year = Integer.parseInt(secondSplit[0]);
		int monthOfYear = Integer.parseInt(secondSplit[1]);
		int dayOfMonth = Integer.parseInt(secondSplit[2]);
		String[] thirdSplit = theTime.split("[:]");
		int hourOfDay = Integer.parseInt(thirdSplit[0]);
		int minuteOfHour = Integer.parseInt(thirdSplit[1]);
		int secondOfMinute = 0;
		int millisOfSecond = 0;
		DateTimeZone zone = DateTimeZone.getDefault();
		DateTime dt = new DateTime(year, monthOfYear, dayOfMonth, hourOfDay,
				minuteOfHour, secondOfMinute, millisOfSecond, zone);
		String day = dt.dayOfWeek().getAsText();
		if (day.equals("Saturday") || day.equals("Sunday"))
			return "weekend";
		else
			return "weekday";
	}

	public static User findUserById(List<User> theUsers, int theKey) {
		if (theUsers != null) {
			for (User u : theUsers) {
				if (u.getUserID() == theKey)
					return u;
			}
		}
		return null;
	}
}
