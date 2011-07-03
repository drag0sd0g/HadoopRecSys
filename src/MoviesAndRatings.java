

import java.util.List;

public class MoviesAndRatings {
	int userId;
	private List<Movie> movies;
	private List<Integer> ratings;
	private List<Movie> unratedMovies;
	public int getUserId() {
		return userId;
	}
	public List<Movie> getMovies() {
		return movies;
	}
	public List<Integer> getRatings() {
		return ratings;
	}
	public List<Movie> getUnratedMovies() {
		return unratedMovies;
	}
	public MoviesAndRatings(int userId, List<Movie> movies,
			List<Integer> ratings, List<Movie> unratedMovies) {
		super();
		this.userId = userId;
		this.movies = movies;
		this.ratings = ratings;
		this.unratedMovies = unratedMovies;
	}
	
	

	
}
