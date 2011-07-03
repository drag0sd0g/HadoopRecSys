


public class Recommendation {
	
	private int userID;
	private int movieID;
	private double predictedRating;
	public int getUserID() {
		return userID;
	}
	public int getMovieID() {
		return movieID;
	}
	public double getPredictedRating() {
		return predictedRating;
	}
	public Recommendation(int userID, int movieID, double predictedRating) {
		super();
		this.userID = userID;
		this.movieID = movieID;
		this.predictedRating = predictedRating;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + movieID;
		long temp;
		temp = Double.doubleToLongBits(predictedRating);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + userID;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Recommendation other = (Recommendation) obj;
		if (movieID != other.movieID)
			return false;
		if (Double.doubleToLongBits(predictedRating) != Double
				.doubleToLongBits(other.predictedRating))
			return false;
		if (userID != other.userID)
			return false;
		return true;
	}
	
	

}
