


public class Vote {
	private int user;
	private int movie;
	private int rating;

	public int getUser() {
		return user;
	}

	public int getMovie() {
		return movie;
	}

	public int getRating() {
		return rating;
	}

	public Vote(int user, int movie, int rating) {
		super();
		this.user = user;
		this.movie = movie;
		this.rating = rating;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + movie;
		result = prime * result + rating;
		result = prime * result + user;
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
		Vote other = (Vote) obj;
		if (movie != other.movie)
			return false;
		if (rating != other.rating)
			return false;
		if (user != other.user)
			return false;
		return true;
	}
	
	

}
