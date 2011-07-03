


public class UserAndWeight implements Comparable<UserAndWeight>{
	
	private User user;
	private double weight;
	public User getUser() {
		return user;
	}
	public double getWeight() {
		return weight;
	}
	public UserAndWeight(User user, double weight) {
		super();
		this.user = user;
		this.weight = weight;
	}
	@Override
	public int compareTo(UserAndWeight o) {
		Double w1 = new Double(this.weight);
		Double w2 = new Double(o.getWeight());
		return w1.compareTo(w2);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		long temp;
		temp = Double.doubleToLongBits(weight);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		UserAndWeight other = (UserAndWeight) obj;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		if (Double.doubleToLongBits(weight) != Double
				.doubleToLongBits(other.weight))
			return false;
		return true;
	}

	
	

}
