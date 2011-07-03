


public class User {
	private int userID;
	private String name;
	public int getUserID() {
		return userID;
	}
	public String getName() {
		return name;
	}
	public User(int userID, String name) {
		super();
		this.userID = userID;
		this.name = name;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		User other = (User) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (userID != other.userID)
			return false;
		return true;
	}
	
	
}
