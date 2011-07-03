


import java.util.Comparator;

public class UserWeightComparator implements Comparator<UserAndWeight> {

	@Override
	public int compare(UserAndWeight u1, UserAndWeight u2) {
		return u2.compareTo(u1);
	}

}
