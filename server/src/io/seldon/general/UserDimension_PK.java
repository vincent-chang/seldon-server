package io.seldon.general;

import java.io.Serializable;

public class UserDimension_PK implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int dimId;
	public long userId;
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj!=null){
			if(obj instanceof UserDimension_PK){
				UserDimension_PK target = (UserDimension_PK)obj;
				if(this.dimId==target.dimId &&
						this.userId == target.userId){
					return true;
				}else{
					return false;
				}
			}
		}
		return super.equals(obj);
	}
	
	@Override
	public String toString() {
		return String.format("%s-%s", dimId, userId);
	}
}
