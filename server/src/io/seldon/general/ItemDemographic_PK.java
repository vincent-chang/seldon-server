package io.seldon.general;

import java.io.Serializable;

public class ItemDemographic_PK implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public int demoId;
	public long itemId;
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj!=null){
			if(obj instanceof ItemDemographic_PK){
				ItemDemographic_PK target = (ItemDemographic_PK)obj;
				if(this.demoId==target.demoId &&
						this.itemId == target.itemId){
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
		return String.format("%s-%s", demoId, itemId);
	}
}
