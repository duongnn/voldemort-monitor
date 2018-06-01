package predicatedetectionlib.common;

public class TestForPackage {
	private int val;

	public TestForPackage(int val){
		this.val = val;
	}

	public String getMessage(){
		return new String(" TestForPackage:getMessage  val = " + val);
	}
}
