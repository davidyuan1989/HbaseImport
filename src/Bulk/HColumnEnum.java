package Bulk;

/**
 * HBase table columns for the 'srv' column family
 */
public enum HColumnEnum {
	SRV_COL_TXT("txt".getBytes()),
	SRV_COL_RUID("r_uid".getBytes()),
	SRV_COL_RID("r_id".getBytes()),
	SRV_COL_RST("r_st".getBytes()),
	SRV_COL_UID("uid".getBytes());

	private final byte[] columnName;
	
	public final static String UserOutput = "output/user/";
	public final static String TimeOutput = "output/time/";

	HColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}
