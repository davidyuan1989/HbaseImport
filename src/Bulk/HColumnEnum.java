package Bulk;

/**
 * HBase table columns for the 'srv' column family
 */
public enum HColumnEnum {
	SRV_COL_TXT("txt".getBytes()),
	SRV_COL_RUID("rud".getBytes()),
	SRV_COL_RID("rid".getBytes()),
	SRV_COL_RST("rst".getBytes()),
	SRV_COL_UID("uid".getBytes());

	private final byte[] columnName;
	
	public final static String UserOutput = "user/rtw/user";
	public final static String TimeOutput = "time/tw/time";

	public final static String UserTable = "UserTable";
	public final static String TimeTable = "TimeTable";
	
	
	public final static String TXT = "txt";
	public final static String HAS_RETEWEET = "has_r";
	public final static String USER_ID = "user_id";
	public final static String RETEWEET_ID = "re_id";
	public final static String REUSER_ID = "re_user_id";
	
	public final static String splitWord = "#RuCh#";
	
	public final static String splitRowKey = "|";
	
	HColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}
