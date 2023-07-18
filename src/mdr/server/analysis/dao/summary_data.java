package mdr.server.analysis.dao;

import java.io.InputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.postgresql.copy.*;
import org.postgresql.*;
import org.postgresql.core.BaseConnection;

import mdr.server.analysis.util.Config;
import mdr.server.analysis.util.Log;

/**
 * 보안 관련  DB 로그수집 DAO
 * @author JINNEY
 *
 */
public class summary_data {
	private static Connection con = null;
	private static PGConnection pgcon = null;
	private static PreparedStatement pstmt = null;
	private static int COND_COUNT = 3;
	
	public static boolean summary_data_proc(String sProcDate) throws Exception{
		try
		{
			//InputStream csvStr = null;
			byte[] CopyData = null;
			con = Config.Path.connectionPLDM.getConnection();
  		    if(con == null){
				new Exception("DB Connection Error...!!");
			}

  		    //con.setAutoCommit(false);

			CopyManager copyManager = new CopyManager((BaseConnection) con);
			int pCnt = 0;
			
			Config.Path.connectionPLDM.releaseConnection(con);
			String policy_id = "";
			String table_name = "";
			String src_system = "";
			
			StringBuffer query = new StringBuffer();
			
    		if(con == null){
				new Exception("DB Connection Error...!!");
			}
    		
    		query.append("SELECT SRC_TABLE, POLICY_ID, SRC_SYSTEM " +
    				"FROM POLICY_INFO " +
    				"WHERE lower(SRC_TABLE) != 'user_mstr' " +
    				"AND idx_indc = TRUE");
    		
			//query.append(query);
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCnt = rsmd.getColumnCount();
			List<LinkedHashMap<String, String>> list = new ArrayList<LinkedHashMap<String, String>>();
			
			while(rs.next()){
				LinkedHashMap<String, String> info = new LinkedHashMap<String, String>();
				for(int i=1;i<=columnCnt;i++){
					info.put(rsmd.getColumnName(i) , rs.getString(rsmd.getColumnName(i)));
				}
				policy_id = rs.getString("POLICY_ID");
				table_name = rs.getString("SRC_TABLE");
				src_system = rs.getString("SRC_SYSTEM");
						
//				System.out.println(table_name + " " + policy_id + " " + src_system);
				list.add(info);

    			query = new StringBuffer();

	    		if(Config.Status.sProcDate.equals("")){
	    			query.append(String.format("DELETE FROM POLICYFACT " +
	    					"WHERE EVENT_DATE >= CURRENT_DATE-1 " +
	    					"	AND EVENT_DATE < CURRENT_DATE " +
	    					"	AND POLICY_ID = '%s'", policy_id));
	    		}else{
	    			query.append(String.format("DELETE FROM POLICYFACT " +
	    					"WHERE EVENT_DATE >= TO_DATE('%s', 'YYYYMMDD') " +
	    					"   AND EVENT_DATE < TO_DATE('%s', 'YYYYMMDD')+1 " +
	    					"	AND POLICY_ID = '%s'", Config.Status.sProcDate, Config.Status.sProcDate, policy_id));
	    		}
	    		
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();

				query = new StringBuffer();

				if(Config.Status.sProcDate.equals("")){
					query.append(String.format("INSERT INTO POLICYFACT " +
							"	(SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"		POLICY_ID, EVENT_DATE, EVENT_COUNT, SLDM_EVENTDATE) " +
							"	SELECT SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"		'%s', CURRENT_DATE-1, " +
							"		sum(case when SLDM_EXTRACT_VALUE is null then 1 " +
							"			else SLDM_EXTRACT_VALUE end), " +
							"		to_date(to_char(SLDM_EVENTDATE,'YYYYMMDD'),'YYYYMMDD') " +
							"	FROM SLDM_%s " +
							"	WHERE SLDM_ORG_LOGDATE >= CURRENT_DATE-1 " +
							"	AND SLDM_ORG_LOGDATE < CURRENT_DATE " +
							"	AND SLDM_POLICY_ID = '%s' " +
							"	GROUP BY SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"			to_char(SLDM_ORG_LOGDATE, 'YYYYMMDD'), " +
							"			to_char(SLDM_EVENTDATE, 'YYYYMMDD')", 
							policy_id, table_name, policy_id));
				}
				else{
					query.append(String.format("INSERT INTO POLICYFACT " +
							"	(SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"		POLICY_ID, EVENT_DATE, EVENT_COUNT, SLDM_EVENTDATE) " +
							"	SELECT SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"		'%s', to_date('%s', 'YYYYMMDD'), " +
							"		sum(case when SLDM_EXTRACT_VALUE is null then 1 " +
							"			else SLDM_EXTRACT_VALUE end), " +
							"		to_date(to_char(SLDM_EVENTDATE,'YYYYMMDD'),'YYYYMMDD') " +
							"	FROM SLDM_%s " +
							"	WHERE SLDM_ORG_LOGDATE >= TO_DATE('%s', 'YYYYMMDD') " +
							"	AND SLDM_ORG_LOGDATE < TO_DATE('%s', 'YYYYMMDD')+1 " +
							"	AND SLDM_POLICY_ID = '%s' " +
							"	GROUP BY SLDM_EMPNO, SLDM_MAC, SLDM_IP, " +
							"			to_char(SLDM_ORG_LOGDATE, 'YYYYMMDD'), " +
							"			to_char(SLDM_EVENTDATE, 'YYYYMMDD')",
							policy_id, Config.Status.sProcDate, table_name,
							Config.Status.sProcDate, Config.Status.sProcDate, policy_id));
				}

				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();

			}

			return true;

		}catch(Exception e){
			e.printStackTrace();
			Log.TraceLog(e.toString(), "DEBUG");
			return false;
		}
	}
}
