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
public class extract_data {
	private static Connection con = null;
	private static PGConnection pgcon = null;
	private static PreparedStatement pstmt = null;
	private static int COND_COUNT = 3;
	
	public static boolean extract_data_proc(String sProcDate) throws Exception{
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
			String default_fields = "SLDM_EMPNO,SLDM_IP,SLDM_MAC,SLDM_ORG_LOGDATE";
			String default_fields_2 = "SLDM_EMPNO,SLDM_IP,SLDM_MAC";
			String Insert_Head = "";
			String Insert_query = "";
			String copy_query = "";
			String policy_id = "";
			String table_name = "";
			String sel_fields = "";
			String stat_field = "";
			String src_system = "";
			String query_cond = "";
			String extract_term = "";
			int cond_start = 0;
			
			StringBuffer query = new StringBuffer();
			
    		if(con == null){
				new Exception("DB Connection Error...!!");
			}
    		
    		query.append("SELECT POLICY_ID, POLICY_NAME, " 
					+ "SRC_TABLE, SRC_FIELDS, STAT_FIELD, "
					+ "COND_FIELD1, COND_OPER1, COND_VALUE1, "
					+ "COND_FIELD2, COND_OPER2, COND_VALUE2, "
					+ "COND_FIELD3, COND_OPER3, COND_VALUE3, "
					+ "SRC_SYSTEM, QUERY_COND, '0' as EXTRACT_TERM "
					+ "FROM POLICY_INFO "
					+ "WHERE lower(SRC_TABLE) != 'user_mstr' "
					+ "AND idx_indc = TRUE");
    		
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
				sel_fields = rs.getString("SRC_FIELDS");
				stat_field = rs.getString("STAT_FIELD");
				src_system = rs.getString("SRC_SYSTEM");
				query_cond = rs.getString("QUERY_COND");
						
//				System.out.println(policy_id + " " + sel_fields + " " + stat_field);
				list.add(info);

    			query = new StringBuffer();

	    		if(Config.Status.sProcDate.equals("")){
	    			query.append("DELETE FROM sldm_" + table_name + " "
	    					+ "WHERE sldm_ORG_LOGDATE >= CURRENT_DATE-1 "
	    					+ "	AND sldm_ORG_LOGDATE < CURRENT_DATE"
	    					+ "	AND sldm_POLICY_ID = '" + policy_id + "'");
	    		}else{
	    			query.append("DELETE FROM sldm_" + table_name + " "
	    					+ "WHERE sldm_ORG_LOGDATE >= TO_DATE('" + Config.Status.sProcDate + "', 'YYYYMMDD') "
	    					+ "	AND sldm_ORG_LOGDATE < TO_DATE('" + Config.Status.sProcDate + "', 'YYYYMMDD')+1"
	    					+ "	AND sldm_POLICY_ID = '" + policy_id + "'");
	    		}
	    		
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();

				query = new StringBuffer();
	    		query.append(String.format("SELECT column_name " +
		                "FROM INFORMATION_SCHEMA.COLUMNS " +
		                "WHERE table_name = '%s' " +
		                "AND column_name not like 'sldm_%%'", table_name));
	    		
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());

				if(sel_fields == null) sel_fields = "";
				
				if(sel_fields.equals("")){
					ResultSet selFieldrs = pstmt.executeQuery();
					
					StringBuffer selFieldBuf = new StringBuffer();
	
					while(selFieldrs.next()){
						selFieldBuf.append(selFieldrs.getString(1) + ",");
					}
					selFieldBuf.deleteCharAt(selFieldBuf.length() - 1);
					sel_fields = selFieldBuf.toString();
				}
				
				
				query = new StringBuffer();
				
				if(stat_field == null) stat_field = "";
				
				if(stat_field.length() > 0){
					if(Config.Status.sProcDate.equals("")){
						query.append(String.format("INSERT INTO SLDM_%s " +
							"	(%s, %s, SLDM_POLICY_ID, SLDM_EXTRACT_VALUE, " +
							"	SLDM_EVENTDATE) " +
							"	SELECT %s, %s, '%s', %s::integer, " +
							"			SLDM_ORG_LOGDATE as org_eventdate " +
							"		FROM %s " +
							"		WHERE SLDM_ORG_LOGDATE >= CURRENT_DATE - 1 " +
							"		AND SLDM_ORG_LOGDATE < CURRENT_DATE",
							table_name, default_fields, sel_fields,
							default_fields, sel_fields, policy_id, stat_field, table_name));
					}
					else{
						query.append(String.format("INSERT INTO SLDM_%s " +
								"	(%s, %s, SLDM_POLICY_ID, SLDM_EXTRACT_VALUE, " +
								"	SLDM_EVENTDATE) " +
								"	SELECT %s, %s, '%s', %s::integer, " +
								"			SLDM_ORG_LOGDATE as org_eventdate " +
								"		FROM %s " +
								"		WHERE SLDM_ORG_LOGDATE " +
								"				>= TO_DATE('%s', 'YYYYMMDD') " +
								"		AND SLDM_ORG_LOGDATE " +
								"				< TO_DATE('%s', 'YYYYMMDD')+1", 
								table_name, default_fields, sel_fields, 
								default_fields, sel_fields, policy_id, stat_field, 
								table_name, Config.Status.sProcDate, Config.Status.sProcDate));
					}
				}
				else{
					if(Config.Status.sProcDate.equals("")){
						query.append(String.format("INSERT INTO SLDM_%s " +
								"	(%s, %s, SLDM_POLICY_ID, SLDM_EVENTDATE) " +
								"	SELECT %s, %s, '%s', " +
								"			SLDM_ORG_LOGDATE as org_eventdate " +
								"		FROM %s " +
								"		WHERE SLDM_ORG_LOGDATE >= CURRENT_DATE - 1 " +
								"		AND SLDM_ORG_LOGDATE < CURRENT_DATE", 
								table_name, default_fields, sel_fields,
								default_fields, sel_fields, policy_id, table_name));
					}
					else{
						query.append(String.format("INSERT INTO SLDM_%s " +
								"	(%s, %s, SLDM_POLICY_ID, SLDM_EVENTDATE) " +
								"	SELECT %s, %s, '%s',  " +
								"			SLDM_ORG_LOGDATE as org_eventdate " +
								"		FROM %s " +
								"		WHERE SLDM_ORG_LOGDATE " +
								"				>= TO_DATE('%s', 'YYYYMMDD') " +
								"		AND SLDM_ORG_LOGDATE " +
								"				< TO_DATE('%s', 'YYYYMMDD')+1", 
								table_name, default_fields, sel_fields,
								default_fields, sel_fields, policy_id,
								table_name, Config.Status.sProcDate, Config.Status.sProcDate));
					}
				}
				
				if(query_cond == null) query_cond = "";
				
				if(query_cond.length() == 0){
					for(int cond_idx=1;cond_idx<=COND_COUNT;cond_idx++){
						String cond_field = rs.getString("cond_field" + cond_idx);
						String cond_oper = rs.getString("cond_oper" + cond_idx);
						String cond_value = rs.getString("cond_value" + cond_idx);
						
						if(cond_field == null) cond_field = "";
						if(cond_oper == null) cond_oper = "";
						if(cond_value == null) cond_value = "";
						
						if (!cond_field.equals("") && !cond_oper.equals("") && !cond_value.equals("")) {
							query.append(String.format(" AND %s %s '%s'" ,  cond_field, cond_oper, cond_value));
						}
					}
				}
				else{
					query.append(String.format(" AND (%s)" ,  query_cond));
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
