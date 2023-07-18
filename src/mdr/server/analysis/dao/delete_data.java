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

import mdr.server.analysis.util.CommonUtil;
import mdr.server.analysis.util.Config;
import mdr.server.analysis.util.Log;

/**
 * 보안 관련  DB 로그수집 DAO
 * @author JINNEY
 *
 */
public class delete_data {
	private static Connection con = null;
	private static PGConnection pgcon = null;
	private static PreparedStatement pstmt = null;
	private static int COND_COUNT = 3;
	
	public static boolean delete_data_proc() throws Exception{
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
			String table_name = "";
			String log_term = "";
			int cond_start = 0;
			
			log_term = CommonUtil.getPropertiesInfo("logterm");
			
			StringBuffer query = new StringBuffer();
			
    		if(con == null){
				new Exception("DB Connection Error...!!");
			}
    		
    		query.append("SELECT DISTINCT SRC_TABLE "
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
				table_name = rs.getString("SRC_TABLE");
						
//				System.out.println(policy_id + " " + sel_fields + " " + stat_field);
				list.add(info);

    			query = new StringBuffer();

    			query.append("DELETE FROM " + table_name + " "
    					+ "WHERE sldm_ORG_LOGDATE <= CURRENT_DATE - " + log_term + "; " );
	    		
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();

    			query = new StringBuffer();

    			query.append("DELETE FROM sldm_" + table_name + " "
    					+ "WHERE sldm_ORG_LOGDATE <= CURRENT_DATE - " + log_term + "; " );

				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();
				
	
			}

			query = new StringBuffer();

			query.append("DELETE FROM  POL_SUM_INFO "
					+ "WHERE RGDT_DATE <= CURRENT_DATE - " + log_term + "; " );

			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			query = new StringBuffer();

			query.append("DELETE FROM  POLICYFACT "
					+ "WHERE CREATEDATE <= CURRENT_DATE - " + log_term + "; " );

			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			query = new StringBuffer();

			query.append("DELETE FROM  USER_IDX_INFO "
					+ "WHERE RGDT_DATE <= CURRENT_DATE - " + log_term + "; " );

			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			query = new StringBuffer();

			query.append("DELETE FROM  USER_IDX_INFO_DAY "
					+ "WHERE RGDT_DATE <= CURRENT_DATE - " + log_term + "; " );

			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			return true;

		}catch(Exception e){
			e.printStackTrace();
			Log.TraceLog(e.toString(), "DEBUG");
			return false;
		}
	}
}
