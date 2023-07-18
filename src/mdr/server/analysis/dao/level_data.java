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
public class level_data {
	private static Connection con = null;
	private static PGConnection pgcon = null;
	private static PreparedStatement pstmt = null;
	private static int COND_COUNT = 3;
	
	public static boolean level_extract_data(String sProcDate) throws Exception{
		try
		{
			int QueryResult = 0;
			
			//InputStream csvStr = null;
			byte[] CopyData = null;
			con = Config.Path.connectionPLDM.getConnection();
  		    if(con == null){
				new Exception("DB Connection Error...!!");
			}

  		    //con.setAutoCommit(false);

			
			Config.Path.connectionPLDM.releaseConnection(con);

			StringBuffer query = new StringBuffer();
			
    		if(con == null){
				new Exception("DB Connection Error...!!");
				return false;
			}

   			query = new StringBuffer();
   			query.append("select fn_org_user_hier() ");
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			ResultSet rs = pstmt.executeQuery();
			
//			QueryResult = pstmt.executeUpdate();
//			System.out.println("fn_org_user_hier Proc result : ");

			return true;

		}catch(Exception e){
			e.printStackTrace();
			Log.TraceLog(e.toString(), "DEBUG");
			return false;
		}
	}

	public static boolean evaluate_lvl_data(String sProcDate) throws Exception{
		try
		{
			String tidx_rgdt_date = "";
			String temp_no = "";
			String tmac = "";
			String tip = "";
			String sec_pol_cat = "";
			int idx_count = 0;
			int idx_score = 0;
			int con_count = 0;
			int con_from = 0;
			int con_to = 0;
			int con_scor = 0;
			long stime = 0;
			long etime = 0;
		
			//InputStream csvStr = null;
			byte[] CopyData = null;
			con = Config.Path.connectionPLDM.getConnection();
  		    if(con == null){
				new Exception("DB Connection Error...!!");
			}

  		    //con.setAutoCommit(false);

			Config.Path.connectionPLDM.releaseConnection(con);
			String policy_id = "";
			String table_name = "";
			String src_system = "";
			String sec_pol_id = "";
			String sec_sol_id = "";
			
			StringBuffer query = new StringBuffer();
			
    		if(con == null){
				new Exception("DB Connection Error...!!");
			}
    		
    		stime = System.currentTimeMillis();
   		
    		if(Config.Status.sProcDate.equals("")){
    			query.append("DELETE FROM user_idx_info " +
    					"WHERE idx_rgdt_date = TO_CHAR(CURRENT_DATE-1, 'YYYYMMDD')");
    		}else{
    			query.append(String.format("DELETE FROM user_idx_info " +
    					"WHERE idx_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			query = new StringBuffer();
			
    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("DELETE FROM user_idx_info_day " +
    					"WHERE idx_rgdt_date = TO_CHAR(CURRENT_DATE-1, 'YYYYMMDD')");
    		}else{
    			query.append(String.format("DELETE FROM user_idx_info_day " +
    					"WHERE idx_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
    		
			query = new StringBuffer();
			
    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("DELETE FROM org_sum_info " +
    					"WHERE sum_rgdt_date = TO_CHAR(CURRENT_DATE-1, 'YYYYMMDD')");
    		}else{
    			query.append(String.format("DELETE FROM org_sum_info " +
    					"WHERE sum_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();
			
			query = new StringBuffer();
			
    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("DELETE FROM total_sum_info " +
    					"WHERE sum_rgdt_date = TO_CHAR(CURRENT_DATE-1, 'YYYYMMDD')");
    		}else{
    			query.append(String.format("DELETE FROM total_sum_info " +
    					"WHERE sum_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();

			query = new StringBuffer();
			
    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("DELETE FROM pol_sum_info " +
    					"WHERE sum_rgdt_date = TO_CHAR(CURRENT_DATE-1, 'YYYYMMDD')");
    		}else{
    			query.append(String.format("DELETE FROM pol_sum_info " +
    					"WHERE sum_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();

			etime = System.currentTimeMillis();
			Log.TraceLog("STEP #1 Evaluate data delete : " + (etime - stime)/1000 + " Sec", "DEBUG");
			
			query = new StringBuffer();

    		query.append("SELECT sec_pol_id, sec_sol_id, sec_pol_sql, " +
    				"	sec_pol_patn_con, sec_pol_cat " +
    				"FROM pol_idx WHERE use_indc='Y' AND buseo_indc != 'Y'");
    		
			//query.append(query);
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()){
				sec_pol_id = rs.getString(1);
				sec_sol_id = rs.getString(2);
				
				if(sec_pol_id.equals("") || sec_sol_id.equals("")) continue;
				
				query = new StringBuffer();

				if(Config.Status.sProcDate.equals("")){
					query.append(String.format("INSERT INTO user_idx_info( " +
							"idx_rgdt_date, emp_no, mac, pol_idx_id, ip, count, " +
							"rgdt_date, updt_date, org_eventdate) " +
						"SELECT to_char(current_date-1,'YYYYMMDD'), a.sldm_empno, " +
							"'', a.policy_id, '', sum(a.event_count), now(), now(), " +
							"to_date(to_char(max(a.sldm_eventdate), 'YYYYMMDD'),'YYYYMMDD') " +
						"FROM policyfact a, org_user b " +
						"WHERE a.POLICY_ID = '%s' " +
						"AND a.sldm_empno = b.emp_no " +
						"AND b.stat = '1' " +
						"AND a.event_date >= CURRENT_DATE-1 " +
						"AND a.event_date < CURRENT_DATE " +
						"GROUP BY a.sldm_empno, a.policy_id",
						sec_pol_id));
				}
				else{
					query.append(String.format("INSERT INTO user_idx_info( " +
							"idx_rgdt_date, emp_no, mac, pol_idx_id, ip, count, " +
							"rgdt_date, updt_date, org_eventdate) " +
						"SELECT '%s', a.sldm_empno, " +
							"'', a.policy_id, '', sum(a.event_count), now(), now(), " +
							"to_date(to_char(max(a.sldm_eventdate), 'YYYYMMDD'),'YYYYMMDD') " +
						"FROM policyfact a, org_user b " +
						"WHERE a.POLICY_ID = '%s' " +
						"AND a.sldm_empno = b.emp_no " +
						"AND b.stat = '1' " +
						"AND a.event_date >= TO_DATE('%s', 'YYYYMMDD') " +
						"AND a.event_date < TO_DATE('%s', 'YYYYMMDD')+1 " +
						"GROUP BY a.sldm_empno, a.policy_id",
						Config.Status.sProcDate, sec_pol_id, Config.Status.sProcDate, Config.Status.sProcDate));
				}

				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();
				
			}

			etime = System.currentTimeMillis();
			Log.TraceLog("STEP #2 User Idx info Insert : " + (etime - stime)/1000 + " Sec", "DEBUG");
			stime = System.currentTimeMillis();
			
			query = new StringBuffer();

    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("SELECT emp_no, mac, count, pol_idx_id, idx_rgdt_date " +
    					 "FROM user_idx_info " +
    					 "WHERE idx_rgdt_date = to_char(current_date-1, 'YYYYMMDD') ");
    		}else{
    			query.append(String.format("SELECT emp_no, mac, count, pol_idx_id, idx_rgdt_date " +
    					 "FROM user_idx_info " +
    					 "WHERE idx_rgdt_date = '%s'", Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			ResultSet use_idx_rs = pstmt.executeQuery();

			while(use_idx_rs.next()){
				sec_pol_id = use_idx_rs.getString(4);
				tidx_rgdt_date = use_idx_rs.getString(5);
				temp_no = use_idx_rs.getString(1);
				tmac = use_idx_rs.getString(2);
				idx_count = Integer.parseInt(use_idx_rs.getString(3));
				idx_score = 0;
				con_count = 0;
				con_from = 0;
				con_to = 0;
				con_scor = 0;
				
				query = new StringBuffer();

				query.append(String.format("SELECT a.sec_pol_id, a.sec_sol_id, " +
						"a.sec_pol_patn_con, a.sec_pol_cat, " +
						"b.con_cnt, b.con_from_1, b.con_to_1, b.con_scor_1, b.con_from_2, " +
						"b.con_to_2, b.con_scor_2, b.con_from_3, b.con_to_3, " +
						"b.con_scor_3, b.con_from_4, " +
						"b.con_to_4, b.con_scor_4, b.con_from_5, b.con_to_5, b.con_scor_5 " +
						"FROM pol_idx a " +
						"join pol_con b " +
						"on a.sec_pol_id = b.sec_pol_id " +
						"WHERE a.sec_pol_id = '%s' ", sec_pol_id));
    			
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				
				ResultSet pol_con_rs = pstmt.executeQuery();
				
				while(pol_con_rs.next()){
					sec_pol_cat = pol_con_rs.getString("sec_pol_cat");
					con_count = pol_con_rs.getInt("con_cnt");
					
					if(sec_pol_cat.equals("01")){
						for(int i = 1; i<=con_count; i++){
							con_from = pol_con_rs.getInt("con_from_"+i);
							if(pol_con_rs.getString("con_to_"+i).equals("")) con_to = -1;
							else con_to = pol_con_rs.getInt("con_to_"+i);
							con_scor = pol_con_rs.getInt("con_scor_"+i);
							if(con_from > con_to) con_to = -1;
							
							if(con_to == -1){
								if(idx_count >= con_from){
									idx_score = con_scor;
									break;
								}
							}else{
								if(idx_count>=con_from && idx_count<=con_to){
									idx_score = con_scor;
									break;
								}
							}
						}
					}else{
						if(sec_pol_cat.equals("02")){
//							for(int i = 1; i<con_count; i++){
//								con_from = pol_con_rs.getInt("con_from_"+i);
//								int j = i + 1;
//								int con_from2 = pol_con_rs.getInt("con_from_"+j);
//								con_scor = pol_con_rs.getInt("con_scor_"+i);
//								
//								if(idx_count >= con_from)
//								{
//									idx_score = con_scor;
//									if(con_from > con_from2)
//										break;
//								}
//							}
							int i = 1;
							con_from = pol_con_rs.getInt("con_from_"+i);
							int j = i + 1;
							int con_from2 = pol_con_rs.getInt("con_from_"+j);
							con_scor = pol_con_rs.getInt("con_scor_"+i);
							
							if(idx_count >= con_from)
							{
								idx_score = con_scor;
							}
							else
							{
								idx_score = 100;
							}
						}else{
							for(int i = 1; i<=con_count; i++){
								con_from = pol_con_rs.getInt("con_from_"+i);
								con_scor = pol_con_rs.getInt("con_scor_"+i);
								
								if(idx_count == con_from)
								{
									idx_score = con_scor;
									break;
								}
							}
						}
					}

					break;
				}
				
				query = new StringBuffer();
				
    			query.append(String.format("UPDATE user_idx_info " +
    					"SET score=%d " +
    					"WHERE idx_rgdt_date='%s' and emp_no='%s' " +
    					"and pol_idx_id='%s' ",
    					idx_score, tidx_rgdt_date, temp_no, sec_pol_id));
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();
			}

			etime = System.currentTimeMillis();
			Log.TraceLog("STEP #3 User Idx Info Update :  " + (etime - stime)/1000 + " Sec", "DEBUG");
			stime = System.currentTimeMillis();

			query = new StringBuffer();

    		if(Config.Status.sProcDate.equals("")){ 
    			query.append("INSERT INTO user_idx_info( " +
    					"idx_rgdt_date, emp_no, mac, pol_idx_id, ip, count, " +
    					"rgdt_date, updt_date, org_eventdate, score) " +
    				"SELECT to_char(current_date-1,'YYYYMMDD'), aaa.emp_no, " +
    					"'', aaa.sec_pol_id, '', 0, now(), now(), " +
    					"current_date-1, 100 " +
    				"FROM " +
    					"(SELECT a.emp_no, c.sec_pol_id " +
    					"FROM org_user a, org_group b, pol_idx c " +
    					"WHERE a.org_code = b.org_code " +
    					"AND a.stat = '1' " +
    					"AND b.use_indc = '1' " +
    					"AND c.use_indc = 'Y' " +
    					"AND c.buseo_indc != 'Y' " +
    					"EXCEPT " +
    					"SELECT sldm_empno, policy_id " +
    					"FROM policyfact " +
    					"WHERE event_date >= current_date-1 " +
    					"AND event_date < current_date " +
    					"GROUP BY sldm_empno, policy_id) aaa");
    		}else{
    			query.append(String.format("INSERT INTO user_idx_info( " +
    					"idx_rgdt_date, emp_no, mac, pol_idx_id, ip, count, " +
    					"rgdt_date, updt_date, org_eventdate, score) " +
    				"SELECT '%s', aaa.emp_no, " +
    					"'', aaa.sec_pol_id, '', 0, now(), now(), " +
    					"to_date('%s', 'YYYYMMDD'), 100 " +
    				"FROM " +
    					"(SELECT a.emp_no, c.sec_pol_id " +
    					"FROM org_user a, org_group b, pol_idx c " +
    					"WHERE a.org_code = b.org_code " +
    					"AND a.stat = '1' " +
    					"AND b.use_indc = '1' " +
    					"AND c.use_indc = 'Y' " +
    					"AND c.buseo_indc != 'Y' " +
    					"EXCEPT " +
    					"SELECT sldm_empno, policy_id " +
    					"FROM policyfact " +
    					"WHERE event_date >=  TO_DATE('%s', 'YYYYMMDD') " +
    					"AND event_date <  TO_DATE('%s', 'YYYYMMDD')+1 " +
    					"GROUP BY sldm_empno, policy_id) aaa",
    					Config.Status.sProcDate, Config.Status.sProcDate, Config.Status.sProcDate, Config.Status.sProcDate));
    		}
    		
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			pstmt.executeUpdate();

			etime = System.currentTimeMillis();
			Log.TraceLog("STEP #4 User Idx Info Exception proc : " + (etime - stime)/1000 + " Sec", "DEBUG");
			stime = System.currentTimeMillis();

			int tsec_pol_cnt = 0;
			int tpol_weight = 0;
			int row_cnt_pol = 0;
			float tsum_weight = 0;
			float tpol_weight_sum = 0;
			float tmp_mod_sum = 0;
			float tmp_mod_weight = 0;
			float tot_weight = 0;

			query = new StringBuffer();

			query.append("SELECT count(sec_pol_id), sum(pol_weight_value) " +
					"FROM pol_idx WHERE use_indc='Y' AND buseo_indc != 'Y'");
			
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			ResultSet tot_rs = pstmt.executeQuery();
			
			while(tot_rs.next()){
				tsec_pol_cnt = tot_rs.getInt(1);
				tpol_weight = tot_rs.getInt(2);
				break;
			}

			query = new StringBuffer();

			query.append("SELECT a.emp_no, '', '' " +
					"FROM org_user a, org_group b " +
					"WHERE a.org_code = b.org_code " +
					"AND a.stat = '1' " +
					"AND b.use_indc = '1'");
			
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			tot_rs = pstmt.executeQuery();
			
			while(tot_rs.next()){
				temp_no = tot_rs.getString(1);
				tmac = tot_rs.getString(2);
				tip = tot_rs.getString(3);
				
				if(temp_no.equals("admin")) continue;
				
				query = new StringBuffer();

	    		if(Config.Status.sProcDate.equals("")){ 
	    			query.append(String.format("SELECT  a.score, b.pol_weight_value, " +
						"((b.pol_weight_value*1.0)/(%d*1.0))*100.0*(a.score*1.0)/100.0 as weight " +
						"FROM user_idx_info a " +
						"JOIN pol_idx b " +
						"ON a.pol_idx_id = b.sec_pol_id " +
						"WHERE a.idx_rgdt_date = to_char(current_date-1, 'YYYYMMDD') " +
						"AND a.emp_no = '%s' ",
						tpol_weight, temp_no));
	    		}else{
	    			query.append(String.format("SELECT  a.score, b.pol_weight_value, " +
	    					"((b.pol_weight_value*1.0)/(%d*1.0))*100.0*(a.score*1.0)/100.0 as weight " +
	    					"FROM user_idx_info a " +
	    					"JOIN pol_idx b " +
	    					"ON a.pol_idx_id = b.sec_pol_id " +
	    					"WHERE a.idx_rgdt_date = '%s' " +
	    					"AND a.emp_no = '%s' ",
	    						tpol_weight, Config.Status.sProcDate, temp_no));
	    		}
	    		
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				
				ResultSet user_pol_rs = pstmt.executeQuery();
				
				tpol_weight_sum = 0;
				tsum_weight = 0;
				
				while(user_pol_rs.next()){
		            tpol_weight_sum = tpol_weight_sum + user_pol_rs.getFloat(2);
		            tsum_weight = tsum_weight + user_pol_rs.getFloat(3);
				}
				
		        tmp_mod_sum = tpol_weight - tpol_weight_sum;
		        tmp_mod_weight = tmp_mod_sum / (float) tpol_weight * 100;
		        tot_weight = tsum_weight + tmp_mod_weight;
		        
		        double floor_round4 = Math.floor(Math.round((double)tot_weight*1000.0)/1000.0);
		        
				query = new StringBuffer();

				if(Config.Status.sProcDate.equals("")){ 
	    			query.append(String.format("INSERT INTO user_idx_info_day( " +
	    					"idx_rgdt_date, emp_no, mac, ip, score, scor_curr, " +
	    					"rgdt_date, updt_date, rat_score) " +
	    					"VALUES (to_char(current_date-1, 'YYYYMMDD'), '%s', '%s', " +
	    					"'%s', %f, %f, now(), now(), %f)",
	    					temp_no, tmac, tip, floor_round4, floor_round4,	tot_weight));
	    		}else{
	    			query.append(String.format("INSERT INTO user_idx_info_day( " +
	    					"idx_rgdt_date, emp_no, mac, ip, score, scor_curr, " +
	    					"rgdt_date, updt_date, rat_score) " +
	    					"VALUES ('%s', '%s', '%s', " +
	    					"'%s', %f, %f, now(), now(), %f)",
	    					Config.Status.sProcDate, temp_no, tmac, tip, floor_round4, floor_round4, tot_weight));
	    		}
				
				pstmt = (PreparedStatement) con.prepareStatement(query.toString());
				pstmt.executeUpdate();
			}

			etime = System.currentTimeMillis();
			Log.TraceLog("STEP #5 User Idx Info Day Insert : " + (etime - stime)/1000 + " Sec", "DEBUG");

			return true;

		}catch(Exception e){
			e.printStackTrace();
			Log.TraceLog(e.toString(), "DEBUG");
			return false;
		}
	}

	public static boolean level_summary_data(String sProcDate) throws Exception{
		try
		{
			//InputStream csvStr = null;
			int torg_level = 0;
			byte[] CopyData = null;
			int level_count = 0;
			
			con = Config.Path.connectionPLDM.getConnection();
  		    if(con == null){
				new Exception("DB Connection Error...!!");
			}

			Config.Path.connectionPLDM.releaseConnection(con);

			StringBuffer query = new StringBuffer();
			
    		query.append("SELECT max(org_level) FROM org_user_hier");
    		
			//query.append(query);
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			ResultSet rs = pstmt.executeQuery();
			
			level_count = 0;
			//if(rs.get.getRow() == 0) return false;
			
			while(rs.next()){

				level_count = Integer.parseInt(rs.getString(1));
    			for(int org_cnt = 0;org_cnt<level_count+1; org_cnt++){
    				torg_level = level_count - org_cnt;
//    				System.out.println("org_level : " + torg_level);
        			query = new StringBuffer();
    				if(Config.Status.sProcDate.equals("")){
		    			query.append(String.format("SELECT a.org_code_%d, a.org_nm_%d, sum(b.rat_score), " +
		    					"count(b.emp_no), " +
		    					"sum(b.rat_score) / (count(b.emp_no)*1.0) " +
		    				"FROM org_user_hier a " +
		    				"JOIN user_idx_info_day b " +
		    				"ON a.emp_no = b.emp_no " +
		    				"WHERE a.org_code_%d is not null " +
		    				"AND b.idx_rgdt_date = to_char(current_date-1, 'YYYYMMDD') " +
		    				"GROUP BY a.org_code_%d, a.org_nm_%d ", torg_level, torg_level,
		    				torg_level, torg_level, torg_level));
		    		}else{
		    			query.append(String.format("SELECT a.org_code_%d, a.org_nm_%d, sum(b.rat_score), " +
		    					"count(b.emp_no), " +
		    					"sum(b.rat_score) / (count(b.emp_no)*1.0) " +
		    				"FROM org_user_hier a " +
		    				"JOIN user_idx_info_day b " +
		    				"ON a.emp_no = b.emp_no " +
		    				"WHERE a.org_code_%d is not null " +
		    				"AND b.idx_rgdt_date = '%s' " +
		    				"GROUP BY a.org_code_%d, a.org_nm_%d ", torg_level, torg_level,
		    				torg_level, Config.Status.sProcDate, torg_level, torg_level));
		    		}
	
					pstmt = (PreparedStatement) con.prepareStatement(query.toString());
	
					ResultSet fact_rs = pstmt.executeQuery();
					
					double floor_round4 = 0.0;
					
					while(fact_rs.next()){
						
						query = new StringBuffer();
						
						floor_round4 = Math.floor(Math.round(Double.parseDouble(fact_rs.getString(5))*1000.0)/1000.0);
						if(Config.Status.sProcDate.equals("")){
							query.append(String.format("INSERT INTO org_sum_info( " +
								"sum_rgdt_date, org_code,org_nm, tot_score, tot_emp, avg, " +
								"rat_tot_score, rat_avg) " +
								"VALUES (to_char(current_date-1,'YYYYMMDD'),'%s','%s', %s, " +
								"%s, %f, %s, %s)",fact_rs.getString(1),
								fact_rs.getString(2),
								fact_rs.getString(3),
								fact_rs.getString(4),
								floor_round4,
								fact_rs.getString(3),
								fact_rs.getString(5)));
						}
						else{
							query.append(String.format("INSERT INTO org_sum_info( " +
									"sum_rgdt_date, org_code,org_nm, tot_score, tot_emp, avg, " +
									"rat_tot_score, rat_avg) " +
									"VALUES ('%s','%s','%s', %s, " +
									"%s, %f, %s, %s)",Config.Status.sProcDate, fact_rs.getString(1),
									fact_rs.getString(2),
									fact_rs.getString(3),
									fact_rs.getString(4),
									floor_round4,
									fact_rs.getString(3),
									fact_rs.getString(5)));
						}
						
						pstmt = (PreparedStatement) con.prepareStatement(query.toString());
						pstmt.executeUpdate();
						
						query = new StringBuffer();
						
						if(Config.Status.sProcDate.equals("")){
							query.append(String.format("INSERT INTO total_sum_info( " +
									"sum_rgdt_date, org_code,org_nm, tot_emp, avg, " +
									"rat_avg) " +
									"VALUES (to_char(current_date-1,'YYYYMMDD'),'%s','%s', " +
									"%s, %f, %s)",fact_rs.getString(1),
									fact_rs.getString(2),
									fact_rs.getString(4),
									floor_round4,
									fact_rs.getString(5)));
							}
							else{
								query.append(String.format("INSERT INTO total_sum_info( " +
										"sum_rgdt_date, org_code,org_nm, tot_emp, avg, " +
										"rat_avg) " +
										"VALUES ('%s','%s','%s', " +
										"%s, %f, %s)",Config.Status.sProcDate, fact_rs.getString(1),
										fact_rs.getString(2),
										fact_rs.getString(4),
										floor_round4,
										fact_rs.getString(5)));
						}
						
						pstmt = (PreparedStatement) con.prepareStatement(query.toString());
						pstmt.executeUpdate();
					}
				}
			}
			
			query = new StringBuffer();
			
    		query.append("SELECT max(org_level) FROM org_user_hier");
    		
			//query.append(query);
			pstmt = (PreparedStatement) con.prepareStatement(query.toString());
			
			rs = pstmt.executeQuery();
			
			level_count = 0;
			//if(rs.get.getRow() == 0) return false;
			
			while(rs.next()){
				
				level_count = Integer.parseInt(rs.getString(1));
				
				for(int org_cnt = 0;org_cnt<level_count+1; org_cnt++){
					torg_level = level_count - org_cnt;
	
							
//					System.out.println("org_level : " + torg_level);
	
	    			query = new StringBuffer();
	
		    		if(Config.Status.sProcDate.equals("")){
		    			query.append(String.format("select a.org_code_%d,a.org_nm_%d, b.pol_idx_id, " +
		    					"count(b.emp_no),  " +
		    					"sum(score), sum(b.count), " +
		    					"(select count(*) from org_user_hier " +
		    						"where org_code_%d = a.org_code_%d), " +
		    					"count(distinct b.emp_no) " +
		    				"from org_user_hier a " +
		    				"join user_idx_info b " +
		    				"on a.emp_no = b.emp_no " +
		    				"where a.org_code_%d is not null " +
		    				"and b.idx_rgdt_date = to_char(current_date-1, 'YYYYMMDD') " +
		    				"group by a.org_code_%d, a.org_nm_%d, b.pol_idx_id ",
		    				torg_level, torg_level,
		    				torg_level, torg_level,
		    				torg_level, torg_level, torg_level));
		    		}else{
		    			query.append(String.format("select a.org_code_%d,a.org_nm_%d, b.pol_idx_id, " +
		    					"count(b.emp_no),  " +
		    					"sum(score), sum(b.count), " +
		    					"(select count(*) from org_user_hier " +
		    						"where org_code_%d = a.org_code_%d), " +
		    					"count(distinct b.emp_no) " +
		    				"from org_user_hier a " +
		    				"join user_idx_info b " +
		    				"on a.emp_no = b.emp_no " +
		    				"where a.org_code_%d is not null " +
		    				"and b.idx_rgdt_date = '%s' " +
		    				"group by a.org_code_%d, a.org_nm_%d, b.pol_idx_id ",
		    				torg_level, torg_level,
		    				torg_level, torg_level, torg_level,
		    				Config.Status.sProcDate, torg_level, torg_level));
		    		}
	
					pstmt = (PreparedStatement) con.prepareStatement(query.toString());
	
					ResultSet fact_rs = pstmt.executeQuery();
					
					double floor_round4 = 0.0;
					
					while(fact_rs.next()){
						int tot_score = 0;
						float pol_avg = 0;
						int event_emp = 0;
						int org_emp = 0;
						int distinct_emp= 0;
						
						event_emp = fact_rs.getInt(4);
						org_emp = fact_rs.getInt(7);
						distinct_emp = fact_rs.getInt(8);
						
						if(distinct_emp < org_emp){
							tot_score = fact_rs.getInt(5) + (org_emp-distinct_emp) * 100;
							pol_avg = (float)tot_score / (float)(event_emp+org_emp-distinct_emp);
						}else{
							tot_score = fact_rs.getInt(5);
							pol_avg = (float)tot_score/(float)(event_emp);
						}
						
						floor_round4 = Math.floor(Math.round((pol_avg*1000.0)/1000.0));
						
						query = new StringBuffer();
						
						if(Config.Status.sProcDate.equals("")){
							query.append(String.format("INSERT INTO pol_sum_info( " +
									"sum_rgdt_date, org_code, org_nm, pol_idx_id, " +
									"tot_emp, tot_score, tot_count, tot_org_emp, avg, " +
									"rat_avg) " +
									"VALUES (to_char(current_date-1,'YYYYMMDD'), '%s', '%s', " +
									"'%s', %s, " +
									"%d, %s, %s, %f, %f)",
									fact_rs.getString(1),
									fact_rs.getString(2),
									fact_rs.getString(3),
									fact_rs.getString(4),
									tot_score,
									fact_rs.getString(6),
									fact_rs.getString(7),
									floor_round4, pol_avg));
						}
						else{
							query.append(String.format("INSERT INTO pol_sum_info( " +
									"sum_rgdt_date, org_code, org_nm, pol_idx_id, "+
									"tot_emp, tot_score, tot_count, tot_org_emp, avg, " +
									"rat_avg) " +
									"VALUES ('%s', '%s', '%s', " +
									"'%s', %s, " +
									"%d, %s, %s, %f, %f)", 
									Config.Status.sProcDate,
									fact_rs.getString(1),
									fact_rs.getString(2),
									fact_rs.getString(3),
									fact_rs.getString(4),
									tot_score,
									fact_rs.getString(6),
									fact_rs.getString(7),
									floor_round4, pol_avg));
						}
						
						pstmt = (PreparedStatement) con.prepareStatement(query.toString());
						pstmt.executeUpdate();
					}
				}
			}
			return true;

		}catch(Exception e){
			e.printStackTrace();
			Log.TraceLog(e.toString(), "DEBUG");
			return false;
		}
	}

}
