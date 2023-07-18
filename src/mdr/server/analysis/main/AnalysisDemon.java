package mdr.server.analysis.main;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.Period;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import mdr.server.analysis.dao.delete_data;
import mdr.server.analysis.dao.extract_data;
import mdr.server.analysis.dao.level_data;
import mdr.server.analysis.dao.summary_data;
import mdr.server.analysis.db.ConnectionPLDM;
import mdr.server.analysis.db.ConnectionPool;
import mdr.server.analysis.util.CommonUtil;
import mdr.server.analysis.util.Config;
import mdr.server.analysis.util.DateUtil;
import mdr.server.analysis.util.Log;
import mdr.server.analysis.util.LogFileWriter;
import mdr.server.analysis.util.SFTPFileTransfer;

import java.io.Reader;
import java.nio.CharBuffer;

public class AnalysisDemon implements Runnable {
	private boolean isRun = true;
	private boolean isCollect = true;
	private String logFilePathFullName = Config.Path.LogFilePath;

	/* (비Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			Log.TraceLog("Aanlysis proc start.", "INFO");
			Config.Path.connectionPLDM = ConnectionPLDM.getPLDMInstance();
			

			while (isRun) {
					try {

						long stime = 0;
						long etime = 0;
						long sstime = 0;
						long eetime = 0;
						
						String sProcDate = Config.Status.sProcDate;
						boolean bProc = false;
 
						stime = System.currentTimeMillis();
						sstime = System.currentTimeMillis();
						bProc = extract_data.extract_data_proc(sProcDate);
						if(!bProc){
							Log.TraceLog("Extract Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Extract data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Extract data proc elapsed time : " + (etime - stime)/1000 + " Sec");

						stime = System.currentTimeMillis();
						bProc = summary_data.summary_data_proc(sProcDate);
						if(!bProc){
							Log.TraceLog("Summary Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Summary data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Summary data proc elapsed time : " + (etime - stime)/1000 + " Sec");

						stime = System.currentTimeMillis();
						bProc = level_data.level_extract_data(sProcDate);
						if(!bProc){
							Log.TraceLog("Level extract Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Level etract data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Level etract data proc elapsed time : " + (etime - stime)/1000 + " Sec");
						
						stime = System.currentTimeMillis();
						bProc = level_data.evaluate_lvl_data(sProcDate);
						if(!bProc){
							Log.TraceLog("Level evaluate Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Evaluate level data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Evaluate level data proc elapsed time : " + (etime - stime)/1000 + " Sec");
						
						stime = System.currentTimeMillis();
						bProc = level_data.level_summary_data(sProcDate);
						if(!bProc){
							Log.TraceLog("Level Summary Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Level Summary data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Level Summary data proc elapsed time : " + (etime - stime)/1000 + " Sec");
						
						stime = System.currentTimeMillis();
						bProc = delete_data.delete_data_proc();
						if(!bProc){
							Log.TraceLog("Delete Data Failed.", "INFO");
							break;
						}
						etime = System.currentTimeMillis();
						Log.TraceLog("Delete data proc elapsed time : " + (etime - stime)/1000 + " Sec", "INFO");
						System.out.println("Delete data proc elapsed time : " + (etime - stime)/1000 + " Sec");
						
						eetime = System.currentTimeMillis();
						Log.TraceLog("Total data proc elapsed time : " + (eetime - sstime)/1000 + " Sec", "INFO");
						System.out.println("Total data proc elapsed time : " + (etime - stime)/1000 + " Sec");

						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						Log.TraceLog(e.toString(), "DEBUG");
						e.printStackTrace();
						
					}finally{
						
						//sftpTransfer.close();
					}
				if(!Config.Status.sProcDate.equals("")){
					break;
				}
				int collect_interval = 1000 * Integer.parseInt(CommonUtil.getPropertiesInfo("collect_interval"));
				if(collect_interval == 0){
					break;
				}
				Thread.sleep(collect_interval);//쓰레드를 잠시 멈춤
				isRun = Boolean.parseBoolean(CommonUtil.getPropertiesInfo("isRun"));
			}
			
			System.out.println("Analysis proc end");
			Log.TraceLog("Analysis proc end.", "INFO");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			Log.TraceLog(e.getStackTrace());
			e.printStackTrace();

		}
	}
}