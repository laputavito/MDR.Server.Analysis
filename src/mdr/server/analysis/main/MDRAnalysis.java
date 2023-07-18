package mdr.server.analysis.main;

import java.util.ArrayList;
import java.util.List;

import mdr.server.analysis.db.ConnectionPool;
import mdr.server.analysis.util.CommonUtil;
import mdr.server.analysis.util.Config;
import mdr.server.analysis.util.Log;
import mdr.server.analysis.util.Monitor;
import mdr.server.analysis.util.SFTPFileTransfer;
import mdr.server.analysis.util.SocketUPDClient;
import mdr.server.analysis.vo.ConnectionInfoVO;

public class MDRAnalysis {

	public static void main(String[] args) throws Exception {

		boolean isExec = false;
		String sDebug = "";
		isExec = true;
		
		if (!isExec) {
//			System.out.println("이전 배치 실행중.... 종료됨");
			Log.TraceLog("이전 배치 실행중.... - Done!!");
			System.exit(0);
		} else {
			Config.Status.isDebug = Boolean.parseBoolean(CommonUtil.getPropertiesInfo("isDebug"));
			
//			System.out.println("Aanlysis demon thread start.");
			Log.TraceLog("Aanlysis demon thread start.", "INFO");
			
			if (args.length > 0){
				if (!args[0].equals(null)){
					Config.Status.sProcDate = args[0]; 
				}
			}else{
				Config.Status.sProcDate = "";
			}
			
			ArrayList<Thread> threadList = new ArrayList<Thread>();
			List<Runnable> threads = new ArrayList<Runnable>();

			threads.add(new AnalysisDemon());

			for (Runnable th : threads) {
				Thread thread = new Thread(th);
				thread.start();
				threadList.add(thread);
			}

			for (Thread t : threadList) {
				t.join(); // 쓰레드의 처리가 끝날때까지 기다립니다.
			}

//			System.out.println("Aanlysis demon thread end.");
			Log.TraceLog("Aanlysis demon thread end.", "INFO");

		}
	}
}