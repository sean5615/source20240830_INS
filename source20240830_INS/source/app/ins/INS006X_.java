/*
 * (INS035X_) �妸�B�z
 *-------------------------------------------------------------------------------*
 * Author    : mars
 * Modification Log :
 * Vers     Date           By             Notes
 *--------- -------------- -------------- ----------------------------------------
 * V0.0.1   106/02/23      mars           �[�c�վ�
 *--------------------------------------------------------------------------------
 */
package app.ins;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Vector;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.acer.db.DBManager;
import com.acer.db.query.DBResult;
import com.acer.log.MyLogger;
import com.acer.util.DateUtil;
import com.acer.util.Utility;
import com.nou.AbstractMedium;
import com.nou.UtilityX;
import com.nou.aut.AUTCONNECT;
import com.nou.exa.EXAEXPENSE;
import com.nou.gra.bo.ConnectionWithoutPool;
import com.nou.ins.dao.INST007DAO;
import com.nou.ins.dao.INST007GATEWAY;
import com.nou.ins.dao.INST008DAO;
import com.nou.per.bo.PERGETTUTTIME;
import com.nou.per.dao.PERT004GATEWAY;
import com.nou.per.dao.PERT055GATEWAY;
import com.nou.per.dao.PERT031GATEWAY;
import com.nou.sys.SYSGETSMSDATA;
import com.nou.sys.dao.SYST001GATEWAY;
import com.nou.tra.dao.TRAT007GATEWAY;
import com.nou.ycm.YCMAbstractJob;

/**
 * @author Administrator
 * 
 */
public class INS006X_ extends YCMAbstractJob {
	String CAL_DATE = "";// �p����

	// �bWindows ���Ҵ��դU�AMark���q
	public static void main(String[] args) throws Exception {
		MyLogger logger = new MyLogger("INS006X_");
		DBManager dbManager = new DBManager(logger);
		Connection conn = ConnectionWithoutPool.getConnection();
		// args = new String[] { "aa", "aaab" };// test
		INS006X_ job = new INS006X_(args[0], "INS006X_", dbManager, conn,
				args[1]); // args[0]��job id, "ycm001x_"���}�o�̳]�w
		System.exit(job.doJob());
	}

	// �bWindows ���Ҵ��դU�AMark���q
	public INS006X_(String job_seq, String job_code, DBManager dbManager,
			Connection conn, String USER_ID) throws Exception {
		super.YCMJob(job_seq, job_code, dbManager, conn, USER_ID);
	}

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		String runClassName = context.getJobDetail().getJobDataMap()
				.get("runClassName").toString();
		String JobId = context.getJobDetail().getJobDataMap().get("JobId")
				.toString();
		String USER_ID = context.getJobDetail().getJobDataMap().get("USER_ID")
				.toString();

		try {
			MyLogger logger = new MyLogger(runClassName);
			dbManager = new DBManager(logger);
			conn = ConnectionWithoutPool.getConnection();
			super.InitSet(JobId, runClassName, USER_ID);
			this.USER_ID = USER_ID; // by poto 20140516
			doJob();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public int doJob() throws Exception// �̧@�~�Ѽư�������@�~
	{
		try {
			ycmProgLog = getProgLogObject(dbManager, conn);
			progParameters = getProgParameter();
			ycmProgLog.logAppend("start");
			// �ŧi�ɶ�
			SimpleDateFormat dateTimeFormat = new SimpleDateFormat(
					"yyyyMMddhhmmss");
			cal = Calendar.getInstance();
			d1 = dateTimeFormat.format(cal.getTime());
			ycmProgLog.setProgExecData("0", " ", d1, " ");
			startProcess(dbManager, conn); // �D�{��
			dbManager.commit();
			cal = Calendar.getInstance();
			d2 = dateTimeFormat.format(cal.getTime());
			sysErrMsg = "���榨�\";
			ycmProgLog.setProgExecData("0", sysErrMsg, d1, d2);
			ycmProgLog.writeProgExecRslt();
			ycmProgLog.logAppend("end");
			return 0;
		} catch (Exception e) {
			dbManager.rollback();
			cal = Calendar.getInstance();
			SimpleDateFormat dateTimeFormat = new SimpleDateFormat(
					"yyyyMMddhhmmss");
			d2 = dateTimeFormat.format(cal.getTime());
			sysErrMsg = "������~: " + e.getMessage();
			ycmProgLog.setProgExecData("1", sysErrMsg, d1, d2);
			ycmProgLog.writeProgExecRslt();
			ycmProgLog.logAppend(ycmProgLog.getStackTrace(e));
			e.printStackTrace();
			return 1;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	private boolean startProcess(DBManager dbManager, Connection conn)
			throws Exception {
		ycmProgLog.logAppend("startProcess--start");

		try {
			int delete_cnt = 0;
			int cnt1 = 0;
			int cnt2 = 0;
			int cnt3 = 0;
			int cnt4 = 0;

			// ���o�ثe�Ǧ~��
			String AYEAR = Utility.checkNull(this.progParameters.get("AYEAR"),
					"");
			String SMS = Utility.checkNull(this.progParameters.get("SMS"), "");
			if (AYEAR.equals("") || SMS.equals("")) {
				SYSGETSMSDATA sys = new SYSGETSMSDATA(dbManager);
				sys.setSYS_DATE(DateUtil.getNowDate());
				sys.setSMS_TYPE("1");// 1����Ǧ~��3���U�Ǧ~��
				sys.execute();
				AYEAR = sys.getAYEAR();
				SMS = sys.getSMS();
			}
			CAL_DATE = Utility.checkNull(
					this.progParameters.get("CAL_DATE"), "");
			if (CAL_DATE.equals("")) {
				CAL_DATE = DateUtil.getNowDate();
			}
			// SMS = "1";// test
			// CAL_DATE = "20161209";// test

			// ���R���w�g��n�����
			INST007DAO INST007 = new INST007DAO(dbManager, conn);
			String condString = "AYEAR = '" + AYEAR + "' AND SMS  = '" + SMS
					+ "' AND TUT_DATE = '" + CAL_DATE + "'";
			delete_cnt = INST007.delete(condString);
			ycmProgLog.logAppend("Delete Before INST007 " + condString + ", "
					+ delete_cnt + " pieces");
			INST008DAO ins035 = new INST008DAO(dbManager, conn);
			condString = "AYEAR = '" + AYEAR + "' AND SMS  = '" + SMS
					+ "' AND SALARY_DATE = '" + CAL_DATE
					+ "' AND NVL(IS_UPDATED,'0') <> '1'";

			delete_cnt = ins035.delete(condString);
			ycmProgLog.logAppend("Delete Before INST008 " + condString + ", "
					+ delete_cnt + " pieces");

			// �p��ݥ��Юv���I�ɼ�(�Ѧ�per037m���k)
			PERGETTUTTIME tt = new PERGETTUTTIME(dbManager, conn);
			tt.setAYEAR(AYEAR);
			tt.setSMS(SMS);
			//tt.setNouVocationType("2"); // �ȭp��ݥ��Юv(�ݥ��~�ݭp���)
			tt.setIS_BATCH_RUN(true);
			// tt.setCENTER_CODE("02");// test
			// tt.setCLASS_KIND("2");// test
			// tt.setIDNO("B120326285");// test
			int executeResult = 1;
			executeResult = tt.execute();
			if (executeResult != AbstractMedium.SUCCESS) {
				throw new Exception(tt.getAllError());
			}
			Vector vt = tt.getData();
			for (int i = 0; i < vt.size(); i++) {
				Hashtable data = (Hashtable) vt.get(i);

				String CLASS_KIND = Utility.nullToSpace(data.get("CLASS_KIND"));

				if (!"".equals(data.get("IDNO"))
						&& !"3".equals(CLASS_KIND)) {

					String msg = this.setInsTable(dbManager, conn,
							data);
					if (!"".equals(msg)) {
						throw new Exception(msg);
					}
					cnt1++;
				}
			}

			// �p�⭱�±Юv�½����I�O(�Ѧ�PER043M���k)
			SYST001GATEWAY syst001 = new SYST001GATEWAY(dbManager, conn);
			INST007GATEWAY INST007G = new INST007GATEWAY(dbManager, conn);
			EXAEXPENSE aa = new EXAEXPENSE(dbManager);

			Hashtable ht = new Hashtable();
			ht.put("AYEAR", AYEAR);
			ht.put("SMS", SMS);
			ht.put("CAL_DATE", CAL_DATE);
			vt = INST007G.getIns006CalData(ht);
			for (int i = 0; i < vt.size(); i++) {
				Hashtable data = (Hashtable) vt.get(i);

				String duttCode = getDUTT_CODE(dbManager,
						Utility.checkNull(data.get("AYEAR"), ""),
						Utility.checkNull(data.get("SMS"), ""),
						Utility.checkNull(data.get("IDNO"), ""),
						Utility.checkNull(data.get("TUT_DAY_KIND"), ""));

				if ("".equals(duttCode)) {
					throw new Exception(Utility.checkNull(data.get("IDNO"), "")
							+ "Check the level of no appointment");
				}

				// �Ҩ��o�����Ŧp�L�]�w�O��,�h������ܿ��~�T���i��
				if (!duttCode.substring(0, 1).equals("E")) {
					throw new Exception(Utility.checkNull(
							data.get("IDNO"), "")
							+ " Appointment level is "
							+ duttCode
							+ syst001.getCodeAndCodeNameForUse("EMP_GRADE",
									duttCode) + ",No cost has been set.");
				}

				// �p��O�ΨñN��Ʀs�JINST008��
				aa.setHEALTH_TYPE(false);
				aa.setCOUNT_TYPE("8");
				aa.setAYEAR(Utility.checkNull(data.get("AYEAR"), ""));
				aa.setSMS(Utility.checkNull(data.get("SMS"), ""));
				aa.setDEP_CODE(Utility.checkNull(data.get("CENTER_CODE"), ""));
				aa.setIDNO(Utility.checkNull(data.get("IDNO"), ""));
				aa.setJOB_CODE(EXAEXPENSE.JOB_TIME_CODE_00E);
				aa.setDUTT_CODE("E" + EXAEXPENSE.getTutDayKind(Utility.checkNull(data.get("TUT_DAY_KIND"), "")) + "0");
				aa.setEXP_ITEM_CODE(duttCode);
				aa.setJOB_NUM(Utility.checkNull(data.get("TOTAL_TIME"), ""));
				aa.setITEM_NAME(Utility.checkNull(data.get("CRSNO"), "") + Utility.checkNull(data.get("CLASS_LIST"), "") + Utility.checkNull(data.get("CLASS_KIND"), ""));
				aa.setUPD_USER_ID("Batch");
				aa.setCAL_DATE(CAL_DATE);
				aa.setYCMProgLog(ycmProgLog);
				aa.execute();

				String errorMsg = "";
				while (aa.hasNextError()) {
					if (!errorMsg.equals(""))
						errorMsg += ";";
					errorMsg += Utility.checkNull(data.get("IDNO"), "") + "," + aa.nextError() + "," + duttCode;
				}

				if (!errorMsg.equals("")) {
					ycmProgLog.logAppend("Error:" + errorMsg);
				} else {
					cnt2++;
					ycmProgLog.logAppend("Insert INST008 IDNO=" + Utility.checkNull(data.get("IDNO"), "") + ", ITEM_NAME=" + Utility.checkNull(data.get("CRSNO"), "") + Utility.checkNull(data.get("CLASS_LIST"), "") + Utility.checkNull(data.get("CLASS_KIND"), "") + ", DUTT_CODE=" + "E" + EXAEXPENSE.getTutDayKind(Utility.checkNull(data.get("TUT_DAY_KIND"), "")) + "0");
				}
			}
			// �M���¸��
			aa.setJOB_CODE(null);
			aa.setEXP_ITEM_CODE(null);

			// �p��q����ߧU�z�u�@�O(�Ѧ�PER061M���k)
			PERT031GATEWAY PERT031 = new PERT031GATEWAY(dbManager, conn);
			// ���o��ߧU�z�`�W�Үɼ�
			DBResult rs = PERT031.getIns035xCal(AYEAR, SMS, CAL_DATE);
			while( rs.next() )
			{
				// �p��O�ΨñN��Ʀs�JINST008��
				aa.setCOUNT_TYPE("8"); 
				aa.setAYEAR(AYEAR);
				aa.setSMS(SMS);
				aa.setDEP_CODE(rs.getString("CENTER_CODE"));
				aa.setIDNO(rs.getString("ASSISTANT_ID"));
				aa.setDUTT_CODE("F10");  
				aa.setJOB_NUM(rs.getString("JOB_NUM"));
				aa.setITEM_NAME(rs.getString("CRS_NAME")+rs.getString("CLASS_CODE"));
				aa.setUPD_USER_ID("Batch");
				aa.setCAL_DATE(CAL_DATE);
				aa.setYCMProgLog(ycmProgLog);
				executeResult = aa.execute();
					
				String errorMsg = "";
				while (aa.hasNextError()) {
					if (!errorMsg.equals(""))
						errorMsg += ";";
					errorMsg += Utility.checkNull(rs.getString("ASSISTANT_ID"), "") + ","
							+ aa.nextError();
				}

				if (!errorMsg.equals("")) {
					ycmProgLog.logAppend("Error:" + errorMsg);
				} else {
					cnt3++;
					ycmProgLog.logAppend("Insert INST008 IDNO="
							+ rs.getString("ASSISTANT_ID") + ", ITEM_NAME="
							+ rs.getString("CRS_NAME")
							+ rs.getString("CLASS_CODE") + ", DUTT_CODE="
							+ "F10");
				}		
			}
				

			// �p��������§U�z�u�@�O(�Ѧ�PER060M���k)
			PERT004GATEWAY PERT004 = new PERT004GATEWAY(dbManager, conn);
			rs = PERT004.getIns006xCal(AYEAR, SMS, CAL_DATE);
			while (rs.next()) {
				// �p��O�ΨñN��Ʀs�JINST008��
				aa.setCOUNT_TYPE("8");
				aa.setAYEAR(rs.getString("AYEAR"));
				aa.setSMS(rs.getString("SMS"));
				aa.setIDNO(rs.getString("ASSISTANT_ID"));
				aa.setDEP_CODE("00");
				aa.setEXP_ITEM_CODE(rs.getString("EXPENSE_ITEM_CODE"));
				aa.setDUTT_CODE(rs.getString("SPEND_DUTY_CODE"));
				aa.setJOB_NUM(rs.getString("JOB_NUM"));
				aa.setITEM_NAME(rs.getString("SPEND_DUTY_NAME"));
				aa.setUPD_USER_ID("Batch");
				aa.setCAL_DATE(CAL_DATE);
				aa.setYCMProgLog(ycmProgLog);
				executeResult = aa.execute();

				String errorMsg = "";
				while (aa.hasNextError()) {
					if (!errorMsg.equals(""))
						errorMsg += ";";
					errorMsg += Utility.checkNull(rs.getString("ASSISTANT_ID"),
							"") + "," + aa.nextError();
				}

				if (!errorMsg.equals("")) {
					ycmProgLog.logAppend("Error:" + errorMsg);
				} else {
					cnt4++;
					ycmProgLog.logAppend("Insert INST008 IDNO="
							+ rs.getString("ASSISTANT_ID") + ", ITEM_NAME="
							+ rs.getString("SPEND_DUTY_NAME") + ", DUTT_CODE="
							+ rs.getString("SPEND_DUTY_CODE"));
				}

			}
			
			// �p��������§U�z�u�@�O(�Ѧ�PER073M���k)
			PERT055GATEWAY PERT055 = new PERT055GATEWAY(dbManager, conn);
			rs = PERT055.getIns006xCal(AYEAR, SMS, CAL_DATE);
			while (rs.next()) {
				// �p��O�ΨñN��Ʀs�JINST008��
				aa.setCOUNT_TYPE("8");
				aa.setAYEAR(rs.getString("AYEAR"));
				aa.setSMS(rs.getString("SMS"));
				aa.setIDNO(rs.getString("ASSISTANT_ID"));
				aa.setDEP_CODE(rs.getString("CENTER_CODE"));
				aa.setEXP_ITEM_CODE(rs.getString("EXPENSE_ITEM_CODE"));
				aa.setDUTT_CODE(rs.getString("SPEND_DUTY_CODE"));
				aa.setJOB_NUM(rs.getString("JOB_NUM"));
				aa.setITEM_NAME(rs.getString("SPEND_DUTY_NAME"));
				aa.setUPD_USER_ID("Batch");
				aa.setCAL_DATE(CAL_DATE);
				aa.setYCMProgLog(ycmProgLog);
				aa.setSOURCE_TABLE("PERT055");
				//aa.setLIST_SUMMARY("108�Ǧ~�פW�Ǵ��M�Z��������U�z�O");
				
				executeResult = aa.execute();

				String errorMsg = "";
				while (aa.hasNextError()) {
					if (!errorMsg.equals(""))
						errorMsg += ";";
					errorMsg += Utility.checkNull(rs.getString("ASSISTANT_ID"),
							"") + "," + aa.nextError();
				}

				if (!errorMsg.equals("")) {
					ycmProgLog.logAppend("Error:" + errorMsg);
				} else {
					cnt4++;
					ycmProgLog.logAppend("Insert INST008 IDNO="
							+ rs.getString("ASSISTANT_ID") + ", ITEM_NAME="
							+ rs.getString("SPEND_DUTY_NAME") + ", DUTT_CODE="
							+ rs.getString("SPEND_DUTY_CODE"));
				}

			}
		
			// ��X�妸���G
			ycmProgLog.setVisibleData("�p��ݥ��Юv���I�ɼ��`�@" + (cnt1) + "��");
			ycmProgLog.writeVisibleRslt();
			ycmProgLog.setVisibleData("�p�⭱�±Юv�½����I�O�`�@" + (cnt2) + "��");
			ycmProgLog.writeVisibleRslt();
			ycmProgLog.setVisibleData("�p��q����ߧU�z�u�@�O�`�@" + cnt3 + "��");
			ycmProgLog.writeVisibleRslt();
			ycmProgLog.setVisibleData("�p��������§U�z�u�@�O�`�@" + cnt4 + "��");
			ycmProgLog.writeVisibleRslt();
			ycmProgLog.logAppend("startProcess--end");
		} catch (Exception e) {
			ycmProgLog.logAppend("startProcess--error");
			throw e;
		}
		return true;
	}

	
	/**
	 * ���oLIST_SUMMARY
	 * @return String
	 */
	private String getLIST_SUMMARY(String LIST_SUMMARY,String expenseTypeName,String AYEAR,String SMS ,String CENTER_CODE,String EXAM_TYPE,String CHKLIST_YYYYMM) throws Exception{
		String s = "";
		String[] smsArray = { "", "�W�Ǵ�", "�U�Ǵ�", "����" };
		String[] examTypeArray = { "", "������", "������" };
		try {
			s = //(Utility.getCNum(Integer.parseInt(AYEAR) + "", 2) + 
				( 
					AYEAR +"�Ǧ~��" + smsArray[Integer.parseInt(SMS)] + expenseTypeName + 
					(Utility.nullToSpace(EXAM_TYPE).equals("") ? "" : examTypeArray[Integer.parseInt(EXAM_TYPE)])
				) +
				( ( "".equals(Utility.nullToSpace(CHKLIST_YYYYMM)) &&  Utility.nullToSpace(CHKLIST_YYYYMM).length() !=6 )? CHKLIST_YYYYMM : CHKLIST_YYYYMM.substring(0, 4) + "�~" + Integer.parseInt(CHKLIST_YYYYMM.substring(4, 6))+ "��" ) +
				"";
	    } catch (Exception e) {
	        throw e;
	    } finally {
	        
	    }
		return s;
	}
	
	private String setInsTable(DBManager dbManager, Connection conn,
		 Hashtable data)
			throws Exception, SQLException {
		String CLASS_KIND = Utility.nullToSpace(data.get("CLASS_KIND"));
		String AYEAR = Utility.nullToSpace(data.get("AYEAR"));
		String SMS = Utility.nullToSpace(data.get("SMS"));
		String CENTER_CODE = Utility.nullToSpace(data.get("CENTER_CODE"));
		String CRSNO = Utility.nullToSpace(data.get("CRSNO"));
		String CLASS_LIST = Utility.nullToSpace(data.get("CLASS_LIST"));
		String errMsg = "";
		String time = "1800";
		try {
			StringBuffer sql = new StringBuffer();
			if ("1".equals(CLASS_KIND)) {

				sql.append("SELECT A.TUT_DATE , A.TUT_DAY_KIND ,COUNT(1) AS TUT_NUM ");
				sql.append("FROM ( ");
				sql.append("	SELECT ");
				sql.append("	CASE  ");
				sql.append("		WHEN M.PLA_WEEK_CODE IN ('6','7') THEN 'H' ");
				sql.append("		WHEN M.PLA_WEEK_CODE NOT IN ('6','7') AND R2.STIME < '"
						+ time + "' THEN 'D' ");
				sql.append("		WHEN M.PLA_WEEK_CODE NOT IN ('6','7') AND R2.STIME >= '"
						+ time + "' THEN 'N' ");
				sql.append("	ELSE 'H' END AS TUT_DAY_KIND, ");
				sql.append("	M.TUT_DATE ");
				sql.append("	FROM PERT013 M  ");
				sql.append("	JOIN SYST002 R1 ON M.CENTER_CODE =  R1.CENTER_CODE  ");
				sql.append("	JOIN PLAT005 R2 ON M.AYEAR =  R2.AYEAR AND M.SMS =  R2.SMS AND R1.CENTER_ABRCODE = R2.CENTER_ABRCODE  ");
				sql.append("	AND R2.WEEK_CODE = M.PLA_WEEK_CODE AND M.CMPS_CODE = R2.CMPS_CODE AND R2.WEEK_CODE ||  R2.SECTION_CODE = '"
						+ CLASS_LIST.substring(2, 4) + "' ");
				sql.append("	WHERE M.AYEAR = '" + AYEAR + "' AND M.SMS = '" + SMS + "' ");
				sql.append("    AND M.CENTER_CODE = '" + CENTER_CODE + "'  ");
				sql.append("    AND M.CMPS_CODE = (  ");
				sql.append("		SELECT r1.CMPS_CODE	FROM PLAT012 r1 ");
				sql.append("		WHERE r1.AYEAR = '" + AYEAR + "'  ");
				sql.append("		AND r1.SMS = '" + SMS + "' ");
				sql.append("		AND r1.CENTER_ABRCODE = (SELECT CENTER_ABRCODE FROM SYST002 WHERE CENTER_CODE = '" + CENTER_CODE + "') ");
				sql.append("		AND r1.CRSNO = '" + CRSNO + "' ");
				sql.append("		AND r1.CLASS_CODE = '" + CLASS_LIST + "'  ) ");
				sql.append("    AND M.TUT_DATE = '" + this.CAL_DATE + "' ");
				sql.append(") A WHERE 1=1 ");
				sql.append("GROUP BY A.TUT_DATE , A.TUT_DAY_KIND ");
				sql.append("ORDER BY A.TUT_DATE , A.TUT_DAY_KIND ");

			} else if ("2".equals(CLASS_KIND)) {

				sql.append("SELECT A.UNIT_HOURS,A.TUT_DATE , A.TUT_DAY_KIND ,COUNT(1) AS TUT_NUM ");
				sql.append("FROM ( ");
				sql.append("	SELECT ");
				sql.append("	CASE  ");
				sql.append("		WHEN M.WEEK IN ('6','7') THEN 'H' ");
				sql.append("		WHEN M.WEEK NOT IN ('6','7') AND M.TUT_STIME < '"
						+ time + "' THEN 'D' ");
				sql.append("		WHEN M.WEEK NOT IN ('6','7') AND M.TUT_STIME >= '"
						+ time + "' THEN 'N' ");
				sql.append("	ELSE 'H' END AS TUT_DAY_KIND, ");
				sql.append("	M.TEA_DATE AS TUT_DATE, ");
				//sql.append("	ROUND((M.TUT_ETIME - M.TUT_STIME)/100,1) AS UNIT_HOURS ");   //��TUT_STIME~TUT_ETIME~��אּ�W�Ҥ��i�ɶ�
				sql.append("	ROUND((M.PERTUT_ETIME - M.PERTUT_STIME)/100,1) AS UNIT_HOURS ");   //��PERTUT_STIME~PERTUT_ETIME���I�־P�ɶ�
 				sql.append("	FROM PERT004 M ");
				sql.append("	WHERE M.AYEAR = '" + AYEAR + "' ");
				sql.append("	AND M.SMS = '" + SMS + "' ");
				sql.append("	AND M.CRSNO = '" + CRSNO + "'  ");
				sql.append("	AND M.CLASS_NAME = '" + CLASS_LIST + "'  ");
				sql.append("    AND M.TEA_DATE = '" + this.CAL_DATE + "' ");
				sql.append(") A WHERE 1=1  ");
				sql.append("GROUP BY A.TUT_DATE , A.TUT_DAY_KIND ,A.UNIT_HOURS ");
				sql.append("ORDER BY A.TUT_DATE , A.TUT_DAY_KIND ");

			} else if ("4".equals(CLASS_KIND)) {

				sql.append("SELECT A.TUT_DATE , A.TUT_DAY_KIND ,COUNT(1) AS TUT_NUM ");
				sql.append("FROM ( ");
				sql.append("	SELECT ");
				sql.append("	CASE  ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.TUT_DATE,'YYYYMMDD'),'D')  IN ('1','7') THEN  'H' ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.TUT_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.TUT_STIME < '"
						+ time + "' THEN 'D' ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.TUT_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.TUT_STIME >= '"
						+ time + "' THEN 'N' ");
				sql.append("	ELSE 'H' END AS TUT_DAY_KIND, ");
				sql.append("	M.TUT_DATE  ");
				sql.append("	FROM PERT032 M WHERE M.AYEAR = '" + AYEAR
						+ "' AND M.SMS = '" + SMS + "' AND CENTER_CODE = '"
						+ CENTER_CODE + "' ");
				sql.append("    AND M.TUT_DATE = '" + this.CAL_DATE + "' ");
				sql.append(") A WHERE 1=1 ");
				sql.append("GROUP BY A.TUT_DATE , A.TUT_DAY_KIND ");
				sql.append("ORDER BY A.TUT_DATE , A.TUT_DAY_KIND ");

			} else if ("7".equals(CLASS_KIND)) {

				sql.append("SELECT A.TUT_DATE , A.TUT_DAY_KIND ,COUNT(1) AS TUT_NUM ");
				sql.append("FROM ( ");
				sql.append("	SELECT ");
				sql.append("	CASE  ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.LAB_DATE,'YYYYMMDD'),'D')  IN ('1','7') THEN  'H' ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.LAB_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.LAB_STIME < '"
						+ time + "' THEN 'D' ");
				sql.append("		WHEN TO_CHAR(TO_DATE(M.LAB_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.LAB_STIME >= '"
						+ time + "' THEN 'N' ");
				sql.append("	ELSE 'H' END AS TUT_DAY_KIND, ");
				sql.append("	M.LAB_DATE AS TUT_DATE  ");
				sql.append("	FROM PERT031 M WHERE M.AYEAR = '" + AYEAR
						+ "' AND M.SMS = '" + SMS + "' AND CRSNO = '" + CRSNO
						+ "' AND CLASS_CODE = '" + CLASS_LIST + "' ");
				sql.append("    AND M.LAB_DATE = '" + this.CAL_DATE + "' ");
				sql.append(") A WHERE 1=1 ");
				sql.append("GROUP BY A.TUT_DATE , A.TUT_DAY_KIND ");
				sql.append("ORDER BY A.TUT_DATE , A.TUT_DAY_KIND ");

			} else if ("8".equals(CLASS_KIND)) {

				sql.append("SELECT A.TUT_DATE , A.TUT_DAY_KIND ,COUNT(1) AS TUT_NUM  ");
				sql.append("FROM (   ");
				sql.append("	SELECT   ");
				sql.append("	CASE    ");
				sql.append("		WHEN TO_CHAR(TO_DATE(R1.TUT_DATE,'YYYYMMDD'),'D')  IN ('1','7') THEN  'H'   ");
				sql.append("		WHEN TO_CHAR(TO_DATE(R1.TUT_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.STIME < '"
						+ time + "' THEN 'D'   ");
				sql.append("		WHEN TO_CHAR(TO_DATE(R1.TUT_DATE,'YYYYMMDD'),'D') NOT  IN ('1','7')  AND M.STIME >= '"
						+ time + "' THEN 'N'   ");
				sql.append("	ELSE 'H' END AS TUT_DAY_KIND,   ");
				sql.append("	R1.TUT_DATE   ");
				sql.append("	FROM COUT022 M  ");
				sql.append("	LEFT JOIN PERT055 R1 ON M.AYEAR = R1.AYEAR AND M.SMS = R1.SMS AND M.CRSNO = R1.CRSNO  ");
				sql.append("	AND M.S_CLASS_TYPE = R1.S_CLASS_TYPE AND M.S_CLASS_NUM = R1.S_CLASS_NUM ");
				sql.append("	WHERE M.AYEAR = '" + AYEAR + "' AND M.SMS = '"
						+ SMS + "' AND M.CRSNO = '" + CRSNO
						+ "' AND M.S_CLASS_ID = '" + CLASS_LIST + "' ");
				sql.append("    AND R1.TUT_DATE = '" + this.CAL_DATE + "' ");
				sql.append(") A WHERE 1=1   ");
				sql.append("GROUP BY A.TUT_DATE , A.TUT_DAY_KIND   ");
				sql.append("ORDER BY A.TUT_DATE , A.TUT_DAY_KIND   ");

			}

			if (sql.length() != 0) {

				Vector vtData = UtilityX.getvtData(dbManager, conn,
						sql.toString());
				for (int k = 0; k < vtData.size(); k++) {
					Hashtable ht1 = (Hashtable) vtData.get(k);
					if (!"".equals(CAL_DATE)
							&& !Utility.nullToSpace(ht1.get("TUT_DATE"))
									.equals(CAL_DATE)) {
						continue;
					}

					String TUT_NUM = Utility.nullToSpace(ht1.get("TUT_NUM"));
					String TUT_DAY_KIND = Utility.nullToSpace(ht1
							.get("TUT_DAY_KIND"));

					// �p��ɼ�
					String TOTAL_TIME = "0";
					if ("2".equals(CLASS_KIND)) {
						TOTAL_TIME = String.valueOf(Double.parseDouble(TUT_NUM)
								* Double.parseDouble(UtilityX.checkNullEmpty(
										ht1.get("UNIT_HOURS"), "0"))
								* Double.parseDouble(UtilityX.checkNullEmpty(
										data.get("HOURS_P"), "0")));
					} else {
						TOTAL_TIME = String.valueOf(Integer.parseInt(TUT_NUM)
								* Integer.parseInt(UtilityX.checkNullEmpty(
										data.get("TUT_UNIT"), "0"))
								* Integer.parseInt(UtilityX.checkNullEmpty(
										data.get("CLASS_NUM"), "0")));
					}

					INST007DAO INST007 = new INST007DAO(dbManager, conn);
					INST007.setAYEAR(Utility.nullToSpace(data.get("AYEAR")));
					INST007.setSMS(Utility.nullToSpace(data.get("SMS")));
					INST007.setCENTER_CODE(Utility.nullToSpace(data
							.get("CENTER_CODE")));
					INST007.setCRSNO(Utility.nullToSpace(data.get("CRSNO")));
					INST007.setCLASS_KIND(Utility.nullToSpace(data
							.get("CLASS_KIND")));
					INST007.setIDNO(Utility.nullToSpace(data.get("IDNO")));
					INST007.setCLASS_LIST(Utility.nullToSpace(data
							.get("CLASS_LIST")));
					INST007.setTUT_DATE(Utility.nullToSpace(ht1.get("TUT_DATE")));
					INST007.setCLASS_NUM(Utility.nullToSpace(data
							.get("CLASS_NUM")));
					INST007.setTOTAL_TIME(TOTAL_TIME);
					INST007.setTUT_DAY_KIND(TUT_DAY_KIND);
					INST007.setREG_NUMBER(Utility.nullToSpace(data
							.get("TOTAL_NUM")));
					INST007.setUPD_MK("1");
					INST007.setUPD_USER_ID("Batch");
					INST007.setUPD_DATE(DateUtil.getNowDate());
					INST007.setUPD_TIME(DateUtil.getNowTime());
					INST007.setROWSTAMP(DateUtil.getNowDate()
							+ DateUtil.getNowTime());
					INST007.insert();
					ycmProgLog.logAppend("Insert INST007 IDNO="
							+ Utility.nullToSpace(data.get("IDNO"))
							+ ", CRSNO="
							+ Utility.nullToSpace(data.get("CRSNO"))
							+ ", CLASS_KIND="
							+ Utility.nullToSpace(data.get("CLASS_KIND")));
				}

			} else {

				INST007DAO INST007 = new INST007DAO(dbManager, conn);
				INST007.setAYEAR(Utility.nullToSpace(data.get("AYEAR")));
				INST007.setSMS(Utility.nullToSpace(data.get("SMS")));
				INST007.setCENTER_CODE(Utility.nullToSpace(data
						.get("CENTER_CODE")));
				INST007.setCRSNO(Utility.nullToSpace(data.get("CRSNO")));
				INST007.setCLASS_KIND(Utility.nullToSpace(data.get("CLASS_KIND")));
				INST007.setIDNO(Utility.nullToSpace(data.get("IDNO")));
				INST007.setCLASS_LIST(Utility.nullToSpace(data.get("CLASS_LIST")));
				INST007.setTUT_DATE("00000000");
				INST007.setCLASS_NUM(Utility.nullToSpace(data.get("CLASS_NUM")));
				INST007.setTUT_DAY_KIND("H");
				INST007.setREG_NUMBER(Utility.nullToSpace(data.get("TOTAL_NUM")));
				INST007.setUPD_MK("1");
				INST007.setUPD_USER_ID("Batch");
				INST007.setUPD_DATE(DateUtil.getNowDate());
				INST007.setUPD_TIME(DateUtil.getNowTime());
				INST007.setROWSTAMP(DateUtil.getNowDate()
						+ DateUtil.getNowTime());
				INST007.insert();

				ycmProgLog.logAppend("Insert INST007 IDNO="
						+ Utility.nullToSpace(data.get("IDNO")) + ", CRSNO="
						+ Utility.nullToSpace(data.get("CRSNO"))
						+ ", CLASS_KIND="
						+ Utility.nullToSpace(data.get("CLASS_KIND"))
						+ ", TUT_DATE=00000000");
			}

		} catch (SQLException sqlex) {
			errMsg = "�нT�{�@�뭱��(PER007M_�n�����±оǤ���� )�B�h������(PLA017M_���@�h�����¯Z�Ÿ��)�B��������(�ҰȲ�PER012M_���@�������±оǮɶ���)�B���Z���µ�����O�_�]�w����";
		} catch (Exception ex) {
			throw ex;
		} finally {

		}
		return errMsg;
	}

	private String getDUTT_CODE(DBManager dbManager, String AYEAR, String SMS,
			String IDNO, String TUT_DAY_KIND) throws Exception {
		Connection conn = dbManager.getConnection(AUTCONNECT.mapConnect("EXA",
				""));

		TRAT007GATEWAY trat007 = new TRAT007GATEWAY(dbManager, conn);
		String level = trat007.get_tchEMP_GRADE(IDNO, AYEAR + SMS);
		ycmProgLog.logAppend("IDNO=" + IDNO + ",TRAT007.EMP_GRADE=" + level);
		String DUTT_CODE = EXAEXPENSE.getPerExpItemCode(level, TUT_DAY_KIND);
		ycmProgLog.logAppend("IDNO=" + IDNO + ",DUTT_CODE=" + DUTT_CODE);
		return DUTT_CODE;
	}
}