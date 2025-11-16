using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Collections.Generic;
using AccosoftRML.Busiess_Logic.RawMaterial;
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RMXtraDriverTripLogic
    {
        static string sConString = Utilities.cnnstr;
        OracleConnection ocConn = new OracleConnection(sConString.ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        #region " FUNCTION FOR FILLING DDL "

        public DataSet FillCurrentStation()
        {
            DataSet dtStation = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT SALES_STN_STATION_CODE Station_Code,   SALES_STN_STATION_NAME  Station_Name FROM SL_STATION_MASTER  ";

                SQL = SQL + "  ORDER BY SALES_STN_STATION_CODE ASC";

                dtStation = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dtStation;
        }

        public DataTable FillSource()
        {
            DataTable dtsource = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_SM_SOURCE_CODE CODE,RM_SM_SOURCE_DESC NAME FROM RM_SOURCE_MASTER  ";

                SQL = SQL + "  ORDER BY RM_SM_SOURCE_CODE ASC";

                dtsource = clsSQLHelper.GetDataTableByCommand(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dtsource;
        }

        public DataTable FillDestination()
        {
            DataTable destination = new DataTable();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT TECH_PLM_PLANT_CODE CODE, TECH_PLM_PLANT_NAME NAME ";

                SQL = SQL + " FROM TECH_PLANT_MASTER ";

               // SQL = SQL + " WHERE TECH_PLM_WASHING_PLANT ='N' AND  TECH_PLM_RM_RCPT_DESITINATION='Y' AND  TECH_PLM_PRODUTION_PLANT='N' ";

                SQL = SQL + " order by  TECH_PLM_PLANT_NAME asc";

                destination = clsSQLHelper.GetDataTableByCommand(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return destination;
        }
 
        
        #endregion

        #region " FUNCTION FOR FETCHING ACCORDING TO LOOKUP SELECTED "

        public DataSet FetchDataSearch(string documentid, object mngrclassobj)// Fetch According TO The asset transfer Code - search
        {
            DataSet dtReturn = new DataSet();
            SessionManager mngrclass = (SessionManager)mngrclassobj;
            try
            {
               // SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT ";              


                SQL = SQL + "   RM_XTR_TRIP_NO,";
                SQL = SQL + "   RM_XTR_TRIP_DATE,";
                SQL = SQL + "   RM_XTR_TRIP_ITEM_CODE,XTRITEMNAME,";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.RM_SM_SOURCE_CODE,RM_XTRA_DRIVER_TRIPS.TECH_PLM_PLANT_CODE,";
                SQL = SQL + "   TECH_PLM_PLANT_NAME,RM_SM_SOURCE_DESC,";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.FA_FAM_ASSET_CODE,FA_FAM_ASSET_DESCRIPTION,";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.HR_EMP_EMPLOYEE_CODE,HR_EMP_EMPLOYEE_NAME,";
                SQL = SQL + "   RM_XTR_TRIP_REMARKS,";
                SQL = SQL + "   RM_XTR_TRIP_CREATEDBY,RM_XTR_TRIP_CREATEDDATE,";
                SQL = SQL + "   RM_XTR_TRIP_APPROVED,RM_XTR_TRIP_APPROVED_BY,";
                SQL = SQL + "   RM_XTR_TRIP_AMOUNT,";
                SQL = SQL + "   RM_XTR_TRIP_TYPE,RM_XTR_TRIP_COUNT,RM_XTR_TRIP_REFNO, ";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.AD_BR_CODE,AD_BR_NAME ,RM_XTRA_DRIVER_TRIPS.RM_XTR_TRIP_DATE_TIME ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS,PR_XTRA_TRIP_ITMES_VW, FA_FIXED_ASSET_MASTER,";
                SQL = SQL + "   HR_EMPLOYEE_MASTER,SL_STATION_MASTER,TECH_PLANT_MASTER,RM_SOURCE_MASTER,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_XTRA_DRIVER_TRIPS.RM_XTR_TRIP_ITEM_CODE =  PR_XTRA_TRIP_ITMES_VW.XTRITEMCODE";
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.HR_EMP_EMPLOYEE_CODE =  HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.FA_FAM_ASSET_CODE =  FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
               
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.RM_SM_SOURCE_CODE =  RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.TECH_PLM_PLANT_CODE =  TECH_PLANT_MASTER.TECH_PLM_PLANT_CODE";
                SQL = SQL + "   AND RM_XTRA_DRIVER_TRIPS.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE";

                SQL = SQL + "   AND RM_XTR_TRIP_NO ='" + documentid + "'";
                SQL = SQL + "   AND AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                dtReturn = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }

            return dtReturn;
        } 

        

        #endregion

        #region " FUNCTION FOR FILLING  LOOKUP "
            
        public DataTable FillAsset()// Look UP For Filling Asset
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " 	SELECT XTRITEMCODE CODE ,XTRITEMNAME  NAME ";
                SQL = SQL + " 	FROM PR_XTRA_TRIP_ITMES_VW  ";
                SQL = SQL + "   order by XTRITEMCODE asc";
                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        public DataTable FillXtraTripDetails(DateTime fromdate, DateTime todate, string approvalstatus, object mngrclassobj)// Look UP For Filling Asset Transfer
        {
            DataTable dtassetTransfer = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "SELECT RM_XTR_TRIP_NO CODE , to_char(RM_XTR_TRIP_DATE) EntryDate ,";
               // SQL = SQL + " RM_XTR_TRIP_ITEM_CODE ITEMCODE,XTRITEMNAME ITEMNAME";
                SQL = SQL + " RM_XTRA_DRIVER_TRIPS.FA_FAM_ASSET_CODE ITEMCODE,FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION ITEMNAME,RM_XTR_TRIP_REFNO REFNO";

                SQL = SQL + " FROM  RM_XTRA_DRIVER_TRIPS,FA_FIXED_ASSET_MASTER ";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_XTRA_DRIVER_TRIPS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND RM_XTR_TRIP_DATE  BETWEEN '" + fromdate.ToString("dd-MMM-yyyy") + "' AND '" + todate.ToString("dd-MMM-yyyy") + "'";


                if (approvalstatus == "Y")
                {

                    SQL = SQL + "  AND RM_XTRA_DRIVER_TRIPS.RM_XTR_TRIP_APPROVED ='Y'";
                }
                else if (approvalstatus == "N")
                {
                    SQL = SQL + "  AND RM_XTRA_DRIVER_TRIPS.RM_XTR_TRIP_APPROVED ='N'";
                }
                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + "  and     RM_XTRA_DRIVER_TRIPS.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                SQL = SQL + " order by RM_XTR_TRIP_NO desc";

                dtassetTransfer = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtassetTransfer;

        }

        public DataTable FillDriverAccountView()//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE CAT_CODE, HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_NAME CAT_NAME, ";
                SQL = SQL + " HR_EMPLOYEE_MASTER.HR_EMP_TELNO TEL_NO, HR_EMPLOYEE_MASTER.HR_EMP_MOBILENO MOBILE_NO ";
                SQL = SQL + " FROM HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE  HR_EMP_STATUS ='A' AND  HR_DM_DESIGNATION_CODE  ";
                SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) ";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        public DataTable FillTrailerView()//
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();




                SQL = "  SELECT   FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE REF_CODE ,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION name,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID phone,PR_DRIVER_MASTER_DETS.PR_DR_DRIVER_CODE drcode,HR_EMP_EMPLOYEE_NAME drname,'TRUCK' asset_type  ";
                SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,PR_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                SQL = SQL + " AND  PR_DRIVER_MASTER_DETS.PR_DR_DRIVER_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";
                SQL = SQL + " AND  PR_DRIVER_MASTER_DETS.WS_AM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND  FA_FAM_APPROVED='Y' AND  FA_FAM_ASSET_STATUS ='A' AND  FA_FAM_VEHICLE_TYPE   ";
                SQL = SQL + " IN  (   SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE'";
                SQL = SQL + "  ) ";



                SQL = SQL + "  UNION SELECT   FA_FIXED_ASSET_MASTER.FA_FAM_REF_CODE REF_CODE ,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code , FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION name,  ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID phone,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE drcode,HR_EMP_EMPLOYEE_NAME drname,'TRAILER' asset_type  ";
                SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND  FA_FAM_APPROVED='Y' AND  FA_FAM_ASSET_STATUS ='A' AND  FA_FAM_VEHICLE_TYPE   ";
                SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE'";
                SQL = SQL + "  ) ORDER BY asset_type DESC ";

                
                dtType = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        public DataTable FillVechicleAsset()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " 	SELECT FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE code ,FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION  name, ";
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID phone,";
                SQL = SQL + " PR_DRIVER_MASTER_DETS.PR_DR_DRIVER_CODE drcode,HR_EMP_EMPLOYEE_NAME drname,'' asset_type";
                SQL = SQL + " FROM  FA_FIXED_ASSET_MASTER,PR_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER";
                SQL = SQL + " WHERE PR_DRIVER_MASTER_DETS.PR_DR_DRIVER_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE (+)";
                SQL = SQL + " AND  PR_DRIVER_MASTER_DETS.WS_AM_ASSET_CODE (+) = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND  FA_FAM_ASSET_STATUS='A' ";
                SQL = SQL + " AND FA_FAM_WS_ASSETFILE_ENTRY ='Y'order by FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE asc";
                dtType = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtType;

        }

        #endregion

        #region " DML  INSERT FUNCTION "

        public string InsertSQL(RMXtraDriverTripEntiy oEntity, bool bDocAutogenerated, object mngrclassobj ,     bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                string fullClearanceDateTime = oEntity.Transferdate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryDateTimeText;


                SQL = " INSERT INTO RM_XTRA_DRIVER_TRIPS (";                
                SQL = SQL + " RM_XTR_TRIP_NO, AD_FIN_FINYRID,AD_CM_COMPANY_CODE,RM_XTR_TRIP_DATE, ";
                SQL = SQL + " FA_FAM_ASSET_CODE, HR_EMP_EMPLOYEE_CODE,RM_XTR_TRIP_ITEM_CODE,RM_SM_SOURCE_CODE,TECH_PLM_PLANT_CODE,";
                SQL = SQL + " RM_XTR_TRIP_REMARKS,RM_XTR_TRIP_CREATEDBY,RM_XTR_TRIP_CREATEDDATE, ";
                SQL = SQL + " RM_XTR_TRIP_UPDATEDBY, RM_XTR_TRIP_UPDATEDDATE ,RM_XTR_TRIP_APPROVED,RM_XTR_TRIP_APPROVED_BY,SALES_STN_STATION_CODE,";
                SQL = SQL + " RM_XTR_TRIP_AMOUNT,RM_XTR_TRIP_TYPE,RM_XTR_TRIP_COUNT,";
                SQL = SQL + " RM_XTR_TRIP_REFNO ,AD_BR_CODE,RM_XTR_TRIP_DATE_TIME) ";
                SQL = SQL + " VALUES ( '" + oEntity.DOCno + "'," + mngrclass.FinYearID + " ,'" + mngrclass.CompanyCode + "' ,'" + oEntity.Transferdate.ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " '" + oEntity.TrailerCode + "','" + oEntity.DriverCode + "','" + oEntity.ItemCode + "','" + oEntity.Source + "',";
                SQL = SQL + " '" + oEntity.Destination+ "','" + oEntity.Remarks + "',";
                SQL = SQL + "'" + mngrclass.UserName + "',TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "'','',";
                SQL = SQL + "'" + oEntity.Approved + "', '" + oEntity.ApprovedBY + "','" + oEntity.StCode + "', " + oEntity.Amt + ",";
                SQL = SQL + " '" + oEntity.AssetType + "','" + oEntity.TripCount + "','" + oEntity.ReferenceNo + "','"+ oEntity.Branch + "',TO_DATE('" + fullClearanceDateTime + "', 'DD-MON-YYYY HH12:MI:SS AM') )";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTXTRRIP", oEntity.DOCno, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }

        public string DeleteSql(string TransferNo, object mngrclassobj)
        {
            string sRetun = string.Empty;
            sSQLArray.Clear();
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SQL = "DELETE FROM RM_XTRA_DRIVER_TRIPS ";
                SQL = SQL + " WHERE RM_XTR_TRIP_NO ='" + TransferNo + "' AND  AD_FIN_FINYRID = " + mngrclass.FinYearID + "";
                sSQLArray.Add(SQL);
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTXTRRIP", TransferNo, false, Environment.MachineName, "D", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }


            return sRetun;
        }

        public string UpdateSql(RMXtraDriverTripEntiy oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " UPDATE RM_XTRA_DRIVER_TRIPS SET ";
                SQL = SQL + " FA_FAM_ASSET_CODE = '" + oEntity.TrailerCode + "', HR_EMP_EMPLOYEE_CODE = '" + oEntity.DriverCode + "',";
                SQL = SQL + " RM_XTR_TRIP_ITEM_CODE = '" + oEntity.ItemCode + "' ,TECH_PLM_PLANT_CODE = '" + oEntity.Destination + "' , RM_XTR_TRIP_DATE =  '" + oEntity.Transferdate.ToString("dd-MMM-yyyy") + "',";
                SQL = SQL + " RM_SM_SOURCE_CODE = '" + oEntity.Source + "',RM_XTR_TRIP_REMARKS = '" + oEntity.Remarks + "',";
                SQL = SQL + " RM_XTR_TRIP_UPDATEDBY = '" + mngrclass.UserName + "',RM_XTR_TRIP_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_XTR_TRIP_APPROVED = '" + oEntity.Approved + "',RM_XTR_TRIP_APPROVED_BY = '" + oEntity.ApprovedBY + "',SALES_STN_STATION_CODE = '" + oEntity.StCode + "',";
                SQL = SQL + " RM_XTR_TRIP_AMOUNT = " + oEntity.Amt + ",";
                SQL = SQL + " RM_XTR_TRIP_TYPE = '" + oEntity.AssetType + "',RM_XTR_TRIP_COUNT = '" + oEntity.TripCount + "',";
                SQL = SQL + " RM_XTR_TRIP_REFNO = '" + oEntity.ReferenceNo + "'";
                SQL = SQL + " WHERE RM_XTR_TRIP_NO = '" + oEntity.DOCno + "' AND AD_FIN_FINYRID = "+mngrclass.FinYearID+"";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTXTRRIP", oEntity.DOCno, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }


        public string prLastPayrollEntryValidation(string sEmployeeCode, DateTime fdtFromDate, DateTime fdtToDate)
        {
            DataTable dsCount = new DataTable();

            string sReturn = string.Empty;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "     SELECT  count(*) CNT ";
                SQL = SQL + "    FROM ";
                SQL = SQL + "    HR_PAYSLIP_MASTER, HR_PAYSLIP_EMPLOYEES ";
                SQL = SQL + "    WHERE HR_PAYSLIP_MASTER.hr_psm_payslipno = HR_PAYSLIP_EMPLOYEES.hr_psm_payslipno ";
                SQL = SQL + "    AND HR_PAYSLIP_MASTER.ad_fin_finyrid =   HR_PAYSLIP_EMPLOYEES.ad_fin_finyrid  ";
                SQL = SQL + "    and HR_EMP_EMPLOYEE_CODE  = '" + sEmployeeCode + "'";
                SQL = SQL + "  aND  TO_DATE(TO_CHAR( HR_PSM_DATE ,'DD-MON-YYYY')) BETWEEN ";
                SQL = SQL + " last_day('" + fdtFromDate.ToString("dd-MMM-yyyy") + "') AND  Last_day('" + fdtToDate.ToString("dd-MMM-yyyy") + "')";


                dsCount = clsSQLHelper.GetDataTableByCommand(SQL);

                if (int.Parse(dsCount.Rows[0]["CNT"].ToString()) > 0)
                {
                    sReturn = "Payroll Already  processed.Unable to continue";
                }
                else
                {
                    sReturn = "CONTINUE";
                }


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                sReturn = "Error occured ";

            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return sReturn;
        }



        public string UNApproveSql(RMXtraDriverTripEntiy oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                sRetun = prLastPayrollEntryValidation(oEntity.DriverCode.ToString(), oEntity.Transferdate, oEntity.Transferdate);


                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty; 


                SQL = " UPDATE RM_XTRA_DRIVER_TRIPS SET ";
                SQL = SQL + " RM_XTR_TRIP_APPROVED = 'N',RM_XTR_TRIP_APPROVED_BY = ''";
                SQL = SQL + " WHERE RM_XTR_TRIP_NO = '" + oEntity.DOCno + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTXTRRIP", oEntity.DOCno, false, Environment.MachineName, "U", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;
        }


        #endregion      
              
       
        
        #region " PRINT ASSET TRANSFER "

        public DataSet XtraTripPrintData(string TransferNo, object mngrclassobj)
        {
            DataSet dSPrint = new DataSet("RMXTRATRIPPRINT");

            DataTable dtMaster = new DataTable();
            DataTable dtMatchDetails = new DataTable();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT  ";
                SQL = SQL + "  RM_XTR_TRIP_NO, AD_FIN_FINYRID,AD_CM_COMPANY_CODE, ";
                SQL = SQL + "  RM_XTR_TRIP_DATE,SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,";
                SQL = SQL + "  FA_FAM_ASSET_CODE,FA_FAM_ASSET_DESCRIPTION,RM_XTR_TRIP_ITEM_CODE,";
                SQL = SQL + "  XTRITEMNAME,TECH_PLM_PLANT_CODE,RM_SM_SOURCE_CODE,TECH_PLM_PLANT_NAME,RM_SM_SOURCE_DESC,";
                SQL = SQL + "  RM_XTR_TRIP_AMOUNT,RM_XTR_TRIP_REMARKS,RM_XTR_TRIP_CREATEDBY,";
                SQL = SQL + "  HR_EMP_EMPLOYEE_CODE,HR_EMP_EMPLOYEE_NAME,RM_XTR_TRIP_UPDATEDBY,RM_XTR_TRIP_APPROVED_BY,";
                SQL = SQL + "  RM_XTR_TRIP_COUNT,RM_XTR_TRIP_TYPE,RM_XTR_TRIP_REFNO, ";
                SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "  RM_XTRA_TRIP_PRINT_VIEW";
                SQL = SQL + " where  ";
                SQL = SQL + " RM_XTR_TRIP_NO  ='" + TransferNo + "'  AND   AD_FIN_FINYRID  =" + mngrclass.FinYearID;
        
                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dtMaster.TableName = "RMXTRA_TRIP_PRINT_VIEW";
                dSPrint.Tables.Add(dtMaster);                
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }
            finally
            {
                //cCollection = null;
                //objBase = null;
            }
            return dSPrint;

        }

        #endregion 
        
    }
}

public class RMXtraDriverTripEntiy
{
    public RMXtraDriverTripEntiy()
    {
        //
        // TODO: Add constructor logic here
        //
    }

    public string DOCno
    {
        get;
        set;
    }

    public string Source
    {
        get;
        set;
    }


    public string Destination
    {
        get;
        set;
    }
    public string ItemCode
    {
        get;
        set;
    }

    public string StCode
    {
        get;
        set;
    }
    
    public string Branch
    {
        get;
        set;
    }
    
       
    public string TrailerCode
    {
        get;
        set;
    }

   
    public string DriverCode
    {
        get;
        set;
    }

  
    public string CurrentStation
    {
        get;
        set;
    }

   
    public string CurrentDepartment
    {
        get;
        set;
    }

    public string NewDepartment
    {
        get;
        set;
    }
    
    public DateTime Transferdate
    {
        get;
        set;
    }

    public string ApprovedBY
    {
        get;
        set;
    }
  
    public string Approved
    {
        get;
        set;
    }

    public string Remarks
    {
        get;
        set;
    }

    public string TripCode
    {
        get;
        set;
    }

    public string FromSiteLoc
    {
        get;
        set;
    }


    public string ToSiteLoc
    {
        get;
        set;
    }

    public double Amt
    {
        get;
        set;
    }

    public int TripCount
    {
        get;
        set;
    }

    public string AssetType
    {
        get;
        set;
    }

    public string ReferenceNo
    {
        get;
        set;
    }
    public string EntryDateTimeText
    {
        get;
        set;
    }
}

