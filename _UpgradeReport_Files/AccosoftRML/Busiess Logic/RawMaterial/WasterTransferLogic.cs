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

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class WasterTransferLogic
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

              //  SQL = SQL + " where SALES_RAW_MATERIAL_STATION ='Y'";

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

        public DataSet FillCurrentDepartment()
        {
            DataSet dtDepartment = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT HR_DEPT_DEPT_CODE DEPART_CODE,   HR_DEPT_DEPT_DESC  DEPART_NAME FROM HR_DEPT_MASTER  ";

                SQL = SQL + "  ORDER BY HR_DEPT_DEPT_CODE ASC";

                dtDepartment = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dtDepartment;
        }

        public DataSet GetDestination()
        {
            DataSet dsReturn = new DataSet();

            SqlHelper ClsSqlhelper = new SqlHelper();

            try
            {

                SQL = " SELECT TECH_PLM_PLANT_CODE CODE, TECH_PLM_PLANT_NAME NAME ";

                SQL = SQL + " FROM TECH_PLANT_MASTER ";

               // SQL = SQL + " WHERE TECH_PLM_WASHING_PLANT ='N' AND  TECH_PLM_RM_RCPT_DESITINATION='Y' AND  TECH_PLM_PRODUTION_PLANT='N' ";

             
                SQL = SQL + " order by  TECH_PLM_PLANT_NAME asc";

                dsReturn = ClsSqlhelper.GetDataset(SQL);

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
            return dsReturn;
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

                SQL = SQL + "   RM_WST_TRANSFER_NO,";
                SQL = SQL + "   RM_WST_TRANSFER_DATE,";
                SQL = SQL + "   RM_WASTE_TRANSFER_TRIPS.RM_WST_WASTE_ITEM_CODE,RM_WASTAGE_TRIP_ITMES_VW.WASTEITEMNAME,";
                SQL = SQL + "   RM_WASTE_TRANSFER_TRIPS.SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,RM_WASTE_TRANSFER_TRIPS.TECH_PLM_PLANT_CODE,";
                SQL = SQL + "   RM_WST_WASTE_DUMP_LOCAION,RM_WASTE_TRANSFER_TRIPS.FA_FAM_ASSET_CODE,FA_FAM_ASSET_DESCRIPTION,";
                SQL = SQL + "   RM_WST_WASTE_QTY,RM_WASTE_TRANSFER_TRIPS.HR_EMP_EMPLOYEE_CODE,HR_EMP_EMPLOYEE_NAME,";
                SQL = SQL + "   RM_WST_REMARKS,";
                SQL = SQL + "   RM_WST_CREATEDBY,RM_WST_CREATEDDATE,";
                SQL = SQL + "   RM_WST_APPROVED,RM_WST_APPROVED_BY,RM_WST_WEIGHT_BRIDGE_NO , ";
                SQL = SQL + "   RM_SM_SOURCE_CODE  ,  RM_WST_TRIP_COUNT , ";
                SQL = SQL + "   RM_WASTE_TRANSFER_TRIPS.AD_BR_CODE,AD_BR_NAME,RM_WASTE_TRANSFER_TRIPS.RM_WST_TRANSFER_DATE_TIME  ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   RM_WASTE_TRANSFER_TRIPS,RM_WASTAGE_TRIP_ITMES_VW, ";
                SQL = SQL + "  FA_FIXED_ASSET_MASTER,HR_EMPLOYEE_MASTER,SL_STATION_MASTER ,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE ";
                SQL = SQL + "   RM_WASTE_TRANSFER_TRIPS.RM_WST_WASTE_ITEM_CODE =  RM_WASTAGE_TRIP_ITMES_VW.WASTEITEMCODE";
                SQL = SQL + "   AND RM_WASTE_TRANSFER_TRIPS.SALES_STN_STATION_CODE =  SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + "   AND RM_WASTE_TRANSFER_TRIPS.HR_EMP_EMPLOYEE_CODE =  HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";
                SQL = SQL + "   AND RM_WASTE_TRANSFER_TRIPS.FA_FAM_ASSET_CODE =  FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + "   AND RM_WASTE_TRANSFER_TRIPS.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE";
                SQL = SQL + "   AND RM_WST_TRANSFER_NO ='" + documentid + "'";
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

                SQL = " 	SELECT WASTEITEMCODE CODE ,WASTEITEMNAME  NAME ";
                SQL = SQL + " 	FROM RM_WASTAGE_TRIP_ITMES_VW  ";
                SQL = SQL + "   order by WASTEITEMCODE asc";
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

        public DataTable FillWasteTransfer(DateTime fromdate, DateTime todate, string approvalstatus, object mngrclassobj)// Look UP For Filling Asset Transfer
        {
            DataTable dtassetTransfer = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = "SELECT RM_WST_TRANSFER_NO CODE , to_char(RM_WST_TRANSFER_DATE) EntryDate ,";
                SQL = SQL + " RM_WASTE_TRANSFER_TRIPS.FA_FAM_ASSET_CODE ITEMCODE,FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_DESCRIPTION ITEMNAME";

                SQL = SQL + " FROM  RM_WASTE_TRANSFER_TRIPS,FA_FIXED_ASSET_MASTER ";

                SQL = SQL + " WHERE  ";
                SQL = SQL + "  RM_WASTE_TRANSFER_TRIPS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND RM_WST_TRANSFER_DATE  BETWEEN '" + fromdate.ToString("dd-MMM-yyyy") + "' AND '" + todate.ToString("dd-MMM-yyyy") + "'";

                if (approvalstatus == "Y")
                {

                    SQL = SQL + "  AND RM_WASTE_TRANSFER_TRIPS.RM_WST_APPROVED ='Y'";
                }
                else if (approvalstatus == "N")
                {
                    SQL = SQL + "  AND RM_WASTE_TRANSFER_TRIPS.RM_WST_APPROVED ='N'";
                }

                if (mngrclass.UserName != "ADMIN")
                {
                    SQL = SQL + "  and     RM_WASTE_TRANSFER_TRIPS.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                SQL = SQL + " order by RM_WST_TRANSFER_NO desc";

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
                SQL = SQL + " FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_ID , FA_FIXED_ASSET_MASTER.FA_FAM_PLATENO phone,RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE drcode,HR_EMP_EMPLOYEE_NAME drname  ";
                SQL = SQL + " FROM FA_FIXED_ASSET_MASTER , WS_ASSET_CATEGORY_TYPE,RM_DRIVER_MASTER_DETS,HR_EMPLOYEE_MASTER  ";
                SQL = SQL + " WHERE FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE = WS_ASSET_CATEGORY_TYPE.SL_NO  ";
                SQL = SQL + " AND FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_STATUS ='A'  ";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.HR_EMP_EMPLOYEE_CODE = HR_EMPLOYEE_MASTER.HR_EMP_EMPLOYEE_CODE";
                SQL = SQL + " AND  RM_DRIVER_MASTER_DETS.FA_FAM_ASSET_CODE = FA_FIXED_ASSET_MASTER.FA_FAM_ASSET_CODE";
                SQL = SQL + " AND  FA_FIXED_ASSET_MASTER.FA_FAM_VEHICLE_TYPE   ";
                SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";

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

        #region " DML FUNCTION "

        public string InsertSQL(WasterTransferEntiy oEntity, bool bDocAutogenerated, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                string fullClearanceDateTime = oEntity.Transferdate.ToString("dd-MMM-yyyy") + " " + oEntity.EntryDateTimeText;

                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_WASTE_TRANSFER_TRIPS (";                
                SQL = SQL + " RM_WST_TRANSFER_NO, AD_FIN_FINYRID,AD_CM_COMPANY_CODE,RM_WST_TRANSFER_DATE, ";
                SQL = SQL + " FA_FAM_ASSET_CODE, HR_EMP_EMPLOYEE_CODE,RM_WST_WASTE_ITEM_CODE, ";
                SQL = SQL + " RM_WST_WASTE_QTY,RM_WST_WASTE_DUMP_LOCAION,RM_WST_REMARKS,RM_WST_CREATEDBY,RM_WST_CREATEDDATE, ";
                SQL = SQL + " RM_WST_UPDATEDBY, RM_WST_UPDATEDDATE ,RM_WST_APPROVED,RM_WST_APPROVED_BY,SALES_STN_STATION_CODE,TECH_PLM_PLANT_CODE,";
                SQL = SQL + " RM_WST_WEIGHT_BRIDGE_NO ,  RM_SM_SOURCE_CODE  ,  RM_WST_TRIP_COUNT , AD_BR_CODE,RM_WST_TRANSFER_DATE_TIME    ) ";

                SQL = SQL + " VALUES ( '" + oEntity.DOCno + "'," + mngrclass.FinYearID + " ,'" + mngrclass.CompanyCode + "' ,'" + oEntity.Transferdate.ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " '" + oEntity.TrailerCode + "','" + oEntity.DriverCode + "','" + oEntity.ItemCode + "'," + oEntity.Qty + ",'" + oEntity.DumpLocation + "','" + oEntity.Remarks + "',";                            
                SQL = SQL + "'" + mngrclass.UserName + "',TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + "'','',";
                SQL = SQL + "'" + oEntity.Approved + "', '" + oEntity.ApprovedBY + "','" + oEntity.StCode + "','" + oEntity.DestinationCode + "','" + oEntity.WeighBridgeNo + "',";
                SQL = SQL + "'" + oEntity.Sourcecode + "'," + oEntity.TripCount + " ,'" + oEntity.Branch  + "', TO_DATE('" + fullClearanceDateTime + "', 'DD-MON-YYYY HH12:MI:SS AM') ";
                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTWSTTRP", oEntity.DOCno, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, oEntity.Branch, sAtuoGenBranchDocNumber);

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

        public string UpdateSql(WasterTransferEntiy oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
               
                sSQLArray.Clear();

                SQL = " UPDATE RM_WASTE_TRANSFER_TRIPS SET ";
                SQL = SQL + " FA_FAM_ASSET_CODE = '" + oEntity.TrailerCode + "', HR_EMP_EMPLOYEE_CODE = '" + oEntity.DriverCode + "',";
                SQL = SQL + " RM_WST_WASTE_ITEM_CODE = '" + oEntity.ItemCode + "' ,RM_WST_WASTE_QTY = " + oEntity.Qty + ",";
                SQL = SQL + " RM_WST_WASTE_DUMP_LOCAION = '" + oEntity.DumpLocation + "',RM_WST_REMARKS = '" + oEntity.Remarks + "',";
                SQL = SQL + " RM_WST_UPDATEDBY = '" + mngrclass.UserName + "',RM_WST_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_WST_APPROVED = '" + oEntity.Approved + "',RM_WST_APPROVED_BY = '" + oEntity.ApprovedBY + "',SALES_STN_STATION_CODE = '" + oEntity.StCode + "',";
                SQL = SQL + " RM_WST_WEIGHT_BRIDGE_NO  = '" + oEntity.WeighBridgeNo + "',TECH_PLM_PLANT_CODE= '" + oEntity.DestinationCode + "',";
                SQL = SQL + "RM_SM_SOURCE_CODE='" + oEntity.Sourcecode + "', RM_WST_TRIP_COUNT = " + oEntity.TripCount + ",";
       
                SQL = SQL + " WHERE RM_WST_TRANSFER_NO = '" + oEntity.DOCno + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";


                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTWSTTRP", oEntity.DOCno, false, Environment.MachineName, "U", sSQLArray);

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
                SQL =      "DELETE FROM RM_WASTE_TRANSFER_TRIPS "; 
                SQL = SQL+ " WHERE RM_WST_TRANSFER_NO ='"+TransferNo+"' AND  AD_FIN_FINYRID = "+mngrclass.FinYearID+"" ;
                sSQLArray.Add(SQL);
                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTWSTTRP", TransferNo, false, Environment.MachineName, "D", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }


            return sRetun;
        }

        public string UNApproveSql(WasterTransferEntiy oEntity, object mngrclassobj)
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


                SQL = " UPDATE RM_WASTE_TRANSFER_TRIPS SET ";
                SQL = SQL + " RM_WST_APPROVED = 'N',RM_WST_APPROVED_BY = ''";
                SQL = SQL + " WHERE RM_WST_TRANSFER_NO = '" + oEntity.DOCno + "' AND AD_FIN_FINYRID = " + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTWSTTRP", oEntity.DOCno, false, Environment.MachineName, "U", sSQLArray);

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

        #endregion                           
        
        #region " PRINT WASTAGETRANSFER "

        public DataSet WasterTransferPrintData(string TransferNo, object mngrclassobj)
        {
            DataSet dSPrint = new DataSet("WASTETRANSFERPRINT");

            DataTable dtMaster = new DataTable();
            DataTable dtMatchDetails = new DataTable();
            try
            {
                SQL = string.Empty;

                SqlHelper clsSQLHelper = new SqlHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT  ";
                SQL = SQL + "  RM_WST_TRANSFER_NO, RM_WST_TRANSFER_DATE,RM_WST_WASTE_ITEM_CODE, ";
                SQL = SQL + "  WASTEITEMNAME,SALES_STN_STATION_CODE,SALES_STN_STATION_NAME,RM_WST_WASTE_DUMP_LOCAION,";
                SQL = SQL + "  FA_FAM_ASSET_CODE,FA_FAM_ASSET_DESCRIPTION,RM_WST_WASTE_QTY,";
                SQL = SQL + "  TECH_PLM_PLANT_CODE,TECH_PLM_PLANT_NAME,";
                SQL = SQL + "  HR_EMP_EMPLOYEE_CODE,HR_EMP_EMPLOYEE_NAME,RM_WST_REMARKS,RM_WST_CREATEDBY,";
                SQL = SQL + "  RM_WST_APPROVED_BY,RM_WST_WEIGHT_BRIDGE_NO ,";
                SQL = SQL + "       MASTER_BRANCH_CODE ,   MASTER_BRANCH_NAME , ";
                SQL = SQL + "       MASTER_BRANCHDOC_PREFIX,  MASTER_BRANCH_POBOX,  MASTER_BRANCH_ADDRESS, ";
                SQL = SQL + "       MASTER_BRANCH_CITY, MASTER_BRANCH_PHONE,  MASTER_BRANCH_FAX, ";
                SQL = SQL + "       MASTER_BRANCH_SPONSER_NAME,  MASTER_BRANCH_TRADE_LICENSE_NO,  MASTER_BRANCH_EMAIL_ID, ";
                SQL = SQL + "       MASTER_BRANCH_WEB_SITE, MASTER_BRANCH_VAT_REG_NUMBER ";
                SQL = SQL + "  FROM  ";
                SQL = SQL + "  RM_WASTE_TRANSFER_PRINT_VIEW";
                SQL = SQL + "  where  ";
                SQL = SQL + "  RM_WST_TRANSFER_NO  ='" + TransferNo + "'  AND   AD_FIN_FINYRID  =" + mngrclass.FinYearID;
        
                dtMaster = clsSQLHelper.GetDataTableByCommand(SQL);

                dtMaster.TableName = "WASTE_TRANSFER_PRINT_VIEW";
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

        #region "Validate Vendor document number "


        public DataSet ValidateDocumentNumber(string WasterEntryNo, string WeighBridgeNo, DateTime EntryDate, int FinYearId)
        {
            DataSet dsReturn = new DataSet("RM_WASTER");
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();

                OracleCommand ocCommand = new OracleCommand("PK_RM_WASTE_WEIGHT_DOC_VALID.ValidateWeighBridgeNumber");
                ocCommand.Connection = ocConn;
                ocCommand.CommandType = CommandType.StoredProcedure;

                ////ocCommand.Parameters.Add("WasterEntryNo", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("WasterEntryNo", OracleType.VarChar).Value = WasterEntryNo;

                ////ocCommand.Parameters.Add("WeighBridgeNo", OracleType.VarChar).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("WeighBridgeNo", OracleType.VarChar).Value = WeighBridgeNo;


                //ocCommand.Parameters.Add("entryDate", OracleType.DateTime).Direction = ParameterDirection.Input;
                //ocCommand.Parameters.Add("entryDate", OracleType.DateTime).Value = EntryDate.ToString("dd-MMM-yyyy");

                ////ocCommand.Parameters.Add("finYearId", OracleType.Int32).Direction = ParameterDirection.Input;
                ////ocCommand.Parameters.Add("finYearId", OracleType.Int32).Value = FinYearId;

                ocCommand.Parameters.Add("WasterEntryNo", OracleDbType.Varchar2, WasterEntryNo, ParameterDirection.Input);
                ocCommand.Parameters.Add("WeighBridgeNo", OracleDbType.Varchar2, WeighBridgeNo, ParameterDirection.Input);
                ocCommand.Parameters.Add("entryDate", OracleDbType.Date, EntryDate  , ParameterDirection.Input);                
                ocCommand.Parameters.Add("finYearId", OracleDbType.Decimal, FinYearId, ParameterDirection.Input);
                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);

                odAdpt.Fill(dsReturn);
            }
            catch (Exception ex)
            {

            }
            return dsReturn;
        }
        #endregion 


        
        public DataSet GetDefault(string Type, string Code)
        {
            DataSet dsReturn = new DataSet();

            SqlHelper ClsSqlhelper = new SqlHelper();

            try
            {
                if (Type == "STATION")
                {
                    SQL = " SELECT SALES_STN_STATION_CODE CODE, SALES_STN_STATION_NAME NAME ";
                    SQL = SQL + " FROM SL_STATION_MASTER ";
                    SQL = SQL + " WHERE SALES_STN_DELETESTATUS=0  and SALES_RAW_MATERIAL_STATION ='Y'  order by SALES_STN_STATION_NAME asc";
                }
                else if (Type == "SOURCE")
                {
                    SQL = " SELECT ";
                    SQL = SQL + "RM_SM_SOURCE_CODE CODE, RM_SM_SOURCE_DESC NAME ";
                    SQL = SQL + "FROM RM_SOURCE_MASTER  order by RM_SM_SOURCE_DESC asc ";
                }
                 
                 
                dsReturn = ClsSqlhelper.GetDataset(SQL);

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
            return dsReturn;
        }


        
    }
}

public class WasterTransferEntiy
{
    public WasterTransferEntiy()
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
    public string DestinationCode
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

   public string DumpLocation
    {
        get;
        set;
    }

    public double Qty
    {
        get;
        set;
    }

    public string WeighBridgeNo
    {
        get;
        set;
    }
    public string Sourcecode
    {
        get;
        set;
    }

    public double TripCount
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

