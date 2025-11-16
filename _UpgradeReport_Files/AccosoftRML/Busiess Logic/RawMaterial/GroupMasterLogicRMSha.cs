using System;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Configuration;
using System.Linq;
using System.Web;

using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class GroupMasterLogicRMSha
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());


        //OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public GroupMasterLogicRMSha()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region "Fetch "


        public DataTable FillView()
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_CM_CATEGORY_CODE CODE, RM_CM_CATEGORY_DESC	  NAME , RM_CM_CATEGORY_DESC FIELD_THREE        FROM RM_CATEGORY_MASTER ORDER BY RM_CM_CATEGORY_CODE DESC";


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



        public DataTable FetchData(string sCode)
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the company type master recrods 

                SQL = " SELECT RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE,        RM_CATEGORY_MASTER.RM_CM_CATEGORY_DESC ,";
                SQL = SQL + " RM_CATEGORY_MASTER.RM_CM_INV_ACC_CODE, GL_COA_MASTER.GL_COAM_ACCOUNT_NAME,        RM_CATEGORY_MASTER.RM_CM_CONS_ACC_CODE ,";
                SQL = SQL + " CONSUMPTION.GL_COAM_ACCOUNT_NAME  CON_ACCNAME     ,  RM_CATEGORY_MASTER.RM_CM_INC_ACC_CODE ,";
                SQL = SQL + " INCOME.GL_COAM_ACCOUNT_NAME INCOME_NAME,  RM_CM_CATEGORY_PREFIX,    ";
                SQL = SQL + " RM_CATEGORY_MASTER.RM_CM_COS_ACC_CODE, COSTOFSALES.GL_COAM_ACCOUNT_NAME COSTOFSALNAME,         RM_CATEGORY_MASTER.RM_CM_INC_REMARKS ";
                SQL = SQL + " FROM RM_CATEGORY_MASTER,  ";
                SQL = SQL + " GL_COA_MASTER, GL_COA_MASTER COSTOFSALES, GL_COA_MASTER INCOME ,";
                SQL = SQL + " GL_COA_MASTER CONSUMPTION WHERE ";
                SQL = SQL + " RM_CATEGORY_MASTER.RM_CM_INV_ACC_CODE  = GL_COA_MASTER.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " AND RM_CATEGORY_MASTER.RM_CM_COS_ACC_CODE   = COSTOFSALES.GL_COAM_ACCOUNT_CODE(+) ";
                SQL = SQL + " AND RM_CATEGORY_MASTER.RM_CM_INC_ACC_CODE = INCOME.GL_COAM_ACCOUNT_CODE(+) ";
                SQL = SQL + " AND RM_CATEGORY_MASTER.RM_CM_CONS_ACC_CODE = CONSUMPTION.GL_COAM_ACCOUNT_CODE(+) 	";
                SQL = SQL + " AND RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE ='" + sCode + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

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


        public string GetCode()
        {
            DataTable dtCode = new DataTable();
            try
            {


                String strName = string.Empty;


                OracleCommand ocCommand = new OracleCommand("PK_CODE_CREATION.GetGroupCodeRM");

                // OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());

                ocCommand.Connection = ocConn;

                ocCommand.CommandType = CommandType.StoredProcedure;

                ocCommand.Parameters.Add("cur_return", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                OracleDataAdapter odAdpt = new OracleDataAdapter(ocCommand);
                odAdpt.Fill(dtCode);


            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return System.Convert.ToString(dtCode.Rows[0].ItemArray[0]);
        }




        #region "DML RNM "


        public string InsertSQL(GroupMasterEntityRMSha oEntity, bool bDocAutogenerated, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();



                SQL = " INSERT INTO RM_CATEGORY_MASTER (";
                SQL = SQL + " RM_CM_CATEGORY_CODE,AD_CM_COMPANY_CODE, RM_CM_CATEGORY_DESC, ";
                SQL = SQL + "RM_CM_INC_REMARKS, RM_CM_INV_ACC_CODE, RM_CM_CONS_ACC_CODE, RM_CM_COS_ACC_CODE, ";
                SQL = SQL + " RM_CM_INC_ACC_CODE ,RM_CM_CREATEDBY, ";
                SQL = SQL + " RM_CM_CREATEDDATE,RM_CM_UPDATEDBY, RM_CM_UPDATEDDATE, RM_CM_DELETESTATUS,RM_CM_CATEGORY_PREFIX,RM_INSERT_YN,RM_FILE_GENERATED_YN) ";
                SQL = SQL + " VALUES ('" + oEntity.GroupCode + "' ,'" + mngrclass.CompanyCode + "' ,'" + oEntity.GroupName + "',";
                SQL = SQL + "'" + oEntity.Remarks + "' ,'" + oEntity.InvAccCode + "'  ,'" + oEntity.CostAccCode + "', '" + oEntity.CostAccCode + "',";
                SQL = SQL + "'" + oEntity.IncomeAccCode + "'  ,'" + mngrclass.UserName + "',";
                SQL = SQL + "'" + DateTime.Now.ToString("dd-MMM-yyyy") + "','','',0,'" + oEntity.CategoryPreFix + "','I','N' )";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMCM", oEntity.GroupCode, bDocAutogenerated, Environment.MachineName, "I", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;

        }

        public string UpdateSQL(GroupMasterEntityRMSha oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                sSQLArray.Clear();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "  UPDATE  RM_CATEGORY_MASTER SET ";
                SQL = SQL + "   RM_CM_CATEGORY_DESC = '" + oEntity.GroupName + "' , ";
                SQL = SQL + "   RM_CM_INC_REMARKS ='" + oEntity.Remarks + "'  , ";
                SQL = SQL + "   RM_CM_CATEGORY_PREFIX ='" + oEntity.CategoryPreFix + "'  , ";
                SQL = SQL + "   RM_INSERT_YN='U',RM_FILE_GENERATED_YN='N', ";
                if (UpdateValidate(oEntity.GroupCode) == false)
                {
                    SQL = SQL + "   RM_CM_INV_ACC_CODE ='" + oEntity.InvAccCode + "'  , RM_CM_COS_ACC_CODE ='" + oEntity.CostAccCode + "', ";
                    SQL = SQL + "   RM_CM_INC_ACC_CODE ='" + oEntity.IncomeAccCode + "' , RM_CM_CONS_ACC_CODE ='" + oEntity.CostAccCode + "' ,  ";

                }

                SQL = SQL + "   RM_CM_UPDATEDDATE = '" + DateTime.Now.ToString("dd-MMM-yyyy") + "', RM_CM_UPDATEDBY ='" + mngrclass.UserName + "'";
                SQL = SQL + "  WHERE RM_CM_CATEGORY_CODE = '" + oEntity.GroupCode + "'";

                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMCM", oEntity.GroupCode, false, Environment.MachineName, "I", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;

        }

        public string DeleteSQL(string sCode, object mngrclassobj)
        {
            string sRetun = string.Empty;
            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                OracleHelper oTrns = new OracleHelper();

                sSQLArray.Clear();
                sRetun = DeleteCheck(sCode);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;


                SQL = "  DELETE FROM   RM_CATEGORY_MASTER     ";
                SQL = SQL + "  WHERE RM_CM_CATEGORY_CODE = '" + sCode + "'";



                sSQLArray.Add(SQL);




                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMCM", sCode, false, Environment.MachineName, "D", sSQLArray);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
            }
            finally
            {
                // ocConn.Close();
                // ocConn.Dispose();
            }
            return sRetun;

        }

        private Boolean UpdateValidate(string Code)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 
                string SQLCHK = string.Empty;

                SQLCHK = " SELECT    count(*) CNT   FROM RM_CATEGORY_SUB_MASTER where RM_CM_CATEGORY_CODE='" + Code + "'";

                dsCnt = clsSQLHelper.GetDataset(SQLCHK);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return true;
                }


            }
            catch (Exception ex)
            {
                return false;
            }

            return false;

        }


        private string DeleteCheck(string Code)
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 

                SQL = " SELECT    count(*) CNT   FROM RM_CATEGORY_SUB_MASTER where RM_CM_CATEGORY_CODE='" + Code + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "referernce exists in sub group master";
                }


            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }


        #endregion
    }

    public class GroupMasterEntityRMSha
    {
        public GroupMasterEntityRMSha()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string CategoryPreFix
        {
            get;
            set;
        }

        private string sGroupCode = string.Empty;
        public string GroupCode
        {
            get { return sGroupCode; }
            set { sGroupCode = value; }
        }

        private string sGroupName = string.Empty;
        public string GroupName
        {
            get { return sGroupName; }
            set { sGroupName = value; }
        }

        private string sRemarks = string.Empty;
        public string Remarks
        {
            get { return sRemarks; }
            set { sRemarks = value; }
        }

        private string sInvAccCode = string.Empty;
        public string InvAccCode
        {
            get { return sInvAccCode; }
            set { sInvAccCode = value; }
        }

        private string sCostAccCode = string.Empty;
        public string CostAccCode
        {
            get { return sCostAccCode; }
            set { sCostAccCode = value; }
        }

        private string sIncomeAccCode = string.Empty;
        public string IncomeAccCode
        {
            get { return sIncomeAccCode; }
            set { sIncomeAccCode = value; }
        }

    }
}
