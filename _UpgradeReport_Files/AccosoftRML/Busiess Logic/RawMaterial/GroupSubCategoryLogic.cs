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

/// <summary>
/// Summary description for GroupSubCategoryLogic
/// </summary>
/// 
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class GroupSubCategoryLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());



        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public GroupSubCategoryLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public DataSet FillCombo ( )
        {
            DataSet dsData = new DataSet();
            DataTable dtGrade = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_CM_CATEGORY_CODE CAT_COE, RM_CM_CATEGORY_DESC	  CATE_NAME   FROM RM_CATEGORY_MASTER";
                dtGrade = clsSQLHelper.GetDataTableByCommand(SQL);
                // GRADE
                dsData.Tables.Add(dtGrade);
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
            return dsData;
        }

        public DataTable FillView ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_SCM_SUBCATEGORY_CODE CODE, RM_SCM_SUBCATEGORY_DESC NAME , RM_SCM_INV_ACC_CODE FIELD_THREE ,";
                SQL = SQL + " RM_SCM_ITEM_CODE_GROUP CODE_GROUP,DECODE(RM_SCM_SUBCAT_CODE_PATTERN_YN,'Y','YES','NO') PATTERN_YN, ";
                SQL = SQL + " RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME ";

                SQL = SQL + " FROM RM_CATEGORY_SUB_MASTER,PC_BUD_RESOURCE_MASTER ";
                SQL = SQL + " WHERE RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+) ";
                SQL = SQL + " ORDER BY RM_SCM_SUBCATEGORY_CODE DESC";


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

        public DataTable FillBudgetBreakDownView ( )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT PC_BUD_RESOURCE_CODE CODE, PC_BUD_RESOURCE_NAME NAME  FROM PC_BUD_RESOURCE_MASTER";

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

        public DataTable FetchData ( string sCode )
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the company type master recrods 

                SQL = " SELECT RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE, RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_DESC,";
                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_CM_CATEGORY_CODE, RM_CATEGORY_MASTER.RM_CM_CATEGORY_DESC,";
                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_SCM_REMARKS, RM_SCM_ITEM_CODE_GROUP ,  ";

                SQL = SQL + " RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE RESOURCE_CODE,PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME RESOURCE_NAME, ";

                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_SCM_INV_ACC_CODE,";
                SQL = SQL + " 	INV.GL_COAM_ACCOUNT_NAME INVENTORY_ACC_NAME,";
                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_SCM_COS_ACC_CODE,";
                SQL = SQL + " 	COSTOFSALES.GL_COAM_ACCOUNT_NAME COST_ACCOUNT_NAME,";

                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_SCM_CONS_ACC_CODE,";

                SQL = SQL + " RM_CATEGORY_SUB_MASTER.RM_SCM_INC_ACC_CODE ,  ";
                SQL = SQL + " 	INCOME.GL_COAM_ACCOUNT_NAME INCOME_ACCOUNT_NAME,";
                SQL = SQL + "   CONS.GL_COAM_ACCOUNT_NAME CONS_ACC_NAME,RM_SCM_SUBCAT_CODE_PATTERN_YN ";

                SQL = SQL + " FROM RM_CATEGORY_MASTER , RM_CATEGORY_SUB_MASTER ,   ";
                SQL = SQL + " 	GL_COA_MASTER INV,";
                SQL = SQL + " 	GL_COA_MASTER COSTOFSALES,";
                SQL = SQL + " 	GL_COA_MASTER CONS,";
                SQL = SQL + "   GL_COA_MASTER INCOME,";
                SQL = SQL + "   PC_BUD_RESOURCE_MASTER";
                SQL = SQL + " WHERE  RM_CATEGORY_SUB_MASTER.RM_CM_CATEGORY_CODE = RM_CATEGORY_MASTER.RM_CM_CATEGORY_CODE  ";
                SQL = SQL + " 	AND  RM_CATEGORY_SUB_MASTER.RM_SCM_INV_ACC_CODE =INV.GL_COAM_ACCOUNT_CODE  (+)";
                SQL = SQL + " 	AND  RM_CATEGORY_SUB_MASTER.RM_SCM_COS_ACC_CODE =COSTOFSALES.GL_COAM_ACCOUNT_CODE  (+)";
                SQL = SQL + " 	AND  RM_CATEGORY_SUB_MASTER.RM_SCM_CONS_ACC_CODE =CONS.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " 	AND  RM_CATEGORY_SUB_MASTER.RM_SCM_INC_ACC_CODE =INCOME.GL_COAM_ACCOUNT_CODE (+) ";
                SQL = SQL + " 	AND  RM_CATEGORY_SUB_MASTER.PC_BUD_RESOURCE_CODE= PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+)";
                SQL = SQL + "   AND  RM_CATEGORY_SUB_MASTER.RM_SCM_SUBCATEGORY_CODE ='" + sCode + "'";


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

        public DataTable fnValidateChildTable ( string txtSubCatCode )
        {
            DataSet dData = new DataSet();
            DataTable dsChild = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " 	SELECT    *  ";
                SQL = SQL + " 	FROM   RM_RAWMATERIAL_MASTER ";
                SQL = SQL + " 	WHERE    RM_SCM_SUBCATEGORY_CODE =  '" + txtSubCatCode + "'";

                dsChild = clsSQLHelper.GetDataTableByCommand(SQL);


            }
            catch (Exception ex)
            {
                //ScriptManager.RegisterClientScriptBlock(this, this.GetType(), "InsertData", "alert('" + ex.Message.ToString() + "');", true);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dsChild;
        }

        public string GetCode ( )
        {
            DataTable dtCode = new DataTable();
            try
            {


                String strName = string.Empty;


                OracleCommand ocCommand = new OracleCommand("PK_CODE_CREATION.GetGroupSubCodeRM");

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

        public string InsertSQL ( string SUBCATEGORY_CODE, string SUBCATEGORY_DESC, string CATEGORY_CODE, string REMARKS,
            string RESOURCE_CODE, string ITEM_CODE_GROUP, string INV_ACC_CODE, string COS_ACC_CODE, string INC_ACC_CODE,
            bool bDocAutogenerated, object mngrclassobj, string PrecastCodePattern )
        {
            string sRetun = string.Empty;

            SessionManager mngrclass = (SessionManager)mngrclassobj;

            try
            {
                OracleHelper oTrns = new OracleHelper();

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_CATEGORY_SUB_MASTER (";
                SQL = SQL + " RM_SCM_SUBCATEGORY_CODE, AD_CM_COMPANY_CODE, RM_SCM_SUBCATEGORY_DESC, ";
                SQL = SQL + " RM_CM_CATEGORY_CODE, RM_SCM_REMARKS, RM_SCM_INV_ACC_CODE, ";
                SQL = SQL + "RM_SCM_COS_ACC_CODE,RM_SCM_INC_ACC_CODE, RM_SCM_CONS_ACC_CODE,  ";
                SQL = SQL + " RM_SCM_CREATEDBY, RM_SCM_CREATEDDATE, RM_SCM_UPDATEDBY, ";
                SQL = SQL + " RM_SCM_UPDATEDDATE, RM_SCM_DELETESTATUS,PC_BUD_RESOURCE_CODE ,RM_SCM_ITEM_CODE_GROUP,RM_SCM_SUBCAT_CODE_PATTERN_YN,RM_INSERT_YN,RM_FILE_GENERATED_YN ) ";


                SQL = SQL + " VALUES ('" + SUBCATEGORY_CODE + "' ,'" + mngrclass.CompanyCode + "' ,'" + SUBCATEGORY_DESC + "',";
                SQL = SQL + "'" + CATEGORY_CODE + "','" + REMARKS + "'  , '" + INV_ACC_CODE + "'  ,";
                SQL = SQL + "'" + COS_ACC_CODE + "'  ,'" + INC_ACC_CODE + "','" + COS_ACC_CODE + "' ,";
                SQL = SQL + "'" + mngrclass.UserName + "','" + DateTime.Now.ToString("dd-MMM-yyyy") + "' ,'' ,";
                SQL = SQL + "'',0,'" + RESOURCE_CODE + "','" + ITEM_CODE_GROUP + "', '" + PrecastCodePattern + "','I','N' )";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSCM", SUBCATEGORY_CODE, bDocAutogenerated, Environment.MachineName, "I", sSQLArray);
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

        public string UpdateSQL ( string SUBCATEGORY_CODE, string SUBCATEGORY_DESC, string CATEGORY_CODE, string REMARKS,
            string RESOURCE_CODE, string ITEM_CODE_GROUP,
            string INV_ACC_CODE, string COS_ACC_CODE, string INC_ACC_CODE, object mngrclassobj, string PrecastCodePattern )
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                sSQLArray.Clear();
                SessionManager mngrclass = (SessionManager)mngrclassobj;


                SQL = " UPDATE RM_CATEGORY_SUB_MASTER set ";
                SQL = SQL + "    RM_SCM_SUBCATEGORY_DESC = '" + SUBCATEGORY_DESC + "', ";
                SQL = SQL + "    RM_CM_CATEGORY_CODE = '" + CATEGORY_CODE + "', RM_SCM_REMARKS ='" + REMARKS + "' , RM_SCM_INV_ACC_CODE ='" + INV_ACC_CODE + "',";
                SQL = SQL + "    RM_SCM_COS_ACC_CODE ='" + COS_ACC_CODE + "', RM_SCM_INC_ACC_CODE = '" + INC_ACC_CODE + "' , RM_SCM_CONS_ACC_CODE ='" + COS_ACC_CODE + "' ,";
                SQL = SQL + "      RM_INSERT_YN='U',RM_FILE_GENERATED_YN='N',";
                SQL = SQL + "      RM_SCM_UPDATEDBY= '" + mngrclass.UserName + "', ";
                SQL = SQL + "    RM_SCM_UPDATEDDATE ='" + DateTime.Now.ToString("dd-MMM-yyyy") + "',RM_SCM_SUBCAT_CODE_PATTERN_YN='" + PrecastCodePattern + "',";
                SQL = SQL + "    PC_BUD_RESOURCE_CODE='" + RESOURCE_CODE + "' ,RM_SCM_ITEM_CODE_GROUP = '" + ITEM_CODE_GROUP + "'";

                SQL = SQL + "    WHERE  RM_SCM_SUBCATEGORY_CODE = '" + SUBCATEGORY_CODE + "'";

                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSCM", SUBCATEGORY_CODE, false, Environment.MachineName, "U", sSQLArray);

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

        public string DeleteSQL ( string sCode, object mngrclassobj )
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                sRetun = DeleteCheck(sCode);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;


                SQL = " DELETE FROM  RM_CATEGORY_SUB_MASTER   ";

                SQL = SQL + " where  RM_SCM_SUBCATEGORY_CODE = '" + sCode + "'";

                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMSCM", sCode, false, Environment.MachineName, "D", sSQLArray);
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

        private string DeleteCheck ( string Code )
        {
            DataSet dsCnt = new DataSet();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                // SQL Statment for fethiing the  opening 

                SQL = " SELECT    count(*) CNT   FROM RM_RAWMATERIAL_MASTER where RM_CM_CATEGORY_CODE='" + Code + "'";

                dsCnt = clsSQLHelper.GetDataset(SQL);

                if (System.Convert.ToInt16(dsCnt.Tables[0].Rows[0].ItemArray[0]) > 0)
                {
                    return "cannot delete, referernce exists in raw material master";
                }
            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

    }
}