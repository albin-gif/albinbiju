using System;
using System.Data; using AccosoftUtilities;   using AccosoftLogWriter;  using AccosoftNineteenCDL;   
using System.Configuration;
using System.Linq;
using System.Web; 
using System.Xml.Linq;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
/// <summary>
/// Summary description for AccountCode
/// </summary>
/// 
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
 
    public class AccountCodeLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString()); 
    

      //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public AccountCodeLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public DataTable FetchAccountsGroup(string sType)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT GL_COAM_ACCOUNT_CODE CODE, GL_COAM_ACCOUNT_NAME NAME ,GL_COAM_ACCOUNT_GROUP  FIELDTHREE ";
                SQL = SQL + "  FROM GL_COA_MASTER WHERE   ";
                SQL = SQL + "  GL_COAM_LEDGER_TYPE = 'MAIN' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";

               if ((sType == "INV") || (sType == "TRANS")) 
                {
                    SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE =5 ";
                }
                
                else if ( (sType == "COS") || (sType == "CONS")) 
                {
                    SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE IN (   25) ";
                }
                 else if (sType == "FNGDS")
                {
                    SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE  in ( 41) ";
                }   
                else if (sType == "INCOME")
                {
                    SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE  in ( 23, 24) ";
                } 

                SQL = SQL + " ORDER BY  GL_COAM_ACCOUNT_CODE asc  ";

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
        public DataTable FetchAccountsSubGroup(string sType )
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT GL_COAM_ACCOUNT_CODE CODE, GL_COAM_ACCOUNT_NAME NAME ,GL_COAM_ACCOUNT_GROUP  FIELDTHREE ";
                SQL = SQL + "  FROM GL_COA_MASTER , RM_DEFUALTS_PARAMETERS_LOOK_UP WHERE   ";
                SQL = SQL + "  GL_COAM_LEDGER_TYPE = 'SUB' AND GL_COAM_ACCOUNT_STATUS = 'ACTIVE'";
                SQL = SQL + " AND  GL_COA_MASTER.GL_COAM_ACCOUNT_CODE= RM_DEFUALTS_PARAMETERS_LOOK_UP. PARAMETER_VALUE ";

                if (sType == "INV")
                {
                    //SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE =5 ";
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_INVENTORY_ACC_CODE' ";
                }
                else if (sType == "TRANS")  
                {
                   // SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE =5 ";
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_INVENTORY_TRANS_ACC_CODE' ";
                }
                else if (sType == "CONS")
                {
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_CONSUMPTION_ACC_CODE' "; 
                }
                else if (sType == "PRODUCTTYPECONS")
                {
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_CONSUMPTION_ACC_CODE' "; 
                }
                else if (sType == "PRODUCTTYPECONSONSITE")
                {
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_CONSUMPTION_ACC_CODE' "; 
                }
                else if (sType == "COS")
                {
                   // SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE IN (   25) ";
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_COST_OF_SALES_ACC_CODE' "; 
                }

                else if (sType == "INCOME")
                {
                   // SQL = SQL + "  and GL_COAM_ACC_TYPE_CODE  in ( 23, 24) ";
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_INCOME_ACC_CODE' "; 
                } 
                else if (sType == "FNGDS")
                {
                    SQL = SQL + "  and RM_DEFUALTS_PARAMETERS_LOOK_UP.PARAMETER_DESC ='RM_FINSHED_GOODS_ACC_CODE'  "; 
                } 

                SQL = SQL + " ORDER BY  GL_COAM_ACCOUNT_CODE asc  ";

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


    


    }
}