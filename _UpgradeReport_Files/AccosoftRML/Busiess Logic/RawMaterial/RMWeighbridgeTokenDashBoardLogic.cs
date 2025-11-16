using AccosoftLogWriter;
using AccosoftNineteenCDL;
using AccosoftUtilities;
using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
  public class RMWeighbridgeTokenDashBoardLogic
    {
         static string sConString = Utilities.cnnstr;
        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        // OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        //   ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;

        public string UserName
        {
            get;
            set;
        }


        #region "Combo Box"

        public string FillViewStation(string Station)
        {
            string jsonString = "";
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " SELECT   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME, ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE CODE  , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME NAME  , ";
                SQL = SQL + "  SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE  ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   SL_STATION_BRANCH_MAPP_DTLS_VW WHERE   ";
                SQL = SQL + "    SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_PRODUCTION_STATION ='Y'    ";


                if (UserName != "ADMIN" && UserName!="" && Station != "False")
                {
                    SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE in (select AD_BR_CODE from  ";
                    SQL = SQL + "  AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + UserName + "')   ";
               
                    SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";

                    if (Station != "False")
                    {
                        SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE ='" + Station + "'";
                    }


                }
                if (UserName == "")
                {
                    SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE ='" + Station + "'";
                }




                SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME     ASC  ";




                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);

                jsonString = JsonConvert.SerializeObject(dtReturn);

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
            return jsonString;

        }


        #endregion

        #region "DN TRUCK count"

        public string GetPendingTruckGatePass( string Station)
        {
            CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
            DataTable dtMain = new DataTable();
            DataSet dsReturn = new DataSet();
            string jsonString = "";
            SqlHelper clsSQLHelper = new SqlHelper();

            try
            {
                SQL = "  SELECT ";
                SQL = SQL + "        RM_RT_TOKEN_NO,SUBSTR(RM_RT_TOKEN_NO,5,4)  TOKEN   , RM_RECEPIT_TOKEN_MST.RM_VM_VENDOR_CODE,RM_VM_VENDOR_NAME,   ";
                SQL = SQL + "        RM_RECEPIT_TOKEN_MST.AD_FIN_FINYRID, FA_FAM_ASSET_CODE,   ";
                SQL = SQL + "   RM_RECEPIT_TOKEN_MST.RM_RMM_RM_CODE,CASE WHEN RM_RECEPIT_TOKEN_MST.RM_RMM_RM_CODE='16' THEN 'OTHER ITEMS' ELSE  RM_RMM_RM_DESCRIPTION  END AS RM_DESCRIPTION, "; 
                SQL = SQL + "        RM_RT_VEHICLE_DESCRIPTION, RM_RT_DRIVER_CODE, RM_RT_DRIVER_NAME,   ";
                SQL = SQL + "        RM_MRM_RECEIPT_NO,DECODE (RM_RT_STATUS, 'O', 'WAITING ','P','PROGRESS','C','CLOSE') RM_RT_STATUS   ";
                SQL = SQL + "  FROM  ";
                SQL = SQL + "        RM_RECEPIT_TOKEN_MST, rm_vendor_master ,RM_RAWMATERIAL_MASTER  ";
                SQL = SQL + "  WHERE  ";
                SQL = SQL + "        RM_RECEPIT_TOKEN_MST.RM_VM_VENDOR_CODE = rm_vendor_master.RM_VM_VENDOR_CODE   ";
                SQL = SQL + "        AND RM_RECEPIT_TOKEN_MST.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  (+)";
                SQL = SQL + "        AND RM_RT_STATUS in('O','P')   "; 
                SQL = SQL + "        AND RM_RECEPIT_TOKEN_MST.SALES_STN_STATION_CODE = '" + Station + "' ";
                SQL = SQL + "  ORDER BY RM_RMM_RM_CODE ASC ";

                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);
                dsReturn.Tables.Add(dtMain);
                dsReturn.Tables[0].TableName = "RM_RECEPIT_TOKEN_MST";

                jsonString = JsonConvert.SerializeObject(dsReturn);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteSQL(SQL);
                objLogWriter.WriteLog(ex);
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return jsonString;
        }

        #endregion


    }
     
}
