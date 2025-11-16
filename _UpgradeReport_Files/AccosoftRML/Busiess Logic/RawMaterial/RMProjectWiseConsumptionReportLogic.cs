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
using System.Globalization;


namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class RMProjectWiseConsumptionReportLogic
    {

        static string sConString = Utilities.cnnstr;
        
        OracleConnection ocConn = new OracleConnection(sConString.ToString());
      
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public RMProjectWiseConsumptionReportLogic()
        {
            
        }
        public string ProjectCode
        {
            get;
            set;
        }
        public string Resource
        {
            get;
            set;
        }



        public DateTime FromDate
        {
            get;
            set;
        }

        public DateTime ToDate
        {
            get;
            set;
        }


        public DataSet FetchProjectWiseConsumptionDetails()
        {
            DataSet dsType = new DataSet();
            DataTable dtTable1 = new DataTable();
            DataTable dtTable2 = new DataTable();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                string sRetun = string.Empty;

                SQL = " select  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO,RM_PSPR_ENTRY_DATE, ";
                SQL = SQL + " RM_PSPR_FROM_DATE,RM_PSPR_TO_DATE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE,SALES_PM_PROJECT_NAME, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE RM_Code,  ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE,PC_BUD_RESOURCE_NAME,  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED,  ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_FACTOR,  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_BASIC,RM_PSPD_CONSUM_THEORITICAL, RM_PSED_AVG_PRICE_NEW, ";
                SQL = SQL + " RM_PSPD_CONSUMPTION RM_PSPD_CONSUMPTION , ";
                SQL = SQL + " RM_PSPD_CONSUMPTION*RM_PSED_AVG_PRICE_NEW RMITEMAMOUNT ";
                SQL = SQL + " from   ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_MASTER,RM_PHYSICAL_STOCK_PROJ_DETAILS,  ";
                SQL = SQL + " RM_UOM_MASTER, RM_RAWMATERIAL_MASTER ,RM_PHYSICAL_STOCK_MASTER,RM_PHYSICAL_STOCK_DETAILS, ";
                SQL = SQL + " CN_ENQUIRY_MASTER,PC_BUD_RESOURCE_MASTER ";
                SQL = SQL + " where   ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO=RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO=RM_PHYSICAL_STOCK_MASTER.RM_PSPRM_ENTRY_NO ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO=RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE   ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE  ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE  ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE =CN_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " and RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE=PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+) ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPR_ENTRY_DATE between'" + FromDate.ToString("dd-MMM-yyyy") + "' AND '" + ToDate.ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(ProjectCode))
                {
                    SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE in ( '" + ProjectCode + "')";
                }

                if (!string.IsNullOrEmpty(Resource))
                {
                    SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE  in('" + Resource + "' )  ";
                }

                //SQL = SQL + " GROUP BY  RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE,SALES_PM_PROJECT_NAME, ";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE,  ";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,RM_PSED_AVG_PRICE_NEW  ";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE,PC_BUD_RESOURCE_NAME ";
                SQL = SQL + " ORDER BY SALES_PM_PROJECT_CODE, RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE ";




                dsType = clsSQLHelper.GetDataset(SQL);

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
            return dsType;

        }

        public DataSet FetchTotalConsumptionDetails()
        {
            DataSet dsType = new DataSet();
            DataTable dtTable1 = new DataTable();
            DataTable dtTable2 = new DataTable();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                string sRetun = string.Empty;
               
                SQL =  " select     ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE,PC_BUD_RESOURCE_NAME,  RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE,RM_RMM_RM_DESCRIPTION, RM_PSED_AVG_PRICE_NEW,   ";
                SQL = SQL + " ((RM_PSED_CLOSING)-(RM_PSED_STOCK_ON_HAND+RM_PSED_RECEIVED))*-1 ";
                SQL = SQL + "  RM_PSPD_CONSUMPTION,   ";
                SQL = SQL + " (((RM_PSED_CLOSING)-(RM_PSED_STOCK_ON_HAND+RM_PSED_RECEIVED))*-1 )* RM_PSED_AVG_PRICE_NEW   ";
                SQL = SQL + " RMITEMAMOUNT   ";
                SQL = SQL + " from    ";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER,RM_PHYSICAL_STOCK_DETAILS,  RM_RAWMATERIAL_MASTER ,   ";
                SQL = SQL + " PC_BUD_RESOURCE_MASTER    ";
                SQL = SQL + " where      ";
                SQL = SQL + " RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO=RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO    ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_NO=RM_PHYSICAL_STOCK_DETAILS.RM_PSEM_ENTRY_NO     ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE     ";
                SQL = SQL + " and RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE=PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE(+)    ";
                SQL = SQL + " and RM_PSEM_APPROVED='Y'   ";
                SQL = SQL + " and RM_PHYSICAL_STOCK_MASTER.RM_PSEM_ENTRY_DATE between'" + FromDate.ToString("dd-MMM-yyyy") + "' AND '" + ToDate.ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and RM_PHYSICAL_STOCK_MASTER.SALES_STN_STATION_CODE='01'  ";
                if (!string.IsNullOrEmpty(Resource))
                {
                    SQL = SQL + " AND  RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE  in('" + Resource + "' )  ";
                }

                SQL = SQL + " ORDER BY     ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.PC_BUD_RESOURCE_CODE ,RM_PHYSICAL_STOCK_DETAILS.RM_RMM_RM_CODE  ";

                dsType = clsSQLHelper.GetDataset(SQL);

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
            return dsType;

        }

        public DataTable FillViewLookUp(string Type)
        {
            DataTable dtData = new DataTable();
            try
            {

                SqlHelper clsSQLHelper = new SqlHelper();

            if (Type == "Project")
            {
                SQL = "  SELECT   ";
                SQL = SQL + "   CN_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE CODE,";
                SQL = SQL + "   CN_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME  NAME,";
                SQL = SQL + "   CN_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE CustCode, ";
                SQL = SQL + "   SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_NAME CustName ";
                SQL = SQL + " FROM ";
                SQL = SQL + "   CN_ENQUIRY_MASTER,SL_CUSTOMER_MASTER ";
                SQL = SQL + " WHERE  ";
                SQL = SQL + "   CN_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE = SL_CUSTOMER_MASTER.SALES_CUS_CUSTOMER_CODE";
                SQL = SQL + "   AND SALES_INQM_LIVE_APPROVED = 'Y' ";
                SQL = SQL + "   AND SALES_INQM_BLOCKED = 'N'";
                SQL = SQL + " ORDER BY ";
                SQL = SQL + "   CN_ENQUIRY_MASTER.SALES_CUS_CUSTOMER_CODE ASC";
            }
             else  if (Type == "Resource")
             {
                 SQL = "  SELECT   ";
                 SQL = SQL + "   PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE CODE,";
                 SQL = SQL + "   PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_NAME  NAME ";
                 SQL = SQL + " FROM ";
                 SQL = SQL + "   PC_BUD_RESOURCE_MASTER ";
                 SQL = SQL + " ORDER BY ";
                 SQL = SQL + "   PC_BUD_RESOURCE_MASTER.PC_BUD_RESOURCE_CODE ASC";
             }

                dtData = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
                return null;
            }
            finally
            {
                //ocConn.Close();
                //ocConn.Dispose();
            }
            return dtData;
        }

    }
}
