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
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Globalization;
using System.Collections.Generic;
// Manu : USING FOR PROJECT WISE CONSUMPTION 
// WHILE doing the consumption posting its posting based one each coset center wise 
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class PhysicalStockEntryProjectPRCLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;
        public DataSet dsUOMConv = new DataSet();
        public DataTable dtConsumption = new DataTable("TECH_CONS_TEMP");

        OracleConnection ocConn = new OracleConnection(sConString.ToString());
         
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;
        public DateTime BatchFromDate
        {
            get;
            set;
        }
        public DateTime BatchToDate
        {
            get;
            set;
        }

        public string FilterType
        {
            get;
            set;
        }
        string fsInvDesc = null;
        double lobjFactor = 0;

        System.DateTime dtTrDate = default(System.DateTime);

        //static DataSet dsComposistion=new DataSet();


        #region "look up "

        public DataTable FillSearchView(string sFilterType, object mngrclassobj)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " SELECT ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO ENTRYNO, AD_FIN_FINYRID FINID,  to_char(RM_PSPR_ENTRY_DATE,'DD-MON-YYYY') ENTRYDATE, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE STCODE,SALES_STN_STATION_NAME STNAME,  to_char(RM_PSPR_FROM_DATE,'DD-MON-YYYY') FROMDATE, ";
                SQL = SQL + " to_char(RM_PSPR_TO_DATE,'DD-MON-YYYY') TODATE,   ";
                SQL = SQL + " RM_PSPR_REMARKS REMARKS  ";

                SQL = SQL + " FROM  RM_PHYSICAL_STOCK_PROJ_MASTER,SL_STATION_MASTER  ";

                SQL = SQL + "    WHERE  ";
                SQL = SQL + "    RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "    AND SL_STATION_MASTER.SALES_STN_STATION_CODE=RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE ";

                if (sFilterType == "APPROVED")
                {
                    SQL = SQL + "  And RM_PSPR_APPROVED ='Y'";

                }
                else if (sFilterType == "NOT APPROVED")
                {
                    SQL = SQL + "  And RM_PSPR_APPROVED ='N'";
                }

                SQL = SQL + "  order by RM_PSPRM_ENTRY_NO   desc";


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

        public DataTable FillBranchAndStationUserGranted ( string UserName, string SelectedBranchCode = "" )
        {

            DataTable dt = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                //   SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = "SELECT SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE, SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_NAME,  ";
                SQL = SQL + "          SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_NAME ,  ";
                SQL = SQL + "          SL_STATION_BRANCH_MAPP_DTLS_VW.GL_COSM_ACCOUNT_CODE   ";
                SQL = SQL + "     FROM SL_STATION_BRANCH_MAPP_DTLS_VW  ";
                SQL = SQL + "    WHERE SALES_STATION_BRANCH_STATUS_AI ='A' and SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_RAW_MATERIAL_STATION ='Y' and AD_BR_PRECAST_VISIBILE_YN = 'Y'  ";

                if (!string.IsNullOrEmpty(SelectedBranchCode))
                {
                    SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE ='" + SelectedBranchCode + "'";
                }

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   AND SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE in (select AD_BR_CODE from  ";
                    SQL = SQL + "  AD_USER_GRANTED_BRANCH where AD_UM_USERID = '" + UserName + "')   ";
                }
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND SL_STATION_BRANCH_MAPP_DTLS_VW.SALES_STN_STATION_CODE IN ( SELECT     SALES_STN_STATION_CODE FROM ";
                    SQL = SQL + "     WS_GRANTED_STATION_USERS WHERE    AD_UM_USERID   ='" + UserName + "') ";
                }
                SQL = SQL + "  ORDER BY   SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_CODE , SL_STATION_BRANCH_MAPP_DTLS_VW.AD_BR_SORT_ID    ASC  ";



                dt = clsSQLHelper.GetDataTableByCommand(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }

        #endregion

      

        #region "  Fetch Opening  balances / RVD qty / stk in / stk out / sales / consumption

        public DataSet FetchProjectWiseConsumption_oldWithLiveConsData( DateTime dDnFromDate, DateTime dToDate)
        {
            DataSet dsReturn = new DataSet("CONSUMPTION");
            DataTable dtDnData = new DataTable();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

              //  #region "Mix Details Consumptions"

                SQL = " SELECT PROJCODE SALES_PM_PROJECT_CODE ,PROJNAME,RM_CODE RM_RMM_RM_CODE , RM_NAME, STN_CODE, STN_NAME,";
                SQL = SQL + "  FROM_UOM_CODE TECH_PCD_UOM_CODE , FROM_UOM_NAME, ";
                SQL = SQL + " TO_BASIC_UOM RM_UM_UOM_CODE , TO_UOM_DESC, ";
                SQL = SQL + "  SUM (WEIGHT_FOR_BATCH) WEIGHT_FOR_BATCH, ";
                SQL = SQL + " SUM (WEIGHT_FOR_CONUMPTION) TECT_CONTMP_THR_CON_QTY  ";
                SQL = SQL + " FROM ( ";
                SQL = SQL + "  SELECT PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE PROJCODE, ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJNAME, ";
                SQL = SQL + " TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION RM_NAME, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE STN_CODE, '' STN_NAME, ";
                SQL = SQL + " TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE FROM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC FROM_UOM_NAME, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE TO_BASIC_UOM, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC TO_UOM_DESC, ";
                SQL = SQL + " SUM (TECH_PCD_WEIGHT_FROM * pc_element_master.PC_ELM_VOLUME  ) WEIGHT_FOR_BATCH, ";
                SQL = SQL + " SUM (TECH_PCD_WEIGHT_FROM * pc_element_master.PC_ELM_VOLUME  ) WEIGHT_FOR_CONUMPTION ";
                SQL = SQL + " FROM PC_PRODUCTION_MASTER, pc_element_master, TECH_PRODUCT_COMP_DETAILS, ";
                SQL = SQL + " RM_UOM_MASTER MIX_DETAILS_UOM_MASTER,  RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " RM_UOM_MASTER,PC_ENQUIRY_MASTER ";
                SQL = SQL + " WHERE PC_PRODUCTION_MASTER.TECH_PM_PRODUCT_CODE =   TECH_PRODUCT_COMP_DETAILS.TECH_PM_PRODUCT_CODE ";
                SQL = SQL + " AND TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE =  MIX_DETAILS_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =  RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE =   PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE = pc_element_master.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLDG_CODE =  pc_element_master.PC_BLDG_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLD_CODE =   pc_element_master.PC_BLD_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_FLR_CODE =   pc_element_master.PC_FLR_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_ELM_CODE =   pc_element_master.PC_ELM_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_PHYSC_BATCH_DONE = 'Y' ";

                SQL = SQL + " AND PC_PROD_DOC_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
                //if (!string.IsNullOrEmpty(sFilter))
                //{
                //    SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE IN( '" + sFilter + "')";
                //}

                SQL = SQL + " GROUP BY ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE , ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME , ";
                SQL = SQL + " TECH_PRODUCT_COMP_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + " TECH_PRODUCT_COMP_DETAILS.RM_UOM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC ";

                SQL = SQL + " UNION ALL ";

                SQL = SQL + " SELECT PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE PROJCODE, ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJNAME, ";
                SQL = SQL + " PC_ELEMENT_RIFCOMP_DETAILS.RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION RM_NAME, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " '' STN_NAME, ";
                SQL = SQL + " PC_ELEMENT_RIFCOMP_DETAILS.RM_UOM_UOM_CODE FROM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC FROM_UOM_NAME, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE TO_BASIC_UOM, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC TO_UOM_DESC, ";
                SQL = SQL + " SUM (PC_ELMRIF_WEIGHT_FROM * PC_PROD_QTY_PRODUCED ";
                SQL = SQL + " ) WEIGHT_FOR_BATCH, ";
                SQL = SQL + " SUM (PC_ELMRIF_WEIGHT_FROM * PC_PROD_QTY_PRODUCED ";
                SQL = SQL + " ) WEIGHT_FOR_CONUMPTION ";
                SQL = SQL + " FROM PC_PRODUCTION_MASTER, ";
                SQL = SQL + " PC_ELEMENT_RIFCOMP_DETAILS, ";
                SQL = SQL + " RM_UOM_MASTER MIX_DETAILS_UOM_MASTER, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " RM_UOM_MASTER,PC_ENQUIRY_MASTER ";
                SQL = SQL + " WHERE PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE = PC_ELEMENT_RIFCOMP_DETAILS.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLDG_CODE =  PC_ELEMENT_RIFCOMP_DETAILS.PC_BLDG_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLD_CODE = PC_ELEMENT_RIFCOMP_DETAILS.PC_BLD_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_FLR_CODE =   PC_ELEMENT_RIFCOMP_DETAILS.PC_FLR_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_ELM_CODE =   PC_ELEMENT_RIFCOMP_DETAILS.PC_ELM_CODE ";
                SQL = SQL + " AND PC_ELEMENT_RIFCOMP_DETAILS.RM_UOM_UOM_CODE =  MIX_DETAILS_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =  RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = PC_ELEMENT_RIFCOMP_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE =  PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_PHYSC_BATCH_DONE = 'Y' ";

                SQL = SQL + " AND PC_PROD_DOC_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
                //if (!string.IsNullOrEmpty(sFilter))
                //{
                //    SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE IN( '" + sFilter + "')";
                //}

                SQL = SQL + " GROUP BY PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE , ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME , ";
                SQL = SQL + " PC_ELEMENT_RIFCOMP_DETAILS.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + " PC_ELEMENT_RIFCOMP_DETAILS.RM_UOM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC ";
                SQL = SQL + " UNION ALL ";
                SQL = SQL + " SELECT PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE PROJCODE, ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJNAME, ";
                SQL = SQL + " PC_ELEMENT_OTHER_RMITEM_DET.RM_RMM_RM_CODE RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION RM_NAME, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE STN_CODE, ";
                SQL = SQL + " '' STN_NAME, ";
                SQL = SQL + " PC_ELEMENT_OTHER_RMITEM_DET.RM_UOM_UOM_CODE FROM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC FROM_UOM_NAME, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE TO_BASIC_UOM, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC TO_UOM_DESC, ";
                SQL = SQL + " SUM (PC_ELMOIDRM_TOT_QTY * PC_PROD_QTY_PRODUCED ) WEIGHT_FOR_BATCH, ";
                SQL = SQL + " SUM (PC_ELMOIDRM_TOT_QTY * PC_PROD_QTY_PRODUCED  ) WEIGHT_FOR_CONUMPTION ";
                SQL = SQL + " FROM PC_PRODUCTION_MASTER, ";
                SQL = SQL + " PC_ELEMENT_OTHER_RMITEM_DET, ";
                SQL = SQL + " RM_UOM_MASTER MIX_DETAILS_UOM_MASTER, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER, ";
                SQL = SQL + " RM_UOM_MASTER,PC_ENQUIRY_MASTER ";
                SQL = SQL + " WHERE PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE =  PC_ELEMENT_OTHER_RMITEM_DET.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLDG_CODE =   PC_ELEMENT_OTHER_RMITEM_DET.PC_BLDG_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_BLD_CODE =   PC_ELEMENT_OTHER_RMITEM_DET.PC_BLD_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_FLR_CODE =   PC_ELEMENT_OTHER_RMITEM_DET.PC_FLR_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_ELM_CODE = PC_ELEMENT_OTHER_RMITEM_DET.PC_ELM_CODE ";
                SQL = SQL + " AND PC_ELEMENT_OTHER_RMITEM_DET.RM_UOM_UOM_CODE =  MIX_DETAILS_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE =   RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = PC_ELEMENT_OTHER_RMITEM_DET.RM_RMM_RM_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE =   PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " AND PC_PRODUCTION_MASTER.PC_PHYSC_BATCH_DONE = 'Y' ";
                SQL = SQL + " AND PC_PROD_DOC_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
 

                SQL = SQL + " GROUP BY PC_PRODUCTION_MASTER.SALES_PM_PROJECT_CODE , ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME , ";
                SQL = SQL + " PC_ELEMENT_OTHER_RMITEM_DET.RM_RMM_RM_CODE, ";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " PC_PRODUCTION_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + " PC_ELEMENT_OTHER_RMITEM_DET.RM_UOM_UOM_CODE, ";
                SQL = SQL + " MIX_DETAILS_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC) ";
                SQL = SQL + " GROUP BY ";
                SQL = SQL + " PROJCODE,PROJNAME,RM_CODE,RM_NAME,STN_CODE,STN_NAME, ";
                SQL = SQL + " FROM_UOM_CODE,FROM_UOM_NAME,TO_BASIC_UOM,TO_UOM_DESC ";


                dtDnData = clsSQLHelper.GetDataTableByCommand(SQL);

                //LoadDefaultDatasets();
                //prDefineDataTable();

                // lppoping for dattable s 


                //double dweight_for_batch = 0;
                //double dweight_for_conumption = 0;

                //DataRow drRowMaster;
                //SortedList sConsQtyReturn = new SortedList();

                //for (int i = 0; i <= dtDnData.Rows.Count - 1; i++)
                //{
                //    dweight_for_batch = 0;
                //    dweight_for_conumption = 0;

                //    dweight_for_batch = double.Parse(dtDnData.Rows[i]["WEIGHT_FOR_BATCH"].ToString());
                //    dweight_for_conumption = double.Parse(dtDnData.Rows[i]["WEIGHT_FOR_CONUMPTION"].ToString());

                //    sConsQtyReturn = prGetConsQty(dtDnData.Rows[i]["RM_CODE"].ToString(), dtDnData.Rows[i]["FROM_UOM_CODE"].ToString(), dtDnData.Rows[i]["TO_BASIC_UOM"].ToString(), dweight_for_batch, dweight_for_conumption); //'Passing ByRef Variable

                //    drRowMaster = dtConsumption.NewRow();
                //    drRowMaster["SALES_PM_PROJECT_CODE"] = dtDnData.Rows[i]["PROJCODE"].ToString();
                //    drRowMaster["SALES_PM_PROJECT_NAME"] = dtDnData.Rows[i]["PROJNAME"].ToString();

                //    drRowMaster["RM_RMM_RM_CODE"] = dtDnData.Rows[i]["RM_CODE"].ToString();
                //    drRowMaster["RM_RMM_RM_DESCRIPTION"] = dtDnData.Rows[i]["RM_NAME"].ToString();

                //    drRowMaster["SALES_STN_STATION_CODE"] = dtDnData.Rows[i]["STN_CODE"].ToString();
                //    drRowMaster["SALES_STN_STATION_NAME"] = dtDnData.Rows[i]["STN_NAME"].ToString();

                //    drRowMaster["PR_DLYN_THEOR_CONS_QTY"] = dweight_for_batch; // before doing the conversion 
                //    drRowMaster["PR_DLYN_ACTUAL_CONS_QTY"] = dweight_for_conumption;  // befor ding the conversion 

                //    drRowMaster["CONVERSION_FACTOR"] = double.Parse(sConsQtyReturn["CONVERSION_FACTOR"].ToString());

                //    drRowMaster["TECT_CONTMP_THR_CON_QTY"] = double.Parse(sConsQtyReturn["WEIGHT_FOR_BATCH"].ToString());
                //    drRowMaster["TECT_CONTMP_ACT_CON_QTY"] = double.Parse(sConsQtyReturn["WEIGHT_FOR_CONSUMPTION"].ToString());

                //    drRowMaster["TECH_PCD_UOM_CODE"] = dtDnData.Rows[i]["FROM_UOM_CODE"].ToString();  // From UoM code 
                //    drRowMaster["MIX_UM_UOM_DESC"] = dtDnData.Rows[i]["FROM_UOM_NAME"].ToString();    //  From UOM Name 

                //    drRowMaster["RM_UM_UOM_CODE"] = dtDnData.Rows[i]["TO_BASIC_UOM"].ToString();   // To Uom Code 
                //    drRowMaster["RM_UM_UOM_DESC"] = dtDnData.Rows[i]["TO_UOM_DESC"].ToString();     // To UOM Name 


                //    dtConsumption.Rows.Add(drRowMaster);
                //}
                //#endregion
                //=========================================BBS DETAILS =====================================



                dtDnData.TableName = "CONSUMPTION";
                dsReturn.Tables.Add(dtDnData);

            }
            catch (Exception ex)
            {

                return null;
            }

            return dsReturn;
        }



        public DataSet FetchProjectWiseConsumption(DateTime dDnFromDate, DateTime dToDate, string ddlStation  )
        {
            DataSet dsReturn = new DataSet("CONSUMPTION");
            DataTable dtDnData = new DataTable();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SqlHelper clsSQLHelper = new SqlHelper();

                //  #region "Mix Details Consumptions"

                // jomy added the old code cometrend and newely added thsi code 25 02 2025 


 

                SQL = " select     ";
                SQL = SQL + "            ACCOUNT_CODE SALES_PM_PROJECT_CODE   , ";
                SQL = SQL + "            ACCOUNT_NAME PROJNAME , ";
                SQL = SQL + "            STATION_CODE STN_CODE , ";
                SQL = SQL + "            STATION_NAME STN_NAME ,  ";
                SQL = SQL + "            RMCODE RM_RMM_RM_CODE  , ";
                SQL = SQL + "            RMDESC RM_NAME , ";
                SQL = SQL + "            UOM_CODE TECH_PCD_UOM_CODE  , ";
                SQL = SQL + "            UOM_DESC FROM_UOM_NAME , TO_BASIC_UOM    RM_UM_UOM_CODE,    TO_UOM_DESC,  ";
                SQL = SQL + "            SUM (QUANTITY)          WEIGHT_FOR_BATCH, ";
                SQL = SQL + "            SUM (QUANTITY)     TECT_CONTMP_THR_CON_QTY  ";
                SQL = SQL + "        from  ";
                SQL = SQL + "        RM_STORE_ISSUE_VOUCHER_PRINT  WHERE  APPROVED_YN ='Y'";
                    // and  STATION_CODE  ='" + ddlStation  + "'";  REMOVED STATAIO CODE SICE ONLY ONE STATION WE NEED TO PASS THE OCNSUMPTIN, OTHER STATION NO RECIEPT 
                    // AND MATERISAL ISSUE SITE NTO KEEPING ANY STOCK LWEDGER TRANSAXCATINOS JOMY 28 02 2025  / /LONG DICUSSIN WITH RADED 
                SQL = SQL + " AND ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'"; 

                SQL = SQL + "         GROUP BY  ";
                SQL = SQL + "         ACCOUNT_CODE   , ";
                SQL = SQL + "        ACCOUNT_NAME   , ";
                SQL = SQL + "        STATION_CODE, ";
                SQL = SQL + "        STATION_NAME,  ";
                SQL = SQL + "        RMCODE   , ";
                SQL = SQL + "        RMDESC   , ";
                SQL = SQL + "        UOM_CODE    , ";
                SQL = SQL + "        UOM_DESC   , TO_BASIC_UOM     ,    TO_UOM_DESC    ";



                dtDnData = clsSQLHelper.GetDataTableByCommand(SQL);

                //LoadDefaultDatasets();
                //prDefineDataTable();

                // lppoping for dattable s 


                //double dweight_for_batch = 0;
                //double dweight_for_conumption = 0;

                //DataRow drRowMaster;
                //SortedList sConsQtyReturn = new SortedList();

                //for (int i = 0; i <= dtDnData.Rows.Count - 1; i++)
                //{
                //    dweight_for_batch = 0;
                //    dweight_for_conumption = 0;

                //    dweight_for_batch = double.Parse(dtDnData.Rows[i]["WEIGHT_FOR_BATCH"].ToString());
                //    dweight_for_conumption = double.Parse(dtDnData.Rows[i]["WEIGHT_FOR_CONUMPTION"].ToString());

                //    sConsQtyReturn = prGetConsQty(dtDnData.Rows[i]["RM_CODE"].ToString(), dtDnData.Rows[i]["FROM_UOM_CODE"].ToString(), dtDnData.Rows[i]["TO_BASIC_UOM"].ToString(), dweight_for_batch, dweight_for_conumption); //'Passing ByRef Variable

                //    drRowMaster = dtConsumption.NewRow();
                //    drRowMaster["SALES_PM_PROJECT_CODE"] = dtDnData.Rows[i]["PROJCODE"].ToString();
                //    drRowMaster["SALES_PM_PROJECT_NAME"] = dtDnData.Rows[i]["PROJNAME"].ToString();

                //    drRowMaster["RM_RMM_RM_CODE"] = dtDnData.Rows[i]["RM_CODE"].ToString();
                //    drRowMaster["RM_RMM_RM_DESCRIPTION"] = dtDnData.Rows[i]["RM_NAME"].ToString();

                //    drRowMaster["SALES_STN_STATION_CODE"] = dtDnData.Rows[i]["STN_CODE"].ToString();
                //    drRowMaster["SALES_STN_STATION_NAME"] = dtDnData.Rows[i]["STN_NAME"].ToString();

                //    drRowMaster["PR_DLYN_THEOR_CONS_QTY"] = dweight_for_batch; // before doing the conversion 
                //    drRowMaster["PR_DLYN_ACTUAL_CONS_QTY"] = dweight_for_conumption;  // befor ding the conversion 

                //    drRowMaster["CONVERSION_FACTOR"] = double.Parse(sConsQtyReturn["CONVERSION_FACTOR"].ToString());

                //    drRowMaster["TECT_CONTMP_THR_CON_QTY"] = double.Parse(sConsQtyReturn["WEIGHT_FOR_BATCH"].ToString());
                //    drRowMaster["TECT_CONTMP_ACT_CON_QTY"] = double.Parse(sConsQtyReturn["WEIGHT_FOR_CONSUMPTION"].ToString());

                //    drRowMaster["TECH_PCD_UOM_CODE"] = dtDnData.Rows[i]["FROM_UOM_CODE"].ToString();  // From UoM code 
                //    drRowMaster["MIX_UM_UOM_DESC"] = dtDnData.Rows[i]["FROM_UOM_NAME"].ToString();    //  From UOM Name 

                //    drRowMaster["RM_UM_UOM_CODE"] = dtDnData.Rows[i]["TO_BASIC_UOM"].ToString();   // To Uom Code 
                //    drRowMaster["RM_UM_UOM_DESC"] = dtDnData.Rows[i]["TO_UOM_DESC"].ToString();     // To UOM Name 


                //    dtConsumption.Rows.Add(drRowMaster);
                //}
                //#endregion
                //=========================================BBS DETAILS =====================================



                dtDnData.TableName = "CONSUMPTION";
                dsReturn.Tables.Add(dtDnData);

            }
            catch (Exception ex)
            {

                return null;
            }

            return dsReturn;
        }

        private SortedList prGetConsQty(string sRmCode, string sFromUOM, string sToUOM, double dWeightForBatch, double dweight_for_conumption)
        {
            SortedList sConsQty = new SortedList();
            try
            {
                // Dim dConvQty, dConvQty_Therotical As Double
                DataRow[] drConvArray = null;
                DataRow drConvRow = default(DataRow);

                double dConversionFactor = 0;
                double dConvQty_WeightForBatch = 0;
                double dConvQty_weight_for_conumption = 0;

                if (sFromUOM.ToString().Trim() == sToUOM.ToString().Trim())
                {
                    dConversionFactor = 1; // bcz this case both  From uom and To UOM is same // jomy p chacko 30 jan 2013
                    dConvQty_WeightForBatch = dWeightForBatch;

                    dConvQty_weight_for_conumption = dweight_for_conumption;
                }
                else
                {
                    if (dsUOMConv.Tables[0].Rows.Count > 0)
                    {
                        drConvArray = dsUOMConv.Tables[0].Select("rm_um_uom_code_from='" + sFromUOM + "' AND rm_um_uom_code_to='" + sToUOM + "' AND rm_rmm_rm_code='" + sRmCode + "'");

                        if (drConvArray.Length > 0)
                        {
                            drConvRow = drConvArray[0];
                            dConvQty_WeightForBatch = dWeightForBatch * double.Parse(drConvRow["rm_ucm_factor"].ToString());

                            dConvQty_weight_for_conumption = dweight_for_conumption * double.Parse(drConvRow["rm_ucm_factor"].ToString());

                            dConversionFactor = double.Parse(drConvRow["rm_ucm_factor"].ToString());
                        }
                        else
                        {
                            drConvArray = dsUOMConv.Tables[0].Select("rm_um_uom_code_from='" + sToUOM + "' AND rm_um_uom_code_to='" + sFromUOM + "' AND rm_rmm_rm_code='" + sRmCode + "'");
                            if (drConvArray.Length > 0)
                            {
                                drConvRow = drConvArray[0];
                                dConvQty_WeightForBatch = dWeightForBatch / double.Parse(drConvRow["rm_ucm_factor"].ToString());

                                dConvQty_weight_for_conumption = dweight_for_conumption / double.Parse(drConvRow["rm_ucm_factor"].ToString());

                                dConversionFactor = double.Parse(drConvRow["rm_ucm_factor"].ToString());
                            }
                            else
                            {
                                dConversionFactor = 1; // bcz this case both  From uom and To UOM is same or not defined  // jomy p chacko 30 jan 2013
                                dConvQty_WeightForBatch = dWeightForBatch;

                                dConvQty_weight_for_conumption = dweight_for_conumption;
                            }
                        }
                    }
                    else
                    {
                        dConversionFactor = 1; // bcz this case both  From uom and To UOM is same or not defined  // jomy p chacko 30 jan 2013

                        dConvQty_WeightForBatch = dWeightForBatch;

                        dConvQty_weight_for_conumption = dweight_for_conumption;
                    }
                }

                sConsQty.Add("WEIGHT_FOR_BATCH", dConvQty_WeightForBatch);
                sConsQty.Add("WEIGHT_FOR_CONSUMPTION", dConvQty_weight_for_conumption);
                sConsQty.Add("CONVERSION_FACTOR", dConversionFactor);
                // slpoReturn.Add("DNLPO", sDnLpo)


            }
            catch (Exception ex)
            {

                // Return 0
            }

            return sConsQty;

        }
        private void LoadDefaultDatasets()
        {
            SqlHelper clsSQLHelper = new SqlHelper();

            SQL = " SELECT rm_um_uom_code_from, rm_um_uom_code_to, rm_ucm_factor, rm_rmm_rm_code";
            SQL = SQL + "   FROM rm_uom_conversion";

            dsUOMConv = null;

            dsUOMConv = clsSQLHelper.GetDataset(SQL);
        }

        //private void prDefineDataTable()
        //{

        //    //DataTable fdtInvMaster = new DataTable("PROD_TBL_INVOICE_MASTER");

        //    dtConsumption.Columns.Add("SALES_PM_PROJECT_CODE", Type.GetType("System.String"));
        //    dtConsumption.Columns.Add("SALES_PM_PROJECT_NAME", Type.GetType("System.String"));


        //    dtConsumption.Columns.Add("rm_rmm_rm_code", Type.GetType("System.String"));
        //    dtConsumption.Columns.Add("rm_rmm_rm_description", Type.GetType("System.String"));



        //    dtConsumption.Columns.Add("sales_stn_station_code", Type.GetType("System.String"));
        //    dtConsumption.Columns.Add("sales_stn_station_name", Type.GetType("System.String"));


        //    dtConsumption.Columns.Add("pr_dlyn_theor_cons_qty", Type.GetType("System.Double"));
        //    dtConsumption.Columns.Add("pr_dlyn_actual_cons_qty", Type.GetType("System.Double"));
        //    dtConsumption.Columns.Add("CONVERSION_FACTOR", Type.GetType("System.Double"));


        //    dtConsumption.Columns.Add("tect_contmp_thr_con_qty", Type.GetType("System.Double"));
        //    dtConsumption.Columns.Add("tect_contmp_act_con_qty", Type.GetType("System.Double"));


        //    dtConsumption.Columns.Add("tech_pcd_uom_code", Type.GetType("System.String")); // From UoM code 
        //    dtConsumption.Columns.Add("Mix_um_uom_desc", Type.GetType("System.String"));   //  From UOM Name 

        //    dtConsumption.Columns.Add("rm_um_uom_code", Type.GetType("System.String")); // To Uom Code 

        //    dtConsumption.Columns.Add("rm_um_uom_desc", Type.GetType("System.String"));   // To UOM Name 


        //    // dtConsumption.Columns.Add("pr_dlyn_dn_qty", Type.GetType("System.Double"));


        //    //  dtConsumption.Columns.Add("PR_DLYN_DN_NO", Type.GetType("System.String")); // To Uom Code 

        //    //  dtConsumption.Columns.Add("PR_DLYN_DN_DATE", Type.GetType("System.DateTime"));   // To UOM Name 


        //}

       #endregion

        #region " otherfuncion "

        //public DataSet FetchStation(string stcode)
        //{
        //    DataSet sReturn = new DataSet();

        //    try
        //    {
        //        SQL = string.Empty;
        //        SqlHelper clsSQLHelper = new SqlHelper();


        //        SQL = " SELECT SALES_STN_STATION_CODE CODE,SALES_STN_STATION_NAME NAME ,GL_COSM_ACCOUNT_CODE COSMACCODE";
        //        SQL = SQL + " FROM SL_STATION_MASTER WHERE SALES_STN_DELETESTATUS=0 ";
        //        SQL = SQL + " AND SALES_STN_STATION_STATUS='A' ";
        //        SQL = SQL + " AND SALES_PC_STATION='Y'   ";// Added As per the discussion with Mr.Jomy and Sreeraj. Needed Precast only in XIP

        //        if (!string.IsNullOrEmpty(stcode))
        //        {
        //            SQL = SQL + " AND SALES_STN_STATION_CODE='" + stcode + "' ";
        //        }

        //        SQL = SQL + " ORDER BY SALES_STN_STATION_NAME ";

        //        sReturn = clsSQLHelper.GetDataset(SQL);
        //    }
        //    catch (Exception ex)
        //    {
        //        ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
        //        objLogWriter.WriteLog(ex);
        //    }

        //    return sReturn;
        //}

        public string FnValidate(string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_SALES_MASTER WHERE RM_MSM_APPROVED ='N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_MSM_ENTRY_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material sales entry exists !! Unable to to continue..";
                }

                sReturn = null;

                SQL = " SELECT COUNT(*)";
                SQL = SQL + " FROM RM_RECEIPT_APPL_MASTER WHERE RM_MRMA_APPROVED_STATUS ='N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR( RM_MRMA_APPROVALTRANS_DATE,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material approval entry exists !! Unable to to continue..";
                }

                sReturn = null;

                //   SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m, rm_mat_stk_transfer_detl d ";
                //   SQL = SQL + "WHERE m.rm_mstm_transfer_no = d.rm_mstm_transfer_no AND m.ad_fin_finyrid = d.ad_fin_finyrid and ";
                ////   SQL = SQL + "d.RM_MSTD_REC_FLG ='N' ";
                //   SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                SQL = " SELECT count(*) FROM rm_mat_stk_transfer_master m  ";
                SQL = SQL + "WHERE  RM_MSTM_APPRV_STATUS <>'Y' ";

                SQL = SQL + "AND TO_DATE(TO_CHAR( m.RM_MSTM_TRANSFER_DATE ,'DD-MON-YYYY')) BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";


                sReturn = clsSQLHelper.GetDataset(SQL);

                if (Convert.ToInt32(sReturn.Tables[0].Rows[0][0]) > 0)
                {
                    return "Unapproved raw material approval entry exists !! Unable to to continue..";
                }
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return "CONTINUE";
        }

        public DataSet fnConversion()
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                //SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT rm_um_uom_code_from, rm_um_uom_code_to, rm_ucm_factor, rm_rmm_rm_code";
                SQL = SQL + " FROM rm_uom_conversion";
                SQL = SQL + " WHERE rm_ucm_conversion_id = (SELECT MAX (rm_ucm_conversion_id)";
                SQL = SQL + " FROM rm_uom_conversion";
                SQL = SQL + " WHERE rm_ucm_deletestatus = 0)";
                SQL = SQL + " AND rm_ucm_deletestatus = 0";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchEntryDateDetails(string sStationCode, object mngrclassobj)
        {
            DataSet dsReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                // jhere norammlly one one station SALES_STN_STATION_CODE =  '" + sStationCode + "' and  ///jomy 03 Aug 2016
                SQL = "   select RM_PSPR_FROM_DATE,RM_PSPR_TO_DATE, SALES_STN_STATION_CODE from   RM_PHYSICAL_STOCK_PROJ_MASTER    where   AD_FIN_FINYRID=" + mngrclass.FinYearID + "";


                dsReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return dsReturn;
        }

        public string PhysicalstockEntryExists(string EntryNo, Int16 FinID)
        {

            DataTable dtCnt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT count(*) cnt  ";
                SQL = SQL + " FROM RM_PHYSICAL_STOCK_MASTER ";
                SQL = SQL + " where  RM_PSPRM_ENTRY_NO ='" + EntryNo + "'";
                SQL = SQL + " and  RM_PSPRM_AD_FINYRID = " + FinID + "";

                dtCnt = clsSQLHelper.GetDataTableByCommand(SQL);

                if (Convert.ToInt16(dtCnt.Rows[0]["cnt"]) > 0)
                {
                    return "Unable to Un Approve the entry since physcial stock entry exists against the selected Entry ";
                }

            }
            catch (Exception ex)
            {
                return System.Convert.ToString(ex.Message);
            }
            return "CONTINUE";
        }

        #endregion

        #region "DML"

        public string InsertMasterSql(PhysicalStockEntryProjectPRCEntity oEntity, List<PhysicalStockEntryProjectPRCDetails> EntityDetails, bool Autogen, object mngrclassobj, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROJ_MASTER (";
                SQL = SQL + " RM_PSPRM_ENTRY_NO, RM_PSPR_ENTRY_DATE, RM_PSPR_FROM_DATE, ";
                SQL = SQL + " RM_PSPR_TO_DATE, RM_PSPR_INTERVAL_DAYS, RM_PSPR_REMARKS, ";
                SQL = SQL + " RM_PSPR_DOC_STATUS, AD_CM_COMPANY_CODE, ";
                SQL = SQL + " AD_FIN_FINYRID, RM_PSPR_CREATEDBY, RM_PSPR_CREATEDDATE, ";
                SQL = SQL + " RM_PSPR_DELETESTATUS, ";
                SQL = SQL + " SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE, RM_PSPR_APPROVED, ";
                SQL = SQL + " RM_PSPR_APPROVEDBY ,RM_PSPR_VERIFIED, RM_PSPR_VERIFIEDBY,GL_COSM_ACCOUNT_CODE , AD_BR_CODE   ";
                SQL = SQL + ") ";

                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "','" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' ,'" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "' ,";
                SQL = SQL + " '" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , " + Convert.ToInt32(oEntity.txtNoOfDays) + ",'" + oEntity.txtRemarks + "' ,";
                SQL = SQL + " '' , '" + mngrclass.CompanyCode + "' ,";
                SQL = SQL + " " + mngrclass.FinYearID + " ,'" + mngrclass.UserName + "' , TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " 0,'" + oEntity.ddlStation + "', ' ' ,'" + oEntity.approve + "' ,";
                SQL = SQL + "'" + oEntity.txtApprovedBy + "',";
                SQL = SQL + "'" + oEntity.varify + "' ,'" + oEntity.txtVerifiedBy + "','" + oEntity.CostCenter + "','" + oEntity.AD_BR_CODE  + "'";

                SQL = SQL + ")";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.approve == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);
                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEPPRC", oEntity.txtEntryNo, Autogen, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, mngrclass.BranchCode, sAtuoGenBranchDocNumber);





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
              
        private string InsertDetails(PhysicalStockEntryProjectPRCEntity oEntity, List<PhysicalStockEntryProjectPRCDetails> EntityDetails, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                int liSlNo = 0;
                {
                    foreach (var Data in EntityDetails)
                    {
                        liSlNo = liSlNo + 1;
                        lobjFactor = 1;

                        // fsStkAdjDesc = "Against Inventory A/c Of RM :" & CStr(lobjRM_Code) & ""
                        fsInvDesc = "Against Consumption A/c Of RM :" + Convert.ToString(Data.lobjRM_CodeDetails) + "";

                        SQL = "";
                        SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROJ_DETAILS (";
                        SQL = SQL + " RM_PSPRM_ENTRY_NO, RM_PSPD_SL_NO,SALES_PM_PROJECT_CODE, RM_RMM_RM_CODE, ";
                        SQL = SQL + " RM_UM_UOM_CODE_MEASURED, RM_PSPD_FACTOR, RM_UM_UOM_CODE_BASIC, ";
                        SQL = SQL + " AD_CM_COMPANY_CODE, AD_FIN_FINYRID, RM_PSPD_DELETESTATUS, ";
                        SQL = SQL + " RM_PSPD_CONSUM_THEORITICAL, RM_PSPD_CONSUMPTION) ";
                        SQL = SQL + " VALUES ( '" + oEntity.txtEntryNo + "', " + liSlNo + ",'" + Data.lobjProject_CodeDetails + "' ,'" + Data.lobjRM_CodeDetails + "' ,";
                        SQL = SQL + " '" + Data.lobjNewUnitDetails + "' , " + lobjFactor + " ,'" + Data.lobjBasicUomDetails + "',";
                        SQL = SQL + " '" + mngrclass.CompanyCode + "'," + mngrclass.FinYearID + ",0,";
                        SQL = SQL + " " + Data.lobjRMCons_TheoriticalDetails + "," + Data.lobjRMConsumptionDetails + "";
                        SQL = SQL + " ) ";

                        sSQLArray.Add(SQL);
                    }
                }
            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            return "CONTINUE";
        }
        
        public string ModifySql(PhysicalStockEntryProjectPRCEntity oEntity, List<PhysicalStockEntryProjectPRCDetails> EntityDetails, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " update RM_PHYSICAL_STOCK_PROJ_MASTER set ";
                SQL = SQL + " RM_PSPR_ENTRY_DATE ='" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' , RM_PSPR_FROM_DATE ='" + Convert.ToDateTime(oEntity.dtpFromDate).ToString("dd-MMM-yyyy") + "', ";
                SQL = SQL + " RM_PSPR_TO_DATE ='" + Convert.ToDateTime(oEntity.dtpToDate).ToString("dd-MMM-yyyy") + "' , RM_PSPR_INTERVAL_DAYs =" + Convert.ToInt32(oEntity.txtNoOfDays) + ", RM_PSPR_REMARKS ='" + oEntity.txtRemarks + "' , ";
                SQL = SQL + " RM_PSPR_UPDATEDBY = '" + mngrclass.UserName + "' , RM_PSPR_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_PSPR_APPROVED = '" + oEntity.approve + "' , ";
                SQL = SQL + " RM_PSPR_APPROVEDBY ='" + oEntity.txtApprovedBy + "',";
                SQL = SQL + "RM_PSPR_VERIFIED= '" + oEntity.varify + "' ,RM_PSPR_VERIFIEDBY ='" + oEntity.txtVerifiedBy + "',GL_COSM_ACCOUNT_CODE='" + oEntity.CostCenter + "' ";

                SQL = SQL + " where RM_PSPRM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_DETAILS WHERE RM_PSPRM_ENTRY_NO = '" + oEntity.txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);


                sRetun = string.Empty;

                sRetun = InsertDetails(oEntity, EntityDetails, mngrclassobj);

                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }

                sRetun = string.Empty;

                if (oEntity.approve == "Y")
                {
                    sRetun = InsertApprovalQrs(oEntity, mngrclassobj);

                    if (sRetun != "CONTINUE")
                    {
                        return sRetun;
                    }
                }

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEPPRC", oEntity.txtEntryNo, false, Environment.MachineName, "U", sSQLArray);

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

        public string DeleteSql(string txtEntryNo, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();


                SQL = " DELETE FROM RM_PHYSICAL_STOCK_PROJ_MASTER WHERE RM_PSPRM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL); 
           
                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_DETAILS WHERE RM_PSPRM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PHYSICAL_STOCK_PROJ_TRIGGER WHERE RM_PSPRM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + mngrclass.FinYearID + "";

                sSQLArray.Add(SQL);


                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEPPRC", txtEntryNo, false, Environment.MachineName, "D", sSQLArray);
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

        //zero deletion.
        public string DeleteRowSql(string sEntryNo, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();



                sRetun = string.Empty;

                SQL = "Delete From RM_PHYSICAL_STOCK_PROJ_DETAILS WHERE RM_PSPRM_ENTRY_NO = '" + sEntryNo + "' and AD_FIN_FINYRID=" + mngrclass.FinYearID + " ";
                SQL = SQL + "    AND RM_PSPD_CONSUM_THEORITICAL =0     AND RM_PSPD_CONSUMPTION =0  ";


                sSQLArray.Add(SQL);

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEPPRC", sEntryNo, false, Environment.MachineName, "U", sSQLArray);





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


        private string InsertApprovalQrs(PhysicalStockEntryProjectPRCEntity oEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQL = " INSERT INTO RM_PHYSICAL_STOCK_PROJ_TRIGGER (";
                SQL = SQL + " RM_PSPRM_ENTRY_NO, RM_PSPR_ENTRY_DATE, AD_FIN_FINYRID, ";
                SQL = SQL + " RM_PSPR_APPROVEDBY) ";
                SQL = SQL + " VALUES ('" + oEntity.txtEntryNo + "' ,'" + Convert.ToDateTime(oEntity.dtpEntryDate).ToString("dd-MMM-yyyy") + "' ," + mngrclass.FinYearID + " ,";
                SQL = SQL + " '" + oEntity.txtApprovedBy + "' ";
                SQL = SQL + " )";

                sSQLArray.Add(SQL);

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
            return "CONTINUE";
        }
        public string UnApprove(string txtEntryNo, int Finid, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();

                SQL = " update RM_PHYSICAL_STOCK_PROJ_MASTER set ";
                SQL = SQL + " RM_PSPR_UPDATEDBY = '" + mngrclass.UserName + "' , RM_PSPR_UPDATEDDATE = TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),";
                SQL = SQL + " RM_PSPR_APPROVED = 'N' , ";
                SQL = SQL + " RM_PSPR_APPROVEDBY = ''";
                SQL = SQL + " where RM_PSPRM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + Finid + " ";

                sSQLArray.Add(SQL);

                SQL = " DELETE FROM RM_PHYSICAL_STOCK_PROJ_TRIGGER WHERE RM_PSPRM_ENTRY_NO = '" + txtEntryNo + "' and AD_FIN_FINYRID =" + Finid + "";

                sSQLArray.Add(SQL);

                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTPSEPPRC", txtEntryNo, false, Environment.MachineName, "U", sSQLArray);

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

        #region "Fetch Data"
        public DataSet FnFillGrid()
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT ";
                SQL = SQL + " RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RMM_INV_ACC_CODE , RM_RMM_CONS_ACC_CODE ";
                SQL = SQL + " FROM ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,RM_UOM_MASTER";
                SQL = SQL + " WHERE ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "  ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
            
                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }


        public DataSet fnCountStkNo(string dtpToDate, string ddlstation)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select count(*) StkNo from RM_PHYSICAL_STOCK_MASTER where RM_PSEM_ENTRY_DATE > " + "'" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "";
                SQL = SQL + "' and SALES_STN_STATION_CODE ='" + ddlstation + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }



        public DataSet FetchPhysicalStockEntryDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT RM_PSPRM_ENTRY_NO, RM_PSPR_ENTRY_DATE, RM_PSPR_FROM_DATE, RM_PSPR_TO_DATE, ";
                SQL = SQL + "       RM_PSPR_INTERVAL_DAYS, RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE, ";
                SQL = SQL + "       SALES_STN_STATION_NAME, RM_PHYSICAL_STOCK_PROJ_MASTER.AD_BR_CODE,AD_BR_NAME, ";
                SQL = SQL + "       HR_DEPT_DEPT_CODE, RM_PSPR_REMARKS, RM_PSPR_DOC_STATUS, ";
                SQL = SQL + "       RM_PHYSICAL_STOCK_PROJ_MASTER.AD_CM_COMPANY_CODE, ";
                SQL = SQL + "       RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID, RM_PSPR_CREATEDBY, RM_PSPR_CREATEDDATE, ";
                SQL = SQL + "       RM_PSPR_UPDATEDBY, RM_PSPR_UPDATEDDATE, RM_PSPR_DELETESTATUS,  ";
                SQL = SQL + "       RM_PSPR_APPROVED, RM_PSPR_APPROVEDBY, RM_PSPR_VERIFIED, RM_PSPR_VERIFIEDBY,  ";
                SQL = SQL + "       RM_PHYSICAL_STOCK_PROJ_MASTER.GL_COSM_ACCOUNT_CODE ";
                SQL = SQL + "  FROM RM_PHYSICAL_STOCK_PROJ_MASTER,SL_STATION_MASTER,AD_BRANCH_MASTER ";
                SQL = SQL + " WHERE   RM_PHYSICAL_STOCK_PROJ_MASTER.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.AD_BR_CODE=AD_BRANCH_MASTER.AD_BR_CODE  ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_MASTER.RM_PSPRM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_MASTER.AD_FIN_FINYRID=" + FinId + " ";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_SL_NO Sl_No,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE PROJECT_CODE,";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJECT_NAME,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE RM_Code,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED,";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_FACTOR,";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_BASIC,";
                SQL = SQL + " RM_PSPD_CONSUM_THEORITICAL, RM_PSPD_CONSUMPTION ";
                SQL = SQL + " from ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER,PC_ENQUIRY_MASTER";
                SQL = SQL + " where ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE = PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_SL_NO";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchProjectDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  select  ";
                SQL = SQL + " distinct(RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE) PROJECT_CODE, ";
                SQL = SQL + " PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJECT_NAME ";
                SQL = SQL + " from  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS,PC_ENQUIRY_MASTER ";
                SQL = SQL + " where  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE = PC_ENQUIRY_MASTER.SALES_PM_PROJECT_CODE ";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO='" + EntryNo + "'";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROJ_DETAILS.SALES_PM_PROJECT_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        public DataSet FetchRmDetails(string EntryNo, string FinId)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                //SQL = " select ";
                //SQL = SQL + " distinct(RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE) RM_Code,";
                //SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description,";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED,";
                //SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC,";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_FACTOR,";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_BASIC";
                //SQL = SQL + " from ";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER,TECH_PLANT_MASTER";
                //SQL = SQL + " where ";
                //SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                //SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE";
                //SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO='" + EntryNo + "'";
                //SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                //SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_SL_NO";

                SQL = "  select  ";
                SQL = SQL + " distinct(RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE) RM_Code, ";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_Description, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED, ";
                SQL = SQL + " RM_UOM_MASTER.RM_UM_UOM_DESC, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPD_FACTOR, ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_BASIC ";
                SQL = SQL + " from  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS, RM_UOM_MASTER, RM_RAWMATERIAL_MASTER ";
                SQL = SQL + " where  ";
                SQL = SQL + " RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_UM_UOM_CODE_MEASURED = RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + " AND RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_PSPRM_ENTRY_NO='" + EntryNo + "' ";
                SQL = SQL + " And RM_PHYSICAL_STOCK_PROJ_DETAILS.AD_FIN_FINYRID=" + FinId + " ";
                SQL = SQL + " ORDER BY RM_PHYSICAL_STOCK_PROJ_DETAILS.RM_RMM_RM_CODE ";


                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }

            return sReturn;
        }

        #endregion

        #region "print"

        public DataSet FetchPrintData(string txtEntryNo, string iDocFinYear)
        {
            DataSet dsData = new DataSet("RMPHYSTOCKPRINT");

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "  ";

                SQL = " SELECT  ";
                SQL = SQL + " RM_PSPRM_ENTRY_NO, AD_FIN_FINYRID, RM_PSPR_ENTRY_DATE, RM_PSPR_FROM_DATE, RM_PSPR_TO_DATE,  ";
                SQL = SQL + " RM_PSPR_INTERVAL_DAYS, RM_PSPR_CREATEDBY, RM_PSPR_VERIFIEDBY, RM_PSPR_APPROVED,  ";
                SQL = SQL + " RM_PSPR_APPROVEDBY, RM_PSPR_REMARKS, SALES_STN_STATION_CODE, SALES_STN_STATION_NAME, ";
                SQL = SQL + " RM_PSPD_SL_NO, SALES_PM_PROJECT_CODE, SALES_PM_PROJECT_NAME, RM_RMM_RM_CODE, RM_RMM_RM_DESCRIPTION, ";
                SQL = SQL + " RM_UM_UOM_CODE_MEASURED, MEASUREDUOMDESC, RM_UM_UOM_CODE_BASIC, BASICUOMDESC,  ";
                SQL = SQL + " RM_PSPD_CONSUM_THEORITICAL, RM_PSPD_CONSUMPTION ,MASTER_BRANCH_CODE ";

                SQL = SQL + "  FROM  ";
                SQL = SQL + "     RM_ACTUAL_CONS_PRJPRC_PRINT_VW  ";

                SQL = SQL + "  WHERE  ";

                SQL = SQL + "     RM_PSPRM_ENTRY_NO ='" + txtEntryNo + "'AND AD_FIN_FINYRID=" + iDocFinYear + "";

                dsData = clsSQLHelper.GetDataset(SQL);
                dsData.Tables[0].TableName = "RM_ACTUAL_CONS_PROJ_PRINT_VW";

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

        #endregion



        public DataSet GetProject( string dDnFromDate, string dToDate )
        {

            DataSet sReturn = new DataSet();
            DataTable dtProject = new DataTable();
            DataTable dtSource = new DataTable();
                   CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
 
                SQL = " SELECT PC_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO PROJ_CODE, PC_ENQUIRY_MASTER.SALES_PM_PROJECT_NAME PROJ_NAME ";
                SQL = SQL + " FROM PC_ENQUIRY_MASTER  ";
                SQL = SQL + " WHERE     PC_ENQUIRY_MASTER.SALES_INQM_ENQUIRY_NO  in ";
                SQL = SQL + " ( ";
                SQL = SQL + " SELECT DISTINCT SALES_PM_PROJECT_CODE   FROM  PC_PRODUCTION_MASTER WHERE  ";
                  SQL = SQL + "    ( PC_PROD_DOC_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'  AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "       or  PC_PROD_COMPLETED_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'  AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + "    ) ";
                SQL = SQL + "   union all  ";
                SQL = SQL + "  select  DISTINCT  ACCOUNT_CODE     from   RM_STORE_ISSUE_VOUCHER_PRINT   ";
                SQL = SQL + "        RM_STORE_ISSUE_VOUCHER_PRINT  WHERE APPROVED_YN ='Y' ";
                SQL = SQL + " AND ENTRY_DATE BETWEEN '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                SQL = SQL + " ) ";
 
                SQL = SQL + " ORDER BY SALES_INQM_ENQUIRY_NO ASC";

                dtProject = clsSQLHelper.GetDataTableByCommand(SQL);
                dtProject.TableName = "Project";
                sReturn.Tables.Add(dtProject);

                SQL = " SELECT  RM_SM_SOURCE_CODE SOURCE_CODE,";
                SQL = SQL + " RM_SM_SOURCE_DESC SOURCE_NAME ";
                SQL = SQL + " FROM RM_SOURCE_MASTER ";
                SQL = SQL + " order By  RM_SM_SOURCE_CODE ";

                dtSource = clsSQLHelper.GetDataTableByCommand(SQL);
                dtSource.TableName = "Source";
                sReturn.Tables.Add(dtSource);



            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);

            }

            return sReturn;
        }

    }



    ////=====================================ENITTY ================================================= DO NOT WRITE TEH CODE BELOW STATEMENT ============================ JOMY 

    #region "enity"

    public class PhysicalStockEntryProjectPRCEntity
    {
        public string txtEntryNo
        {
            get;
            set;
        }

        public string txtNoOfDays
        {
            get;
            set;
        }

        public string txtRemarks
        {
            get;
            set;
        }

        public string ddlStation
        {
            get;
            set;
        }
        public string AD_BR_CODE
        {
            get;
            set;
        }

        
        public string CostCenter
        {
            get;
            set;
        }

        public string txtApprovedBy
        {
            get;
            set;
        }

        public string txtVerifiedBy
        {
            get;
            set;
        }

        public string approve
        {
            get;
            set;
        }

        public string varify
        {
            get;
            set;
        }

        public DateTime dtpEntryDate
        {
            get;
            set;
        }

        public DateTime dtpFromDate
        {
            get;
            set;
        }

        public DateTime dtpToDate
        {
            get;
            set;
        }
    }

    public class PhysicalStockEntryProjectPRCDetails
    {
        public object lobjRM_CodeDetails { get; set; }
        public object lobjProject_CodeDetails { get; set; }
        public object lobjNewUnitDetails { get; set; }
        public object lobjRMInvAccountDetails { get; set; }
        public object lobjRMConsAccountDetails { get; set; }
        public object lobjBasicUomDetails { get; set; }
        public double lobjRMOpeningDetails { get; set; }
        public double lobjRMOpenRateDetails { get; set; }
        public double dOpenAmountDetails { get; set; }
        public double dRVDQtyDetails { get; set; }
        public double dRVDRateDetails { get; set; }
        public double dRVDamountDetails { get; set; }
        public double dStkINQtyDetails { get; set; }
        public double dStkINRateDetails { get; set; }
        public double dStkINamountDetails { get; set; }
        public double dStkOUTQtyDetails { get; set; }
        public double dStkOUTRateDetails { get; set; }
        public double dStkOUTamountDetails { get; set; }
        public double dSalesQtyDetails { get; set; }
        public double dSalesRateDetails { get; set; }
        public double dSalesamountDetails { get; set; }
        public double lobjRMReceivedDetails { get; set; }
        public double lobjRMAVGRateDetails { get; set; }
        public double lobjRMReceived_AMOUNTDetails { get; set; }
        public double lobjRMRate_NewDetails { get; set; }
        public double lobjRMCons_TheoriticalDetails { get; set; }
        public double lobjRMConsumptionDetails { get; set; }
        public double lobjRMConsumption_AmountDetails { get; set; }
        public double lobjActualQtyFrmProjectDetails { get; set; }
        public double lobjRMClosingDetails { get; set; }
        public double lobjRMClosing_AmountDetails { get; set; }
        public double lobjRMVarianceQtyDetails { get; set; }
        public double lobjRMVarianceAmtDetails { get; set; }
    }

    #endregion
    //===========================================END OF ENITY
}
