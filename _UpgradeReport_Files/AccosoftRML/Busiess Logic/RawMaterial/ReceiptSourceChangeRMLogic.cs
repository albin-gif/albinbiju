using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Data;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
// jomy p chacko / dated 03 apr 2013
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ReceiptSourceChangeRMLogic
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;

        #region "Fil view

        public DataTable FillRM(string PONo)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                //SQL = " select  ";
                //SQL = SQL + "    DISTINCT  ";
                //SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  ";
                //SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_PO_DETAILS  ";
                //SQL = SQL + "    WHERE   ";
                //SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_PO_DETAILS.RM_RMM_RM_CODE ";
                //SQL = SQL + "   and RM_PO_DETAILS.RM_PO_PONO='" + PONo + "'";

                //SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";

                SQL = "SELECT     RM_PO_PROJECT_DETAILS.RM_PO_PONO, RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE     Code, ";
                SQL = SQL + "                  RM_RMM_RM_DESCRIPTION                    Name, ";
                SQL = SQL + "                  RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE, ";
                SQL = SQL + "                  GL_COSTING_MASTER_VW.Name                COSTNAME, ";
                SQL = SQL + "                  RM_PO_COST_CENTER, ";
                SQL = SQL + "                  RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE, ";
                SQL = SQL + "                  RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, ";
                SQL = SQL + "                  RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE, ";
                SQL = SQL + "                  SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "                  RM_PO_PROJECT_DETAILS.RM_UOM_UOM_CODE, ";
                SQL = SQL + "                  RM_UOM_MASTER.RM_UM_UOM_DESC ";
                SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER,  ";
                SQL = SQL + "         RM_PO_MASTER, ";
                SQL = SQL + "         RM_PO_PROJECT_DETAILS, ";
                SQL = SQL + "         GL_COSTING_MASTER_VW, ";
                SQL = SQL + "         RM_SOURCE_MASTER, ";
                SQL = SQL + "         SL_STATION_MASTER, ";
                SQL = SQL + "         RM_UOM_MASTER ";
                SQL = SQL + "   WHERE     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = ";
                SQL = SQL + "             RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE  ";
                SQL = SQL + "         AND RM_PO_MASTER.RM_PO_PONO = RM_PO_PROJECT_DETAILS.RM_PO_PONO ";
                SQL = SQL + "         AND RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE =   RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "         AND RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "         AND RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE = ";
                SQL = SQL + "             GL_COSTING_MASTER_VW.CODE(+) ";
                SQL = SQL + "         AND RM_PO_PROJECT_DETAILS.RM_PO_PONO='" + PONo + "'";
                SQL = SQL + "         AND RM_PO_PROJECT_DETAILS.RM_UOM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC ";


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

        #region " Fetch Data" 
        public DataSet FectchReceiptDetails(string OrderNumber, string sSlectedStation, string SupplierCode, string sSelectedRM, string Source, string UOM, string Project, string StartDate, string EndDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_ORDER_QTY, RM_MRD_VENDOR_DOC_NO ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPPLY_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_WEIGH_QTY,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_APPROVE_QTY, RM_RECEPIT_DETAILS.RM_MRD_REJECTED_QTY  ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_UNIT_PRICE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_RATE_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TRANS_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER";

                SQL = SQL + " WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_MRD_APPROVED = 'N' AND RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO IS NULL ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO='" + OrderNumber + "'";
                SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))>= " +
                    "'" + System.Convert.ToDateTime(StartDate).ToString("dd-MMM-yyyy") + "'";
                if (!string.IsNullOrEmpty(EndDate))
                {
                    SQL = SQL + "  AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))<= " +
                        "'" + System.Convert.ToDateTime(EndDate).ToString("dd-MMM-yyyy") + "'";
                }

                if (!string.IsNullOrEmpty(SupplierCode))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + SupplierCode + "'";
                }

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ='" + sSelectedRM + "'";
                }

                if (!string.IsNullOrEmpty(Source))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE ='" + Source + "'";
                }

                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + "  AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = '" + sSlectedStation + "'";
                }


                if (!string.IsNullOrEmpty(UOM))
                {
                    SQL = SQL + "  AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = '" + UOM + "'";
                }



                //if (!string.IsNullOrEmpty(Project))
                //    SQL = SQL + "  AND RM_PO_COST_CENTER = '" + Project + "'";



                SQL = SQL + " order by RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE asc";

                sReturn = clsSQLHelper.GetDataset(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return sReturn;
        }
        public DataTable FillViewPurchaseOrder(string sFilterType, string StartDate, string SuppCode, string Source, string RmCode, string Project, string stnCode, string UomCode, string Branch, string sFromdate, string sTodate, object mngrclassobj)
        {
            DataTable dt = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                if (sFilterType == "FPO")
                {
                    SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PCode,RM_PO_MASTER.RM_VM_VENDOR_CODE SCode,  ";
                    SQL = SQL + "RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName,AD_FIN_FINYRID Id,RM_PO_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME,";
                    SQL = SQL + " to_char(RM_PO_MASTER.RM_PO_PO_DATE,'DD-MON-YYYY') PDate,RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS,";
                    SQL = SQL + " RM_PO_MASTER. RM_PO_VERIFIED VERIFIED, RM_PO_MASTER.RM_PO_APPROVED RM_PO_APPROVED, ";
                    SQL = SQL + " to_char(RM_PO_AGG_START_DATE,'dd/MM/YYYY') START_DATE ,to_char(RM_PO_AGG_END_DATE,'dd/MM/YYYY') END_DATE , 0 PRICE,'' EndDateYN ,";
                    SQL = SQL + " '' RMCODE,'' RMNAME,'' SOCode,'' SOName ";

                    SQL = SQL + " FROM RM_VENDOR_MASTER,RM_PO_MASTER,AD_BRANCH_MASTER ";

                    SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE";
                    SQL = SQL + " and RM_PO_MASTER.AD_BR_CODE =AD_BRANCH_MASTER.AD_BR_CODE";
                 //   SQL = SQL + "  AND RM_PO_MASTER.ad_fin_finyrid = " + mngrclass.FinYearID + "   ";
                    SQL = SQL + "  AND RM_PO_MASTER.RM_PO_ENTRY_TYPE ='PURCHASE' ";
                    SQL = SQL + "  AND RM_PO_MASTER.RM_PO_APPROVED='Y' ";
                    SQL = SQL + "  AND RM_PO_MASTER.RM_PO_PO_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "' ";


                }
                else
                {
                    SQL = " SELECT RM_PO_MASTER.RM_PO_PONO PCode,RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE RMCODE ,RM_RMM_RM_DESCRIPTION RMNAME,";
                    SQL = SQL + " RM_PO_MASTER.RM_VM_VENDOR_CODE SCode,RM_VENDOR_MASTER.RM_VM_VENDOR_NAME SName ,RM_PO_MASTER.AD_BR_CODE,AD_BRANCH_MASTER.AD_BR_NAME,";
                    SQL = SQL + " RM_PO_MASTER.AD_FIN_FINYRID Id,RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE SOCode,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOName,";
                    SQL = SQL + " to_char(RM_PO_MASTER.RM_PO_PO_DATE,'DD-MON-YYYY') PDate,RM_PO_MASTER.RM_MPOM_PAYMENT_TERMS,";
                    SQL = SQL + " RM_PO_MASTER. RM_PO_VERIFIED VERIFIED, RM_PO_MASTER.RM_PO_APPROVED RM_PO_APPROVED, ";
                    SQL = SQL + " to_char(RM_PO_AGG_START_DATE,'dd/MM/YYYY') START_DATE,to_char(RM_PO_AGG_END_DATE,'dd/MM/YYYY') END_DATE , RM_PO_PROJECT_DETAILS.RM_POD_UNIT_PRICE  PRICE , RM_PO_MASTER.RM_PO_AGG_END_DATE_REQUIRED_YN EndDateYN ";

                    SQL = SQL + " FROM RM_PO_MASTER,RM_SOURCE_MASTER, ";
                    SQL = SQL + " AD_BRANCH_MASTER, RM_RAWMATERIAL_MASTER ,RM_VENDOR_MASTER,RM_PO_PROJECT_DETAILS ";

                    SQL = SQL + " WHERE RM_PO_MASTER.RM_VM_VENDOR_CODE =RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                    SQL = SQL + " AND  RM_PO_MASTER.AD_BR_CODE =AD_BRANCH_MASTER.AD_BR_CODE ";
                    SQL = SQL + " AND RM_PO_MASTER.RM_PO_PONO = RM_PO_PROJECT_DETAILS.RM_PO_PONO ";
                    SQL = SQL + " and RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE";
                    SQL = SQL + " and RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE =RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                   // SQL = SQL + "  AND RM_PO_MASTER.ad_fin_finyrid = " + mngrclass.FinYearID + "  ";
                    SQL = SQL + "  AND RM_PO_MASTER.RM_PO_ENTRY_TYPE ='PURCHASE' ";
                    SQL = SQL + "  AND RM_PO_AGG_START_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "' ";
                    SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.RM_SM_SOURCE_CODE <> '" + Source + "' ";

                    if (!string.IsNullOrEmpty(SuppCode))
                        SQL = SQL + " AND RM_PO_MASTER.RM_VM_VENDOR_CODE= '" + SuppCode + "'";

                    if (!string.IsNullOrEmpty(RmCode))
                        SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.RM_RMM_RM_CODE = '" + RmCode + "'";

                    if (!string.IsNullOrEmpty(stnCode))
                        SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.SALES_STN_STATION_CODE = '" + stnCode + "'";

                    if (!string.IsNullOrEmpty(UomCode))
                        SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.RM_UOM_UOM_CODE = '" + UomCode + "'";

                    if (!string.IsNullOrEmpty(Branch))
                        SQL = SQL + " AND RM_PO_MASTER.AD_BR_CODE = '" + Branch + "'";

                    if (!string.IsNullOrEmpty(Project))
                        SQL = SQL + "  AND RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE = '" + Project + "'";


                }
                if (mngrclass.UserName != "ADMIN")
                { // Abin ADDED FOR EB FOR CONTROLLING CATEGOERY WISE // 10/jul/2021
                    SQL = SQL + "  and     RM_PO_MASTER.AD_BR_CODE IN  ";
                    SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                }

                SQL = SQL + " order by RM_PO_MASTER.RM_PO_PONO desc";


                dt = clsSQLHelper.GetDataTableByCommand(SQL);
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return dt;

        }

        public DataTable FetchRMDetails(string RMCode, string PONo)
        {
            DataTable dtType = new DataTable();
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = " select   ";
                // SQL = SQL + "    DISTINCT   ";
                SQL = SQL + "     RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE Code, RM_RMM_RM_DESCRIPTION Name  , ";
                SQL = SQL + "     RM_PO_COST_CENTER,RM_PO_DETAILS.RM_SM_SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC, ";
                SQL = SQL + "     RM_PO_DETAILS.SALES_STN_STATION_CODE,SL_STATION_MASTER.SALES_STN_STATION_NAME, ";
                SQL = SQL + "    RM_PO_DETAILS.RM_UOM_UOM_CODE,RM_UOM_MASTER.RM_UM_UOM_DESC,RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE,GL_COSTING_MASTER_VW.Name COSTNAME ";

                SQL = SQL + "    FROM RM_RAWMATERIAL_MASTER, RM_PO_MASTER,RM_PO_DETAILS ,RM_SOURCE_MASTER, ";
                SQL = SQL + "    SL_STATION_MASTER ,RM_UOM_MASTER,RM_PO_PROJECT_DETAILS,GL_COSTING_MASTER_VW ";
                SQL = SQL + "    WHERE    ";
                SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE =     RM_PO_DETAILS.RM_RMM_RM_CODE  ";
                SQL = SQL + "    and RM_PO_MASTER.RM_PO_PONO= RM_PO_DETAILS.RM_PO_PONO ";
                SQL = SQL + "    and RM_PO_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "    and RM_PO_DETAILS.SALES_STN_STATION_CODE =SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                SQL = SQL + "    and RM_PO_DETAILS.RM_UOM_UOM_CODE=RM_UOM_MASTER.RM_UM_UOM_CODE  ";
                SQL = SQL + "    AND RM_PO_MASTER.RM_PO_PONO=RM_PO_PROJECT_DETAILS.RM_PO_PONO ";
                SQL = SQL + "    AND RM_PO_PROJECT_DETAILS.GL_COSM_ACCOUNT_CODE = GL_COSTING_MASTER_VW.CODE(+)  ";

                SQL = SQL + "    and RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE='" + RMCode + "'  ";
                SQL = SQL + "    and RM_PO_MASTER.RM_PO_PONO='" + PONo + "'  ";

                SQL = SQL + "    ORDER BY RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ASC  ";



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


        #region " sql insrt " 
        public string UpdateSQL(string FirstPONO,string NewPo, string NewPOStartDate, List<ReceiptSourceChangeRMDet> EntityReceipt, object mngrclassobj)
        {
            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQLArray.Clear();

                
                foreach (var Data in EntityReceipt)
                {
                    SQL = "update RM_RECEIPT_MASTER set RM_MPOM_ORDER_NO='" + NewPo + "' where RM_MPOM_ORDER_NO='" + FirstPONO + "' and  ";
                    SQL = SQL + " RM_MRM_RECEIPT_DATE>='" + System.Convert.ToDateTime(NewPOStartDate).ToString("dd-MMM-yyyy") + "'";
                    SQL = SQL + " and  RM_MRM_RECEIPT_NO = '" + Data.ReceiptNo + "' ";


                    SQLArray.Add(SQL);

                    SQL = "update  RM_RECEPIT_DETAILS set RM_MPOM_ORDER_NO='" + NewPo + "',RM_SM_SOURCE_CODE='" + Data.source + "', ";
                    SQL = SQL + "RM_MRD_SUPP_UNIT_PRICE=" + Data.Price + ",RM_MRD_SUPP_AMOUNT=RM_MRD_APPROVE_QTY*" + Data.Price + ", ";
                    SQL = SQL + "RM_MRD_TOTAL_AMOUNT=RM_MRD_APPROVE_QTY*" + Data.Price + ",RM_MRD_SUPP_UNIT_PRICE_NEW=" + Data.Price + ", ";
                    SQL = SQL + "RM_MRD_SUPP_AMOUNT_NEW=RM_MRD_APPROVE_QTY*" + Data.Price + ",RM_MRD_TOTAL_AMOUNT_NEW=RM_MRD_APPROVE_QTY*" + Data.Price + " ";
                    SQL = SQL + " where RM_MPOM_ORDER_NO='" + FirstPONO + "' and  RM_MRM_RECEIPT_NO = '"+Data.ReceiptNo+"' and ";
                    SQL = SQL + " RM_MRM_RECEIPT_DATE>='" + System.Convert.ToDateTime(NewPOStartDate).ToString("dd-MMM-yyyy") + "' ";
                    SQL = SQL + " and RM_RMM_RM_CODE ='" + Data.Item + "' ";

                    SQLArray.Add(SQL);
                }

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMRCSC", "RTMRCSC", false, Environment.MachineName, "U", SQLArray);
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

        #endregion 
    }

    public class ReceiptSourceChangeRMDet
    {
        public string ReceiptNo
        {
            get;
            set;
        }
        public string ReceiptDate
        {
            get;
            set;
        }

        public string Item
        {
            get;
            set;
        }

        public string source
        {
            get;
            set;
        }

        public double Price
        {
            get;
            set;
        }


    }
}
