using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client; using Oracle.ManagedDataAccess.Types;
using System.Collections;
using System.Data; using AccosoftUtilities;   using AccosoftLogWriter;  using AccosoftNineteenCDL;   
// jomy p chacko / dated 03 apr 2013
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class ReceiptPriceRevision
    {

        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;

        #region "Fil view

        public DataTable FillView(string TypeID, string SuppCode,string rawCode,string sFromdate, string sTodate)
        {
            DataTable dtType = new DataTable();
            try
            //-- this look i up is using various places os take care // Jomy & jins  11 / jan 23013
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();

               if (TypeID == "SourceData")
                {


                    SQL = " select  ";
                    SQL = SQL + "    DISTINCT  ";
                    SQL = SQL + "    RM_SOURCE_MASTER.RM_SM_SOURCE_CODE Code, RM_SM_SOURCE_DESC Name  ";
                    SQL = SQL + "    FROM RM_SOURCE_MASTER,RM_RECEPIT_DETAILS ";
                    SQL = SQL + "    WHERE   ";
                    //SQL = SQL + "    RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE = RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ";
                    SQL = SQL + "    RM_SOURCE_MASTER.RM_SM_SOURCE_CODE = RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE ";
                    SQL = SQL + "    AND RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";
                    SQL = SQL + "    AND RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                    if (!string.IsNullOrEmpty(SuppCode))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE = '" + SuppCode + "'";

                    }
                    if (!string.IsNullOrEmpty(rawCode))
                    {
                        SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = '" + rawCode + "'";

                    }

                    SQL = SQL + "    ORDER BY RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ASC  "; 

                }

               else if (TypeID == "TRANSPORTER_Source_REVISION")
               {

                   SQL = " select  ";
                   SQL = SQL + "    DISTINCT  ";
                   SQL = SQL + "    RM_SOURCE_MASTER.RM_SM_SOURCE_CODE Code, RM_SM_SOURCE_DESC Name  ";
                   SQL = SQL + "    FROM RM_SOURCE_MASTER, RM_RECEPIT_DETAILS  ";
                   SQL = SQL + "    WHERE   ";
                   SQL = SQL + "    RM_SOURCE_MASTER.RM_SM_SOURCE_CODE =     RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE ";
                   SQL = SQL + "   and RM_RECEPIT_DETAILS.RM_MRD_APPROVED ='N'";
                   SQL = SQL + " AND  RM_MRM_RECEIPT_DATE BETWEEN '" + System.Convert.ToDateTime(sFromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";

                   if (!string.IsNullOrEmpty(SuppCode))
                   {
                       SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = '" + SuppCode + "'";

                   }

                   if (!string.IsNullOrEmpty(rawCode))
                   {
                       SQL = SQL + " AND  RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = '" + rawCode + "'";

                   }

                   SQL = SQL + "    ORDER BY RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ASC  ";

               }

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
        public DataSet FectchReceiptDetails(string sSlectedStation , string txtSupplierCode, string sType ,  string sExFacoryOrOnsite , string sSelectedRM,string sSourceCode, string dtpFromDate, string dtpToDate)
        {
            DataSet sReturn = new DataSet();

            try
            {
                SQL = string.Empty;
                  SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.AD_FIN_FINYRID,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_ORDER_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_FIN_FINYRID, ";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SL_NO,";
                SQL = SQL + " RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE,";
                SQL = SQL + " SL_STATION_MASTER.SALES_STN_STATION_NAME,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_RMM_RM_CODE,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_UM_UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE, RM_SOURCE_MASTER.RM_SM_SOURCE_DESC ,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MTRANSPO_ORDER_NO, RM_MTRANSPO_FIN_FINYRID , RM_MRM_TRNSPORTER_DOC_NO ,";
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
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_TOTAL_AMOUNT_NEW,";
                SQL = SQL + " RM_RECEPIT_DETAILS.RM_MRD_SUPP_PENO, RM_RECEPIT_DETAILS.RM_MRD_TRANS_PENO ,RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_UNIQE_NO ";

                SQL = SQL + " FROM RM_RECEPIT_DETAILS,";
                SQL = SQL + " RM_RAWMATERIAL_MASTER,";
                SQL = SQL + " RM_UOM_MASTER,";
                SQL = SQL + " SL_STATION_MASTER , RM_SOURCE_MASTER";

                SQL = SQL + " WHERE     RM_RECEPIT_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE   ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_UM_UOM_CODE = RM_UOM_MASTER.RM_UM_UOM_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE    ";
                SQL = SQL + " AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE";
                SQL = SQL + " AND RM_MRD_APPROVED = 'N'";
                SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))";
                SQL = SQL + " BETWEEN '" + Convert.ToDateTime(dtpFromDate).ToString("dd-MMM-yyyy") + "' AND '" + Convert.ToDateTime(dtpToDate).ToString("dd-MMM-yyyy") + "'";

                if (sType == "SUPPLIER")
                {

                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_VM_VENDOR_CODE= '" + txtSupplierCode + "'";

                    // IF THE filter type is supplier then should be  considered the price type otherwise the price revision will not work 
                    // the same supplier has EX-FACTORY AND ONSITE .
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MPOM_PRICE_TYPE= '" + sExFacoryOrOnsite + "'";


                }
                else
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE= '" + txtSupplierCode + "'";
        
                }
                if (!string.IsNullOrEmpty(sSlectedStation))
                {
                    SQL = SQL + " AND  RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE in('" + sSlectedStation + "')";
                }

                //if (!string.IsNullOrEmpty(sSelectedTrans))
                //{
                //    SQL = SQL + "   AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE  in( '" + sSelectedTrans + "')";
                //}

                if (!string.IsNullOrEmpty(sSelectedRM))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE ='" + sSelectedRM + "'";
                }

                if (!string.IsNullOrEmpty(sSourceCode))
                {
                    SQL = SQL + " AND RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE ='" + sSourceCode + "'";
                }

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


        #region " sql insrt " 
        public string InsertSQL(double dAmount, string sType,  List<ReceiptPriceRevisionDet> EntityReceipt, object mngrclassobj)
        {
            //int fiApprNo=0;

            string sRetun = string.Empty;

            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SQLArray.Clear();

                foreach(var Data in EntityReceipt)
                {
                    SQL = "  ";
                    SQL =      "INSERT INTO  RM_RECEPIT_PRICE_REVISION ( ";
                    SQL = SQL  +  "   RM_MRM_RECEIPT_REVISION_TYPE, RM_MRM_RECEIPT_UNIQE_NO, RM_MRM_RECEIPT_NO,  ";
                    SQL = SQL  +  "   AD_FIN_FINYRID, RM_RMM_RM_CODE, RM_MRM_RECEIPT_REVISION_AMOUNT,  ";
                    SQL = SQL  +  "   RM_MRM_UPDATEDBY)  ";
                    SQL = SQL + " values (   ";
                    SQL = SQL + "'"+ sType +"', " + Data.UniqueNoReceipt + " ,  '" + Data.ReceiptNoReceipt + "',";
                    SQL = SQL + "" + Data.RectiptFinyrIdReceipt + " ,  '" + Data.RMCodeReceipt + "',";
                    SQL = SQL + "" + dAmount + " ,  '" + mngrclass.UserName + "'"; 
                    SQL = SQL + "   )  ";  

                    SQLArray.Add(SQL); 
                } 

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RTMPRICERVSN", "RMPR", false, Environment.MachineName, "I", SQLArray);
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

    public class ReceiptPriceRevisionDet
    {
        public object UniqueNoReceipt
        {
            get;
            set;
        }

        public object ReceiptNoReceipt
        {
            get;
            set;
        }

        public object RectiptFinyrIdReceipt
        {
            get;
            set;
        }

        public object RMCodeReceipt
        {
            get;
            set;
        }

    }
}
