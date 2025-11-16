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

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class TrailerDriverTrip
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;
        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

        /// TRUCK DRIVER TRIP SUMMARY LOGIC ////////////////////////
        public string DestinationType
        {
            get;
            set;
        }

        public string Branch
        {
            get;
            set;
        }

        #region "Trailer Driver Trip Details And Driver Trip Summary  "


        public DataSet FetchTrailerDriverTripSummary(string sSelectedStation, string Filter, string Type, DateTime dDnFromDate, DateTime dToDate)
        {

            DataSet dt = new DataSet();
            DataSet dtData = new DataSet();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";

                //SQL = SQL + " ENTRY_NO,FIN_YEAR,ENTRY_TYPE,ENTRY_DATE,STATION_CODE,";
                //SQL = SQL + " STATION_NAME,ASSET_CODE,ASSET_NAME,DRIVER_CODE,";
                //SQL = SQL + " DRIVER_NAME,";
                //SQL = SQL + " ITEM_CODE,ITEM_NAME,UOM_CODE,UOM_DESC,AMOUNT,APPROVED_QTY,TRIPCOUNT,FROMSOURCE,DESTINATION,WEIGHBRIDGENO";


                SQL = SQL + " ENTRY_NO, FIN_YEAR, ENTRY_TYPE, ENTRY_DATE, STATION_CODE,  ";
                SQL = SQL + " STATION_NAME, ASSET_CODE, ASSET_NAME, PLATE_NO, DRIVER_CODE,  ";
                SQL = SQL + " DRIVER_NAME, ITEM_CODE, ITEM_NAME, UOM_CODE, UOM_DESC,  ";
                SQL = SQL + " AMOUNT, APPROVED_QTY, APPROVED, TRIPCOUNT, TRIP_RATE,  ";
                SQL = SQL + " TRIP_AMOUNT, RM_SM_SOURCE_CODE, FROMSOURCE, DESTINATION_CODE, DESTINATION,  ";
                SQL = SQL + " WEIGHBRIDGENO, ASSETTYPE, PARTY_NAME, APPROVEDSATAUS ,AD_BR_CODE,MASTER_BRANCH_CODE ";
                SQL = SQL + " FROM RM_TRAILER_DRIVER_TRIP_PRINT";
                SQL = SQL + " WHERE";
                SQL = SQL + " RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_TYPE   in(  '" + sSelectedStation + "')";
                SQL = SQL + " AND  RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_DATE  between  '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND  '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(Branch))
                {

                    SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.AD_BR_CODE   in(  '" + Branch + "')";
                }



                if (Type == "Trailer")
                {
                    if (!string.IsNullOrEmpty(Filter))
                    {

                        SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.ASSET_CODE in('" + Filter + "')";
                    }
                }
                else if (Type == "Driver")
                {
                    if (!string.IsNullOrEmpty(Filter))
                    {
                        SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.DRIVER_CODE in('" + Filter + "')";
                    }
                }



                dt = clsSQLHelper.GetDataset(SQL);


            }

            catch (Exception Exception)
            { }
            return dt;
        }


        public DataSet FetchDestinationWiseTripSummary(string imagepath, string sSelectedStation, string Filter, DateTime dDnFromDate, DateTime dToDate)
        {

            DataSet dt = new DataSet();
            DataSet dtData = new DataSet();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";
                SQL = SQL + "  ASSET_CODE,ASSET_NAME,PLATE_NO,DRIVER_CODE,";
                SQL = SQL + " DRIVER_NAME,AD_BR_CODE,MASTER_BRANCH_CODE, ";
                SQL = SQL + " SUM(AMOUNT) AMOUNT,SUM(APPROVED_QTY) APPROVED_QTY,";
                SQL = SQL + " SUM(TRIPCOUNT) TRIPCOUNT,SUM(TRIP_AMOUNT) TRIP_AMOUNT, FROMSOURCE,DESTINATION, ";
                SQL = SQL + "    '" + imagepath + "'||DRIVER_CODE||'.jpg'  Imagepath  ";
                SQL = SQL + " FROM RM_TRAILER_DRIVER_TRIP_PRINT  ";

                SQL = SQL + " WHERE";

                SQL = SQL + "   RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_TYPE   in(  '" + sSelectedStation + "')";
                SQL = SQL + " AND  RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_DATE  between  '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND  '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(Branch))
                {

                    SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.AD_BR_CODE   in(  '" + Branch + "')";
                }


                if (!string.IsNullOrEmpty(Filter))
                {
                    SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.DRIVER_CODE in('" + Filter + "')";

                }
                SQL = SQL + " group by FROMSOURCE,DESTINATION,";
                SQL = SQL + " ASSET_CODE,ASSET_NAME,DRIVER_CODE, DRIVER_NAME,PLATE_NO, ";
                SQL = SQL + " AD_BR_CODE,MASTER_BRANCH_CODE";
                SQL = SQL + " order by DRIVER_CODE";
                dt = clsSQLHelper.GetDataset(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }


        public DataSet FetchDestinationWiseTripDetails(string sSelectedStation, string Filter, DateTime dDnFromDate, DateTime dToDate)
        {

            DataSet dt = new DataSet();
            DataSet dtData = new DataSet();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";

                //SQL = SQL + " ENTRY_NO,ENTRY_TYPE,ENTRY_DATE,STATION_CODE,";
                //SQL = SQL + " STATION_NAME,ASSET_CODE,ASSET_NAME,DRIVER_CODE,DRIVER_NAME,";
                //SQL = SQL + " AMOUNT, APPROVED_QTY,";
                //SQL = SQL + " TRIPCOUNT, FROMSOURCE,DESTINATION,WEIGHBRIDGENO "; 

                SQL = SQL + " ENTRY_NO, FIN_YEAR, ENTRY_TYPE, ENTRY_DATE, STATION_CODE,  ";
                SQL = SQL + " STATION_NAME, ASSET_CODE, ASSET_NAME, PLATE_NO, DRIVER_CODE,  ";
                SQL = SQL + " DRIVER_NAME, ITEM_CODE, ITEM_NAME, UOM_CODE, UOM_DESC,  ";
                SQL = SQL + " AMOUNT, APPROVED_QTY, APPROVED, TRIPCOUNT, TRIP_RATE,  ";
                SQL = SQL + " TRIP_AMOUNT, RM_SM_SOURCE_CODE, FROMSOURCE, DESTINATION_CODE, DESTINATION,  ";
                SQL = SQL + " WEIGHBRIDGENO, ASSETTYPE, PARTY_NAME, APPROVEDSATAUS,AD_BR_CODE,MASTER_BRANCH_CODE ";

                SQL = SQL + " FROM RM_TRAILER_DRIVER_TRIP_PRINT  ";

                SQL = SQL + " WHERE";

                SQL = SQL + " RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_TYPE   in(  '" + sSelectedStation + "')";
                SQL = SQL + " AND  RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_DATE  between  '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND  '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";

                if (!string.IsNullOrEmpty(Branch))
                {

                    SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.AD_BR_CODE   in(  '" + Branch + "')";
                }


                if (!string.IsNullOrEmpty(DestinationType))
                {

                    if (!string.IsNullOrEmpty(Filter))
                    {
                        if (DestinationType == "Driver")
                        {
                            SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.DRIVER_CODE in('" + Filter + "')";
                        }
                        else if (DestinationType == "Trailer")
                        {
                            SQL = SQL + " AND RM_TRAILER_DRIVER_TRIP_PRINT.ASSET_CODE in('" + Filter + "')";
                        }

                    }
                }

                SQL = SQL + " order by DRIVER_CODE ";

                dt = clsSQLHelper.GetDataset(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }

        public DataSet FetchTransporterTripDetails(DateTime dDnFromDate, DateTime dToDate)
        {

            DataSet dt = new DataSet();
            DataSet dtData = new DataSet();
            try
            {
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT  ";

                //SQL = SQL + " ENTRY_NO,ENTRY_TYPE,ENTRY_DATE,STATION_CODE,";
                //SQL = SQL + " STATION_NAME,ASSET_CODE,ASSET_NAME,DRIVER_CODE,DRIVER_NAME,";
                //SQL = SQL + " AMOUNT, APPROVED_QTY,";
                //SQL = SQL + " TRIPCOUNT, FROMSOURCE,DESTINATION,WEIGHBRIDGENO "; 

                SQL = SQL + " ENTRY_NO, FIN_YEAR, ENTRY_TYPE, ENTRY_DATE, STATION_CODE,  ";
                SQL = SQL + " STATION_NAME, ASSET_CODE, ASSET_NAME, PLATE_NO, DRIVER_CODE,  ";
                SQL = SQL + " DRIVER_NAME, ITEM_CODE, ITEM_NAME, UOM_CODE, UOM_DESC,  ";
                SQL = SQL + " AMOUNT, APPROVED_QTY, APPROVED, TRIPCOUNT, TRIP_RATE,  ";
                SQL = SQL + " TRIP_AMOUNT, RM_SM_SOURCE_CODE, FROMSOURCE, DESTINATION_CODE, DESTINATION,  ";
                SQL = SQL + " WEIGHBRIDGENO, ASSETTYPE, PARTY_NAME, APPROVEDSATAUS,TRANS_CODE,TRANS_NAME ";
                SQL = SQL + " FROM RM_TRAILER_DRIVER_TRIP_PRINT  ";
                SQL = SQL + " WHERE";
                //  SQL = SQL + " RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_TYPE   in(  '" + sSelectedStation + "')";
                SQL = SQL + "   RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_DATE  between  '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " AND  '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and RM_TRAILER_DRIVER_TRIP_PRINT.ENTRY_TYPE = 'OT'";
                SQL = SQL + " AND TRANS_CODE is not null";
                SQL = SQL + " order by ENTRY_NO ";

                dt = clsSQLHelper.GetDataset(SQL);

            }

            catch (Exception Exception)
            { }
            return dt;
        }

        //public DataSet FetchTrailerDriverTripDetils(string sSelectedStation, string Filter, string Type, DateTime dDnFromDate, DateTime dToDate)
        //{

        //    DataSet dt = new DataSet();

        //    try
        //    {

        //        CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
        //        SqlHelper clsSQLHelper = new SqlHelper();


        //        SQL = "   SELECT     pr_delivery_note.pr_dlyn_dn_no,  pr_delivery_note.ad_fin_finyrid,  pr_delivery_note.pr_dlyn_dn_date,  ";
        //        SQL = SQL + "    pr_delivery_note.pr_dlyn_batched_time,   pr_delivery_note.pr_dlyn_ex_plant,   pr_delivery_note.pr_dlyn_onsite_time, ";
        //        SQL = SQL + "    pr_delivery_note.pr_dlyn_start_dischg_time,   pr_delivery_note.pr_dlyn_end_dischg_time,   pr_delivery_note.pr_dlyn_offsite_time,   ";
        //        SQL = SQL + "    pr_delivery_note.sales_stn_station_code,   sl_station_master.sales_stn_station_name,   pr_delivery_note.sales_cus_customer_code,  ";
        //        SQL = SQL + "    sl_customer_master.sales_cus_customer_name,   pr_delivery_note.sales_pm_project_code,   sl_project_master.SALES_PM_PROJECT_NAME, ";
        //        SQL = SQL + "    pr_delivery_note.tech_plm_plant_code,   pr_delivery_note.pr_dlyn_trips_sofar,   pr_delivery_note.pr_dlyn_dn_qty,  ";
        //        SQL = SQL + "    pr_delivery_note.pr_dlyn_signed_qty,   pr_delivery_note.pr_dlyn_approved_qty,   pr_delivery_note.pr_dlyn_phy_dnno, ";
        //        SQL = SQL + "    pr_delivery_note.pr_dlyn_truck_no,   TRUCKNAME.fa_fam_asset_description TRUCKNAME,   pr_delivery_note.pr_dlyn_driver_code,";
        //        SQL = SQL + "    truckdriver.hr_emp_employee_name TRUCKDRIVER, SL_PROJ_TRIP_CATEGORY.SALES_PTM_PROJECT_TRIP_CODE  ,";
        //        SQL = SQL + "    SL_PROJ_TRIP_CATEGORY.SALES_PTM_PROJECT_TRIP_DESC,SL_PROJ_TRIP_CATEGORY.SALES_PTM_TRIP_RATE ";

        //        SQL = SQL + "    FROM pr_delivery_note,   sl_customer_master,   hr_employee_master truckdriver,  ";

        //        SQL = SQL + "    fa_fixed_asset_master TRUCKNAME,  sl_project_master,   sl_station_master ,SL_PROJ_TRIP_CATEGORY  ";
        //        SQL = SQL + "    where pr_delivery_note.pr_dlyn_driver_code=truckdriver.hr_emp_employee_code  ";
        //        SQL = SQL + "    AND pr_delivery_note.pr_dlyn_truck_no =TRUCKNAME.fa_fam_asset_code(+)    ";
        //        SQL = SQL + "    AND pr_delivery_note.sales_pm_project_code = sl_project_master.sales_pm_project_code (+) ";
        //        SQL = SQL + "    AND pr_delivery_note.sales_stn_station_code=sl_station_master.sales_stn_station_code (+)  ";
        //        SQL = SQL + "    AND pr_delivery_note.sales_cus_customer_code=sl_customer_master.sales_cus_customer_code (+) ";
        //        SQL = SQL + "    AND sl_project_master.SALES_PTM_PROJECT_TRIP_CODE=SL_PROJ_TRIP_CATEGORY.SALES_PTM_PROJECT_TRIP_CODE (+) ";

        //        SQL = SQL + "    and pr_delivery_note.sales_stn_station_code   in(  '" + sSelectedStation + "')";

        //        SQL = SQL + "    AND  pr_delivery_note.pr_dlyn_dn_date  between  '" + System.Convert.ToDateTime(dDnFromDate, culture).ToString("dd-MMM-yyyy") + "'";
        //        SQL = SQL + "    and  '" + System.Convert.ToDateTime(dToDate, culture).ToString("dd-MMM-yyyy") + "'";
        //        SQL = SQL + "    and   PR_DELIVERY_NOTE.PR_DLYN_CONC_MIXED = 'Y' AND pr_delivery_note.PR_CONRTN_DNCANCEL  ='N'";


        //        if (Type == "Trailer")
        //        {
        //            if (!string.IsNullOrEmpty(Filter))
        //            {
        //                SQL = SQL + " AND sl_project_master.SALES_PTM_PROJECT_TRIP_CODE in('" + Filter + "')";
        //            }
        //        }
        //        else if (Type == "Driver")
        //        {
        //            if (!string.IsNullOrEmpty(Filter))
        //            {
        //                SQL = SQL + " AND pr_delivery_note.pr_dlyn_driver_code in('" + Filter + "')";
        //            }
        //        }




        //        dt = clsSQLHelper.GetDataset(SQL);

        //    }

        //    catch (Exception Exception)
        //    { }
        //    return dt;
        //}


        #endregion


        //// END REGION  ////////////////////////////////

        #region "Fill View"

        public DataTable FillView(string TypeID, string selectedItemes)
        {
            DataTable dtType = new DataTable();
            try

            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                if (TypeID == "3" || TypeID == "4" || TypeID == "5" || TypeID == "6")
                {
                    SQL = " SELECT   hr_emp_employee_code CODE, hr_emp_employee_name NAME,";
                    SQL = SQL + " ad_cm_company_code , HR_EMP_EMPLOYEE_REF_CODE assetref , HR_EMP_MOBILENO assetid,";
                    SQL = SQL + "   ''  CostCode, ''  refcode, ''  PLATENO";
                    SQL = SQL + "  FROM hr_employee_master";
                    SQL = SQL + " WHERE  HR_EMP_STATUS ='A'";
                    SQL = SQL + " AND   HR_DM_DESIGNATION_CODE ";
                    SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                    SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_DESIG_CODE' ) ";

                    if (!string.IsNullOrEmpty(selectedItemes.Trim()))
                    {

                        SQL = SQL + "    and    hr_emp_employee_code not in('" + selectedItemes.Replace(",", "','") + "')";

                    }
                    SQL = SQL + " ORDER BY hr_emp_employee_code ASC";
                }

                else
                {
                    SQL = " SELECT   fa_fam_asset_code CODE, fa_fam_asset_description ||'-Plate No'|| FA_FAM_PLATENO NAME,";
                    SQL = SQL + " fa_fam_ref_code assetref, FA_FAM_ASSET_ID  assetid,GL_COSM_ACCOUNT_CODE CostCode,FA_FAM_REF_CODE refcode,FA_FAM_PLATENO PLATENO";
                    SQL = SQL + "     FROM fa_fixed_asset_master";
                    SQL = SQL + " where  FA_FAM_VISIBLE_PROD_WS_YN='Y' and  FA_FAM_ASSET_STATUS ='A' AND  FA_FAM_VEHICLE_TYPE";
                    SQL = SQL + " IN  (  SELECT RM_IP_PARAMETER_VALUE FROM   RM_DEFUALTS_DRIVER_TRAILER ";
                    SQL = SQL + " WHERE RM_IP_PARAMETER_DESC ='TRAILER_TYPE_CODE' ) ";
                    if (!string.IsNullOrEmpty(selectedItemes.Trim()))
                    {
                        SQL = SQL + "    and    fa_fam_asset_code not in('" + selectedItemes.Replace(",", "','") + "')";

                    }
                    SQL = SQL + " ORDER BY fa_fam_asset_code";
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

        #region "station "
        public DataSet GetStation()
        {
            DataSet dsData = new DataSet();
            DataTable dtType = new DataTable();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "  SELECT SALES_STN_STATION_CODE CODE,SALES_STN_STATION_NAME NAME  FROM  ";
                SQL = SQL + "  SL_STATION_MASTER WHERE   SALES_STN_PRODUCTION_STATION ='Y'";

                dtType = clsSQLHelper.GetDataTableByCommand(SQL);
                dsData.Tables.Add(dtType);
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

        #region "Get LastDay"
        public DataSet GetLastDay(DateTime dtLastDepDate)
        {
            string strCondition = string.Empty;
            DataSet dt = new DataSet();

            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "Select To_Char(Last_Day('" + Convert.ToDateTime(dtLastDepDate).ToString("dd-MMM-yyyy") + "'),'DD-MON-YYYY') LASTDAY From Dual";

                dt = clsSQLHelper.GetDataset(SQL);
            }

            catch (Exception ex)
            { }
            return dt;
        }
        #endregion
    }
}
