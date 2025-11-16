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
using System.Collections.Specialized;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using AccosoftRML.Busiess_Logic.RawMaterial;

//------------------------------------------------------------

    // CREATED BY   : NISHA ALOSIOUS
    // CREATED DATE : 25-11-2015
    // DETAILS      : MATERIAL ORDER APPROVAL/POSTING - LOGIC  

//------------------------------------------------------------

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialOrderPostingLogic
    {
        static string sConString = Utilities.cnnstr;
        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;
            
        DataSet dsMaterialOrder = new DataSet();       

        public DataTable dtMain = new DataTable("MAIN");

        public MaterialOrderPostingLogic()
	    {
		    //
		    // TODO: Add constructor logic here
		    //
	    }

        #region " TO LOAD VOUCHER DETAILS "

        public DataTable LoadVoucherRights(object mngrclassobj)
        {
            DataTable dtLoadVoucherRights = new DataTable();
            try
            {
                SQL = string.Empty;

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " SELECT   AD_AF_ID";
                SQL = SQL + "  FROM ad_mail_approval_users";
                SQL = SQL + "  WHERE AD_AF_ID IN ('RMOTOA'";              
                SQL = SQL + " )";
                SQL = SQL + "  AND UPPER (ad_um_userid) = '" + mngrclass.UserName  + "'";
                SQL = SQL + " ORDER BY AD_AF_ID";
                
                dtLoadVoucherRights = clsSQLHelper.GetDataTableByCommand(SQL);
            }

            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {

                //cCollection = null;
                //objBase = null;
            }
            return dtLoadVoucherRights;
        }

        #endregion

        #region " GET VOUCHER DETAILS "

        public DataSet GetMaterialOrderVouchers(object mngrclassobj, DateTime fromdate, DateTime todate)
        {
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT   ";
                SQL = SQL + "   RM_PO_PONO PO_NO ,RM_PO_MASTER.AD_FIN_FINYRID PO_FINYR , RM_PO_PO_DATE  PO_DATE , ";

                SQL = SQL + "   RM_PO_MASTER.RM_VM_VENDOR_CODE SUPPLIER_CODE ,RM_VM_VENDOR_NAME SUPPLIER_NAME,'Material Order Approval' PO_TYPE, ";

                SQL = SQL + "   RM_PO_NET_AMOUNT NET_AMOUNT ,RM_PO_REMARKS  REMARKS, ";

                SQL = SQL + "   RM_PO_CREATEDBY CREATEDBY ,RM_PO_VERIFIEDBY  VERIFIEDBY  ";
                                
                SQL = SQL + "FROM  ";                
                SQL = SQL + "   RM_PO_MASTER,RM_VENDOR_MASTER  ";

                SQL = SQL + "WHERE  ";               
                SQL = SQL + "   RM_PO_MASTER.RM_VM_VENDOR_CODE = RM_VENDOR_MASTER.RM_VM_VENDOR_CODE ";
                SQL = SQL + "   AND RM_PO_VERIFIED='Y' ";
                SQL = SQL + "   AND RM_PO_APPROVED='N' ";
                SQL = SQL + "   AND RM_PO_PO_DATE BETWEEN  '" + fromdate.ToString("dd-MMM-yyyy") + "' AND '" + todate.ToString("dd-MMM-yyyy") + "'  ";

                SQL = SQL + "ORDER BY  ";
                SQL = SQL + "   RM_PO_PO_DATE ASC";

                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);

                dsMaterialOrder.Tables.Add(dtMain);
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

            return dsMaterialOrder;
        }
       
        #endregion

        #region " POSTING VOUCHERS "

        public string PostVoucherDetails(object mngrclassobj, string vouchertype, List<MaterialOrderPostingEntity> objPosintDetailsEntity)
        {
            string sReturn = string.Empty;
         
            try
            {
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SQLArray.Clear();
               
                foreach (var Data in objPosintDetailsEntity)
                {
                   
                    SQL = " UPDATE RM_PO_MASTER SET "; 
                    SQL = SQL + " RM_PO_APPROVED ='Y',"; 
                    SQL = SQL + " RM_PO_APPROVEDBY= '" + mngrclass.UserName + "' "; 
                    SQL = SQL + " WHERE RM_PO_PONO = '" + Data.oPONo + "' AND AD_FIN_FINYRID = " + Data.oPOFinYr + ""; 
                    SQLArray.Add(SQL);

                    SQL = " INSERT INTO RM_PO_TRIGGER (";
                    SQL = SQL + " RM_PO_PONO, AD_FIN_FINYRID, AD_UM_USERID, ";
                    SQL = SQL + " RM_PO_PO_DATE, RM_PO_POST_DATE) "; 
                    SQL = SQL + " VALUES ('" + Data.oPONo + "', '" + Data.oPOFinYr + "', '" + mngrclass.UserName + "', ";
                    SQL = SQL + "'" + Data.oPODate.ToString("dd-MMM-yyyy") + "',  TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM') )";

                    SQLArray.Add(SQL);

                }                                                        
                
                string sRetun = string.Empty;

                sReturn = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "APPROVAL", "0", false, Environment.MachineName, "I", SQLArray);         
            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            finally
            {

                //cCollection = null;
                //objBase = null;
            }

            return sReturn;
        }
        
        #endregion
    }

    #region " GRID ENTITY "

        public class MaterialOrderPostingEntity
        {
            public string oPONo { get; set; }

            public string oPOFinYr { get; set; }

            public DateTime oPODate { get; set; }     
        }

    #endregion
}
