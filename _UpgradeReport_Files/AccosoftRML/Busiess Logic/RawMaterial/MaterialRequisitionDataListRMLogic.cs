using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;
using System.Collections;
//using FarPoint.Web.Spread;
using AccosoftUtilities;
using AccosoftLogWriter;
using AccosoftNineteenCDL;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialRequisitionDataListRMLogic
    {
        static string sConString = Utilities.cnnstr;
        //   string scon = sConString;

        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        //  OracleConnection ocConn = new OracleConnection(System.Configuration.ConfigurationManager.ConnectionStrings["accoSoftConnection"].ToString());
        ArrayList sSQLArray = new ArrayList();
        String SQL = string.Empty;

        public MaterialRequisitionDataListRMLogic()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        #region API
        public string FillDataListDetails(MRRFQPendingEntryWSGridEntity odata, string UserName, int FinYr)
        {
            DataTable dt = new DataTable();
            string JSONString = null;
            try
            {
                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();
                SQL = "SELECT ROWNUM AS SlNO, RM_PR_MASTER.RM_PR_PRNO PRNO, ";
                SQL = SQL + "       RM_PR_MASTER.AD_FIN_FINYRID, TO_CHAR (RM_PR_MASTER.RM_PR_PR_DATE, 'DD-MON-YYYY') PR_DATE, ";
                SQL = SQL + "       RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE ACCOUNT_CODE,GL_COSTING_MASTER_VW.NAME COSTING_NAME,  ";
                SQL = SQL + "       RM_PR_DETAILS.SALES_STN_STATION_CODE STATION_CODE, SL_STATION_MASTER.SALES_STN_STATION_NAME STATION_NAME, ";
                SQL = SQL + "       RM_PR_DETAILS.HR_DEPT_DEPT_CODE, HR_DEPT_MASTER.HR_DEPT_DEPT_DESC, ";
                SQL = SQL + "       RM_PR_DETAILS.RM_PRD_SL_NO, ";
                SQL = SQL + "       RM_PR_DETAILS.RM_SM_SOURCE_CODE SOURCE_CODE,RM_SOURCE_MASTER.RM_SM_SOURCE_DESC SOURCE_NAME, ";
                SQL = SQL + "       RM_PR_DETAILS.RM_RMM_RM_CODE RM_CODE, RM_RAWMATERIAL_MASTER.RM_RMM_RM_DESCRIPTION RM_NAME, ";
                SQL = SQL + "       RM_PR_DETAILS.RM_IM_ITEM_DTL_DESCRIPTION ITEM_DESC, ";
                SQL = SQL + "       RM_PR_DETAILS.PC_BUD_BUDGET_ITEM_CODE, ";
                SQL = SQL + "       RM_PR_DETAILS.RM_UOM_UOM_CODE UOM_CODE, RM_UOM_MASTER.RM_UM_UOM_DESC UOM_NAME, ";
                SQL = SQL + "       RM_PRD_QTY , ";
                SQL = SQL + "       RM_PR_MASTER.AD_BR_CODE, AD_BRANCH_MASTER.AD_BR_NAME, ";
                SQL = SQL + "       RM_PR_MASTER.RM_PR_STOCK_TYPE_CODE,RM_PO_STOCK_TYPE_NAME STOCK_TYPE_NAME,      ";
                SQL = SQL + "       RM_PR_CREATEDBY, ";
                SQL = SQL + "       RM_PR_VERIFIED VERIFIED, RM_PR_VERIFIEDBY VERIFIED_BY, ";
                SQL = SQL + "       RM_PR_APPROVED APPROVED, RM_PR_APPROVEDBY APPROVEDBY  ,nvl(PRICE,0) PRICE,PO_DATE,SUP_NAME ";
                SQL = SQL + "  FROM RM_PR_MASTER, RM_PR_DETAILS, GL_COSTING_MASTER_VW, ";
                SQL = SQL + "       RM_STOCK_TYPE_VW, SL_STATION_MASTER, RM_SOURCE_MASTER,RM_RAWMATERIAL_MASTER,RM_UOM_MASTER, ";
                SQL = SQL + "       AD_BRANCH_MASTER,HR_DEPT_MASTER ,";

                SQL = SQL + "      (SELECT ITEM_CODE, ";
                SQL = SQL + "                 sup_code, ";
                SQL = SQL + "                 sup_name, ";
                SQL = SQL + "                 TO_CHAR (po_date, 'DD-MON-YYYY')     AS po_date, ";
                SQL = SQL + "                 price, ";
                SQL = SQL + "                 po_no ";
                SQL = SQL + "            FROM (SELECT RM_PO_DETAILS.RM_RMM_RM_CODE                          AS ITEM_CODE, ";
                SQL = SQL + "                         rm_vendor_master.rm_vm_vendor_code                     AS sup_code, ";
                SQL = SQL + "                         rm_vendor_master.rm_vm_vendor_name                     AS sup_name, ";
                SQL = SQL + "                         RM_PO_MASTER.RM_po_pono                                AS po_no, ";
                SQL = SQL + "                         TO_DATE (RM_PO_MASTER.RM_po_po_date)                   AS po_date, ";
                SQL = SQL + "                         RM_PO_DETAILS.RM_POD_NEWPRICE                          AS price, ";
                SQL = SQL + "                         ROW_NUMBER () ";
                SQL = SQL + "                             OVER (PARTITION BY RM_PO_DETAILS.RM_RMM_RM_CODE ";
                SQL = SQL + "                                   ORDER BY RM_PO_MASTER.RM_po_po_date DESC)    AS row_num ";
                SQL = SQL + "                    FROM RM_PO_MASTER ";
                SQL = SQL + "                         INNER JOIN RM_PO_DETAILS ";
                SQL = SQL + "                             ON     RM_PO_MASTER.RM_po_pono = ";
                SQL = SQL + "                                    RM_PO_DETAILS.RM_po_pono ";
                SQL = SQL + "                                AND RM_PO_MASTER.ad_fin_finyrid = ";
                SQL = SQL + "                                    RM_PO_DETAILS.ad_fin_finyrid ";
                SQL = SQL + "                         INNER JOIN rm_vendor_master ";
                SQL = SQL + "                             ON     RM_PO_MASTER.rm_vm_vendor_code = ";
                SQL = SQL + "                                    rm_vendor_master.rm_vm_vendor_code ";
                SQL = SQL + "                                AND RM_PO_MASTER.RM_po_deletestatus = ";
                SQL = SQL + "                                    rm_vendor_master.rm_vm_deletestatus ";
                SQL = SQL + "                   WHERE     RM_PO_MASTER.RM_po_deletestatus = 0 ";
                SQL = SQL + "                         AND RM_PO_MASTER.RM_po_approved = 'Y' ";
                SQL = SQL + "                         AND RM_PO_DETAILS.RM_RMM_RM_CODE IN ";
                SQL = SQL + "                                 (SELECT DISTINCT RM_RMM_RM_CODE ";
                SQL = SQL + "                                    FROM RM_PR_DETAILS, RM_PR_MASTER ";
                SQL = SQL + "                                   WHERE     RM_PR_MASTER.RM_PR_PRNO = ";
                SQL = SQL + "                                             RM_PR_DETAILS.RM_PR_PRNO ";
                SQL = SQL + "                                         AND RM_PR_MASTER.AD_FIN_FINYRID = ";
                SQL = SQL + "                                             RM_PR_DETAILS.AD_FIN_FINYRID ";
                SQL = SQL + "                                         AND RM_PR_MASTER.RM_PR_APPROVED = 'Y' ";
                SQL = SQL + "                                         AND RM_PR_MASTER.RM_PR_CANCEL <> 'Y' ";
                SQL = SQL + "                                         AND RM_PR_MASTER.RM_PR_PR_STATUS = 'O')) ";
                SQL = SQL + "           WHERE row_num = 1 ";
                SQL = SQL + "    ORDER BY po_date DESC, po_no  ) LAST_PO_DETAILS ";

                SQL = SQL + " WHERE     RM_PR_MASTER.RM_PR_PRNO = RM_PR_DETAILS.RM_PR_PRNO ";
                SQL = SQL + "       AND RM_PR_DETAILS.GL_COSM_ACCOUNT_CODE=GL_COSTING_MASTER_VW.CODE ";
                SQL = SQL + "       AND RM_PR_DETAILS.RM_RMM_RM_CODE = RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE(+) ";
                SQL = SQL + "       AND RM_PR_DETAILS.SALES_STN_STATION_CODE = SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                SQL = SQL + "       AND RM_PR_DETAILS.HR_DEPT_DEPT_CODE = HR_DEPT_MASTER.HR_DEPT_DEPT_CODE(+) ";
                SQL = SQL + "       AND RM_PR_DETAILS.RM_SM_SOURCE_CODE =RM_SOURCE_MASTER.RM_SM_SOURCE_CODE ";
                SQL = SQL + "       AND RM_PR_MASTER.RM_PR_STOCK_TYPE_CODE = RM_STOCK_TYPE_VW.RM_PO_STOCK_TYPE_CODE ";
                SQL = SQL + "       AND RM_PR_DETAILS.RM_UOM_UOM_CODE =RM_UOM_MASTER.RM_UM_UOM_CODE ";
                SQL = SQL + "       AND RM_PR_MASTER.AD_BR_CODE = AD_BRANCH_MASTER.AD_BR_CODE  AND   RM_PR_MASTER.RM_PR_PR_STATUS ='O' ";
                // SQL = SQL + "   AND RM_PR_MASTER.AD_FIN_FINYRID=" + FinYr + " ";
                SQL = SQL + "   AND RM_PR_MASTER.AD_BR_CODE='" + odata.Branch + "' ";

                if (UserName != "ADMIN")
                {
                    SQL = SQL + "   AND RM_PR_MASTER.AD_BR_CODE  in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }
                SQL = SQL + "   AND RM_PR_MASTER.RM_PR_APPROVED ='Y'  and  RM_PR_DETAILS.RM_PUR_RFQ_DONE = 'N' ";
                SQL = SQL + "        AND RM_PR_DETAILS.RM_RMM_RM_CODE=LAST_PO_DETAILS.ITEM_CODE(+) ";
                dt = clsSQLHelper.GetDataTableByCommand(SQL);
                dt.TableName = "RM_PR_MASTER";
                JSONString = JsonConvert.SerializeObject(dt, Formatting.None);

            }
            catch (Exception ex)
            {
                ExceptionLogWriter objLogWriter = new ExceptionLogWriter();
                objLogWriter.WriteLog(ex);
            }
            return JSONString;

        }

        #endregion

        #region "FillView"

        public DataTable FillViewDivision(string username)
        {
            DataTable dtReturn = new DataTable();
            try
            {

                SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "SELECT AD_BR_CODE CODE , AD_BR_NAME  NAME FROM AD_BRANCH_MASTER ";
                if (username != "ADMIN")
                {
                    SQL = SQL + " where AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + username + "') ";


                }
                SQL = SQL + "   order BY AD_BR_SORT_ID asc  ";

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


        public DataTable FillRFQview(string fromdate, string Todate, string Branch, string UserName)
        {
            DataTable dtGrid = new DataTable();
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "select RM_PRFQ_DOC_NO CODE, TO_CHAR (RM_PRFQ_DOC_DATE, 'DD-MON-YYYY') RM_PRFQ_DOC_DATE ";
                SQL = SQL + "from  RM_PUR_RFQ_MASTER ";
                SQL = SQL + "  where  RM_PRFQ_DOC_DATE  BETWEEN '" + System.Convert.ToDateTime(fromdate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(Todate).ToString("dd-MMM-yyyy") + "'";
                SQL = SQL + " and AD_BR_CODE in ('" + Branch + "') ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + " and AD_BR_CODE in (SELECT AD_BR_CODE FROM AD_USER_GRANTED_BRANCH WHERE  AD_UM_USERID = '" + UserName + "') ";
                }

                SQL = SQL + "ORDER BY RM_PRFQ_DOC_DATE DESC ";

                dtGrid = clsSQLHelper.GetDataTableByCommand(SQL);

            }
            catch (Exception ex)
            {
            }
            return dtGrid;
        }

        #endregion

        #region "DML"

        public string InsertSQL(string RFQNo, string Branch, List<MRRFQPendingEntryRMGridEntity> objRMGridEntity,
            object mngrclassobj, bool bDocAutogenerated, bool bDocAutoGeneratedBrachWise, string sAtuoGenBranchDocNumber, string dtpRFQDate)
        {
            string sRetun = string.Empty;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;

                sSQLArray.Clear();
                if (bDocAutogenerated == true)
                {
                    SQL = " INSERT INTO RM_PUR_RFQ_MASTER ( ";
                    SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID,RM_PRFQ_DOC_DATE,AD_CM_COMPANY_CODE,RM_PRFQ_REMARKS,  ";
                    SQL = SQL + "   RM_PRFQ_CREATEDBY, RM_PRFQ_CREATEDDATE_TIME, AD_BR_CODE)  ";
                    SQL = SQL + " VALUES ('" + RFQNo + "', " + mngrclass.FinYearID + ",TO_DATE('" + Convert.ToDateTime(dtpRFQDate).ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM') ,'" + mngrclass.CompanyCode + "', '',";
                    SQL = SQL + " '" + mngrclass.UserName + "',TO_DATE('" + DateTime.Now.ToString("dd-MMM-yyyy") + "','DD-MM-YYYY HH:MI:SS AM'),  ";
                    SQL = SQL + "  '" + Branch + "' ) ";

                    sSQLArray.Add(SQL);
                }

                sRetun = string.Empty;
                sRetun = InsertDetailsSQL(RFQNo, objRMGridEntity, mngrclassobj);
                if (sRetun != "CONTINUE")
                {
                    return sRetun;
                }
                sRetun = string.Empty;

                sRetun = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "RMPMRQRFQ", RFQNo, bDocAutogenerated, Environment.MachineName, "I", sSQLArray, bDocAutoGeneratedBrachWise, Branch, sAtuoGenBranchDocNumber);

            }
            catch (Exception ex)
            {
                sRetun = System.Convert.ToString(ex.Message);
                return sRetun;
            }
            return sRetun;
        }

        private string InsertDetailsSQL(string RFQNo, List<MRRFQPendingEntryRMGridEntity> objRMGridEntity, object mngrclassobj)
        {
            string sRetun = string.Empty;
            DataTable dtData = new DataTable();
            DataSet sReturn = new DataSet();
            Int32 iRow = 0;

            try
            {
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = "SELECT MAX(RM_PRFQD_SLNO) RM_PRFQD_SLNO FROM RM_PUR_RFQ_DETAILS ";
                SQL = SQL + "WHERE RM_PRFQ_DOC_NO = '" + RFQNo + "'";

                sReturn = clsSQLHelper.GetDataset(SQL);


                if (sReturn.Tables[0].Rows[0]["RM_PRFQD_SLNO"].ToString() == null || sReturn.Tables[0].Rows[0]["RM_PRFQD_SLNO"].ToString() == string.Empty)
                {
                    iRow = 0;
                }
                else
                {
                    iRow = Convert.ToInt32(sReturn.Tables[0].Rows[0]["RM_PRFQD_SLNO"].ToString());
                }

                sReturn = null;
                SQL = string.Empty;


                foreach (var Data in objRMGridEntity)
                {
                    ++iRow;

                    SQL = " INSERT INTO RM_PUR_RFQ_DETAILS ( ";
                    SQL = SQL + "   RM_PRFQ_DOC_NO, AD_FIN_FINYRID, RM_PR_PRNO,AD_PR_FIN_FINYRID,   ";
                    SQL = SQL + "   AD_CM_COMPANY_CODE, AD_BR_CODE,  ";
                    SQL = SQL + "   SALES_STN_STATION_CODE, HR_DEPT_DEPT_CODE, GL_COSM_ACCOUNT_CODE, PC_BUD_BUDGET_ITEM_CODE, ";
                    SQL = SQL + "    RM_PRD_SLNO, RM_RMM_RM_CODE,  RM_PRFQD_SLNO,RM_PRFQ_QTY, RM_PR_STOCK_TYPE_CODE, RM_UM_UOM_CODE,RM_SM_SOURCE_CODE  , ";
                    SQL = SQL + "   RM_POD_UNIT_PRICE_LAST_PO , RM_POD_PO_DATE_LAST_PO, RM_POD_VENDOR_NAME_LAST_PO     )  ";

                    SQL = SQL + " VALUES ('" + RFQNo + "', " + mngrclass.FinYearID + ", '" + Data.PRNO + "', " + Data.PRFinYr + ",  ";
                    SQL = SQL + " '" + mngrclass.CompanyCode + "', '" + Data.DivCode + "', ";
                    SQL = SQL + " '" + Data.StnCode + "', '" + Data.DeptCode + "', '" + Data.AccCode + "', '" + Data.BudgItemCode + "', ";
                    SQL = SQL + " '" + Data.PRSLNo + "', '" + Data.RMCode + "',  " + iRow + ", " + Data.Qty + " , '" + Data.StockItem + "',";
                    SQL = SQL + " '" + Data.UOMCode + "', '" + Data.SourceCode + "' , ";
                    SQL = SQL + " " + Data.Last_PO_UnitPrice + ", '" + Data.Last_PO_Date + "', '" + Data.Last_PO_VendorName + "') ";

                    sSQLArray.Add(SQL);

                    SQL = " UPDATE RM_PR_DETAILS SET RM_PUR_RFQ_NO = '" + RFQNo + "',  ";
                    SQL = SQL + "   RM_PUR_RFQ_DONE = 'Y' ";
                    SQL = SQL + " WHERE RM_PR_PRNO = '" + Data.PRNO + "' ";
                    SQL = SQL + " AND AD_FIN_FINYRID =  " + Data.PRFinYr + " ";
                    SQL = SQL + " AND RM_PRD_SL_NO = '" + Data.PRSLNo + "' ";


                    sSQLArray.Add(SQL);
                }
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

        #endregion
    }


    #region"Entity"

    public class MRRFQPendingEntryRMGridEntity
    {
        public MRRFQPendingEntryRMGridEntity()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        public string PRNO
        {
            get;
            set;
        }

        public int PRFinYr
        {
            get;
            set;
        }

        public string PRSLNo
        {
            get;
            set;
        }

        public string RMCode
        {
            get;
            set;
        }

        public string ItemDescription
        {
            get;
            set;
        }

        public double Qty
        {
            get;
            set;
        }

        public string SourceCode
        {
            get;
            set;
        }

        public string TypeCode
        {
            get;
            set;
        }

        public string CostCentreCode
        {
            get;
            set;
        }

        public string DivCode
        {
            get;
            set;
        }

        public string AccCode
        {
            get;
            set;
        }

        public string StnCode
        {
            get;
            set;
        }

        public string DeptCode
        {
            get;
            set;
        }
        public string UOMCode
        {
            get;
            set;
        }

        public string StockItem
        {
            get;
            set;
        }

        public string BudgItemCode
        {
            get;
            set;
        }
        public double Last_PO_UnitPrice
        {
            get;
            set;
        }

        public string Last_PO_Date
        {
            get;
            set;
        }

        public string Last_PO_VendorName
        {
            get;
            set;
        }
    }

    #endregion

    public class MRRFQPendingEntryWSGridEntity
    {
        public string Branch { get; set; }
    }


}
