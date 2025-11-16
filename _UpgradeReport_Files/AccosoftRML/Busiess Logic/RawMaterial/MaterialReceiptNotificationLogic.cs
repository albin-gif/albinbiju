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
using System.Collections.Specialized;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Diagnostics.Eventing.Reader;
using Newtonsoft.Json;
using System.IO;
namespace AccosoftRML.Busiess_Logic.RawMaterial
{
    public class MaterialReceiptNotificationLogic
    {
        static string sConString = Utilities.cnnstr;
        OracleConnection ocConn = new OracleConnection(sConString.ToString());

        ArrayList SQLArray = new ArrayList();
        String SQL = string.Empty;
        DataSet dsVouchers = new DataSet();
        public DataTable dtMain = new DataTable("MAIN");

        public MaterialReceiptNotificationLogic ( )
        {
            //
            // TODO: Add constructor logic here
            //
        }


        public DateTime oFromDateTime
        {
            get;
            set;
        }
        public DateTime oToDateTime
        {
            get;
            set;
        }
        public string oDateTimeFilterType
        {
            get;
            set;
        }


        #region " TO LOAD VOUCHER DETAILS "

        public DataTable LoadVoucherRights ( string UserName )
        {
            DataTable dtLoadVoucherRights = new DataTable();
            try
            {
                SQL = string.Empty;

                ///   SessionManager mngrclass = (SessionManager)mngrclassobj;

                SqlHelper clsSQLHelper = new SqlHelper();

                ////SQL = " SELECT DISTINCT  AD_MI_ITEMID";
                ////SQL = SQL + "  FROM ad_mail_approval_users";
                ////SQL = SQL + " WHERE AD_AF_ID IN ('GLCPVA',";
                ////SQL = SQL + " 'GLCRVA','GLBPVA','GLBRVA',";
                ////SQL = SQL + " 'GLTCNA', 'GLTDNA', 'GLTJVA',";
                ////SQL = SQL + " 'GLMISCPURA', 'GLMISCINVA',";
                ////SQL = SQL + " 'WTROVERI','WTRODESP','WTROAP',";
                ////SQL = SQL + " 'WTPOVER','WTPOAP','WTCUSTRNTORDRVER','WTCUSTRNTORDRAP',";
                ////SQL = SQL + " 'RMOTOA','RMOTOVER',";
                ////SQL = SQL + " 'GPOVER','GPOAPPROV','GPOREJ','RMOTOREJ','WTPOREJ','WTROREJ',";
                ////SQL = SQL + "'GLCOMMONPRAPP','GLCOMMONVER',";

                ////SQL = SQL + " 'WTPRPOST','WTPRAP','WTPEAPP',";
                ////SQL = SQL + " 'MRAPPVD','MRTOA',";
                ////SQL = SQL + " 'GLPRPOST','GLPRAPP','WTPEAPP','RTMPEAPP' "; 


                ////SQL = SQL + " )";
                ////if (mngrclass.UserName != "ADMIN")
                ////{
                ////    SQL = SQL + "  AND  ad_um_userid  = '" + mngrclass.UserName + "'";
                ////}
                ////SQL = SQL + " ORDER BY AD_MI_ITEMID";



                SQL = " SELECT   ";
                SQL = SQL + "        DISTINCT AD_MODULEITEM.AD_MI_ITEMID , AD_MODULEITEM.AD_MI_ITEMNAME   , ";
                SQL = SQL + "        AD_MODULEITEM.AD_MD_MODULEID , AD_MODULEITEM.AD_MI_SL_NO ";
                SQL = SQL + " FROM  ";
                SQL = SQL + "        AD_MAIL_APPROVAL_FORMS ,AD_MODULEITEM  ";
                SQL = SQL + " WHERE   AD_MODULEITEM .AD_MI_ITEMID  = AD_MAIL_APPROVAL_FORMS.AD_MI_ITEMID  ";
                SQL = SQL + " AND AD_MAIL_APPROVAL_FORMS.AD_MI_ITEMID  IN ";
                SQL = SQL + "  (  ";
                SQL = SQL + "      SELECT DISTINCT  AD_MI_ITEMID ";
                SQL = SQL + "      FROM AD_MAIL_APPROVAL_USERS ";
                SQL = SQL + "     WHERE AD_AF_ID IN ";
                SQL = SQL + "      ( ";
                SQL = SQL + "            'GLCPVA', ";
                SQL = SQL + "            'GLCRVA','GLBPVA','GLBRVA', ";
                SQL = SQL + "            'GLTCNA', 'GLTDNA', 'GLTJVA', ";
                SQL = SQL + "            'GLMISCPURA', 'GLMISCINVA', ";
                SQL = SQL + "            'WTROVERI','WTRODESP','WTROAP', ";
                SQL = SQL + "            'WTPOVER','WTPOAP','WTCUSTRNTORDRVER','WTCUSTRNTORDRAP', ";
                SQL = SQL + "            'RMOTOA','RMOTOVER', ";
                SQL = SQL + "            'GPOVER','GPOAPPROV','GPOREJ','RMOTOREJ','WTPOREJ','WTROREJ', ";
                SQL = SQL + "            'GLCOMMONPRAPP','GLCOMMONVER', ";
                SQL = SQL + "            'WTPRPOST','WTPRAP','WTPEAPP', ";
                SQL = SQL + "            'MRAPPVD','MRTOA', ";
                SQL = SQL + "            'GLPRPOST','GLPRAPP','WTPEAPP','RTMPEAPP' ";
                SQL = SQL + "       ) ";
                if (UserName != "ADMIN")
                {
                    SQL = SQL + "  AND  ad_um_userid  = '" + UserName + "'";
                }
                SQL = SQL + "   ) ";
                SQL = SQL + " ORDER BY AD_MODULEITEM.AD_MD_MODULEID , AD_MODULEITEM.AD_MI_SL_NO ASC  ";




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

        public DataSet LoadMenuRights ( object mngrclassobj )
        {
            DataSet dsLoadVoucherRights = new DataSet();
            try
            {
                SQL = string.Empty;

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                SqlHelper clsSQLHelper = new SqlHelper();

                SQL = " select  distinct(AD_MI_ITEMID)   from ad_mail_approval_users  ";
                SQL = SQL + " WHERE    ";
                SQL = SQL + "  AD_MI_ITEMID in ('WTROR','WTPO','GLTGPO','RTMO')";
                SQL = SQL + "  AND  ad_mail_approval_users.ad_um_userid = '" + mngrclass.UserName + "'";


                dsLoadVoucherRights = clsSQLHelper.GetDataset(SQL);
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
            return dsLoadVoucherRights;
        }

        #endregion

        #region " POSTING VOUCHERS "

        public string PostVoucherDetails ( object mngrclassobj, List<MaterialReceiptNotificationEntity> objPosintDetailsEntity )
        {
            string sReturn = string.Empty;
            int RowCnt = 0;
            try
            {
                OracleHelper oTrns = new OracleHelper();
                SessionManager mngrclass = (SessionManager)mngrclassobj;
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                DataTable dtType = new DataTable();
                SqlHelper clsSQLHelper = new SqlHelper();
                SQLArray.Clear();
                foreach (var Data in objPosintDetailsEntity)
                {
                    switch (Data.oVoucherType)
                    {

                        #region "Raw Material Receipt entry "

                        case "RTMRCVER":
                            {
                                SQL = " update RM_RECEIPT_MASTER set RM_MRM_RECEIPT_VERIFIED='Y',RM_MRM_RECEIPT_VERIFIED_BY='" + mngrclass.UserName + "',RM_MRM_RECEIPT_VERIFIEDDT=sysdate ";
                                SQL = SQL + "  where  RM_MRM_RECEIPT_NO='" + Data.oVoucherNo + "' ";
                                SQL = SQL + " and  AD_FIN_FINYRID=" + Data.oVoucherFinYr + " ";
                                SQLArray.Add(SQL);
                            }
                            break;
                        case "RTMRCAP":
                            {
                                SQL = " update RM_RECEIPT_MASTER set RM_MRM_RECEIPT_APPROVED='Y',RM_MRM_RECEIPT_APPROVE_BY='" + mngrclass.UserName + "',RM_MRM_RECEIPT_APPROVEDDT=sysdate ";
                                SQL = SQL + "  where  RM_MRM_RECEIPT_NO='" + Data.oVoucherNo + "' ";
                                SQL = SQL + " and  AD_FIN_FINYRID=" + Data.oVoucherFinYr + " ";
                                SQLArray.Add(SQL);
                            }
                            break; 
                            
                            
                            case "RTSREVER":
                            {
                                SQL = " update RM_RECEIPT_MASTER set RM_MRM_RECEIPT_VERIFIED='Y',RM_MRM_RECEIPT_VERIFIED_BY='" + mngrclass.UserName + "',RM_MRM_RECEIPT_VERIFIEDDT=sysdate ";
                                SQL = SQL + "  where  RM_MRM_RECEIPT_NO='" + Data.oVoucherNo + "' ";
                                SQL = SQL + " and  AD_FIN_FINYRID=" + Data.oVoucherFinYr + " ";
                                SQLArray.Add(SQL);
                            }
                            break;
                        case "RTSREAPP":
                            {
                                SQL = " update RM_RECEIPT_MASTER set RM_MRM_RECEIPT_APPROVED='Y',RM_MRM_RECEIPT_APPROVE_BY='" + mngrclass.UserName + "',RM_MRM_RECEIPT_APPROVEDDT=sysdate ";
                                SQL = SQL + "  where  RM_MRM_RECEIPT_NO='" + Data.oVoucherNo + "' ";
                                SQL = SQL + " and  AD_FIN_FINYRID=" + Data.oVoucherFinYr + " ";
                                SQLArray.Add(SQL);
                            }
                            break;

                            #endregion


                            // workshop purchase entry  // as  pe the discussion with Mr.Jomy no need to add posting , since there are various entry types existing thsi may create hide buggs  dated 16 jul 2018 , do Alan
                    }
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

        public string RejectVoucher ( object mngrclassobj, string vouchertype, string Remark, string oVoucherNo, int oVoucherFinYr )
        {
            string sReturn = string.Empty;
            int RowCnt = 0;
            try
            {
                OracleHelper oTrns = new OracleHelper();

                SessionManager mngrclass = (SessionManager)mngrclassobj;

                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");

                SQLArray.Clear();

                switch (vouchertype)
                {
                    case "GLTGPO":
                        {

                            SQL = " UPDATE GL_GPO_MASTER SET ";
                            SQL = SQL + " GL_GPOM_PO_STATUS ='R',GL_GPOM_VERIFIED = 'N',GL_GPOM_VERIFIEDBY = '',";
                            SQL = SQL + " GL_GPO_POSTEDREMARKS = '" + Remark + "' ";
                            SQL = SQL + " WHERE GL_GPOM_GPO_NO = '" + oVoucherNo + "' AND AD_FIN_FINYRID = " + oVoucherFinYr + "";
                            SQLArray.Add(SQL);
                        }
                        break;

                    case "RTMO":
                        {
                            SQL = " UPDATE RM_PO_MASTER SET ";
                            SQL = SQL + " RM_PO_PO_STATUS ='R',RM_PO_VERIFIED = 'N',RM_PO_VERIFIEDBY = '',";
                            SQL = SQL + " RM_PO_POSTEDREMARKS= '" + Remark + "' ";
                            SQL = SQL + " WHERE RM_PO_PONO = '" + oVoucherNo + "' AND AD_FIN_FINYRID = " + oVoucherFinYr + "";
                            SQLArray.Add(SQL);
                        }
                        break;

                    case "WTPO":
                        {

                            SQL = " UPDATE WS_PO_MASTER SET ";
                            SQL = SQL + " WS_PO_PO_STATUS  ='R',WS_PO_VERIFIED = 'N',WS_PO_VERIFIEDBY = '',";
                            SQL = SQL + " WS_PO_POSTEDREMARKS= '" + Remark + "' ";
                            SQL = SQL + " WHERE WS_PO_PONO = '" + oVoucherNo + "' AND AD_FIN_FINYRID = " + oVoucherFinYr + "";

                            SQLArray.Add(SQL);
                        }
                        break;

                    case "WTROR":
                        {

                            SQL = " UPDATE WS_ROR_MASTER SET ";
                            SQL = SQL + " WS_ROR_DOC_STATUS  ='R',WS_ROR_VERIFIED = 'N',WS_ROR_VERIFIED_BY = '',";
                            SQL = SQL + " WS_ROR_POSTEDREMARKS= '" + Remark + "' ";
                            SQL = SQL + " WHERE WS_ROR_REQNNO = '" + oVoucherNo + "' AND AD_FIN_FINYRID = " + oVoucherFinYr + "";

                            SQLArray.Add(SQL);

                        }
                        break;
                    case "WTCUSTRNTORDR":
                        {
                            SQL = " UPDATE WS_CUST_RENT_ORDER_MASTER";
                            SQL = SQL + "    SET  WS_CRQM_PO_STATUS  = 'N', WS_CRQM_CANCEL_REMARKS ='" + Remark + "'";
                            SQL = SQL + "  WHERE WS_CROM_ORDER_NO = '" + oVoucherNo + "' AND WS_CRQM_DELETESTATUS  = 0 AND ad_fin_finyrid = " + oVoucherFinYr + "";

                            SQLArray.Add(SQL);

                        }
                        break;
                }

                string sRetun = string.Empty;

                sReturn = oTrns.SetTrans(mngrclass.UserName, mngrclass.FinYearID, mngrclass.CompanyCode, DateTime.Now, "APPROVAL", "0", false, Environment.MachineName, "U", SQLArray);
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


        #region " GET Pending WS Vouchers Details  "

        public double GetGrantedProjectCount ( string UserName )
        {

            DataTable dtReturn = new DataTable();
            double dGrantedProjectCount = 0;
            try
            {

                String SQL = string.Empty;
                SqlHelper clsSQLHelper = new SqlHelper();


                SQL = "    Select count(*)      GRANTEDPROJECTCOUNT   FROM SL_GRANTED_PROJECT_USERS ";
                SQL = SQL + "  WHERE AD_UM_USERID =  '" + UserName + "'";

                dtReturn = clsSQLHelper.GetDataTableByCommand(SQL);
                dGrantedProjectCount = double.Parse(dtReturn.Rows[0]["GRANTEDPROJECTCOUNT"].ToString());



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
            return dGrantedProjectCount;

        }
        #endregion



        #region "For AllAproving Farpoint"

        public DataSet GetReceiptDocPendingAppDetails ( object mngrclassobj, string TypeId )
        {
            SessionManager mngrclass = (SessionManager)mngrclassobj;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                double dGrantedProjectCount = GetGrantedProjectCount(mngrclass.UserName);

                SQL = " SELECT";
                SQL = SQL + "   DOCTYPE, DOCNO   ,PONO,FINYR ,";
                SQL = SQL + "     ENTRYDATE,";
                SQL = SQL + "   PARTYCODE,  PARTYNAME,Item_Code,ITEM_DESC,STATION_CODE, STATION_NAME,  PAYTERM, ";
                SQL = SQL + "   AMOUNT ,  UNMATCHBALANCE,";
                SQL = SQL + "     VENDOR_DOC_NO ,     TRNSPORTER_DOC_NO ,   TRNASPORTER_NAME, ";
                SQL = SQL + "   NARRATION,";
                SQL = SQL + "   CREATEDBY,   VERIFIEDBY,";
                SQL = SQL + "   APPROVEDBY, REMARKS, STATUS , ACTIONTAG,BRANCH_CODE,BRANCH,SOURCE_DESC from (";


                #region "Raw Material" 


                if (TypeId == "RTMRCVER")
                {
                    #region "Raw Material Material Reciept Verification"
                    

                    SQL = SQL + "SELECT  ";
                    SQL = SQL + "        'RTMRC' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO,M.AD_FIN_FINYRID FINYR ,";


                    // SQL = SQL + "  case when TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM' then  to_char(M.RM_MRM_RECEIPT_DATE - 1,'dd-mon-yyyy')   else to_char(M.RM_MRM_RECEIPT_DATE ,'dd-mon-yyyy')  end as ENTRYDATE   ,  ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE   ENTRYDATE,  ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,RM_RECEPIT_DETAILS.RM_RMM_RM_CODE Item_Code,";
                    SQL = SQL + "     RM_RMM_RM_DESCRIPTION ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE,";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, RM_MRD_SUPPLY_QTY PAYTERM,   ";
                    SQL = SQL + "        RM_MRD_WEIGH_QTY     AMOUNT,  RM_MRD_APPROVE_QTY  UNMATCHBALANCE,   ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, ";
                    SQL = SQL + "      RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";
                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,  ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,  ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH   ,RM_SM_SOURCE_DESC  SOURCE_DESC    ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,  RM_VENDOR_MASTER Transorter,  ";
                    SQL = SQL + "     AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER,RM_SOURCE_MASTER ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE = Transorter.RM_VM_VENDOR_CODE  (+)   ";

                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)       ";
                    SQL = SQL + "    and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE and  m.RM_PO_RM_TYPE <>'ST' ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='N' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'  ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }

                    #endregion

                }

                else if (TypeId == "RTMRCAP")
                {
                    #region "Raw Material Material Reciept Approval"

                    

                    SQL = SQL + "SELECT  ";
                    SQL = SQL + "        'RTMRC' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO, M.AD_FIN_FINYRID FINYR ,  ";
                    // SQL = SQL + "  case when TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME,'HH24:MI:SS') < '07:00:00 AM' then  to_char(M.RM_MRM_RECEIPT_DATE - 1,'dd-mon-yyyy')   else  to_char(M.RM_MRM_RECEIPT_DATE ,'dd-mon-yyyy')  end as ENTRYDATE   ,  ";

                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE  ENTRYDATE,  ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,RM_RECEPIT_DETAILS.RM_RMM_RM_CODE Item_Code,";
                    SQL = SQL + "     RM_RMM_RM_DESCRIPTION ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE,";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, RM_MRD_SUPPLY_QTY PAYTERM,   ";
                    SQL = SQL + "        RM_MRD_WEIGH_QTY     AMOUNT,  RM_MRD_APPROVE_QTY  UNMATCHBALANCE,   "; 

                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";


                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,  ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,  ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH    ,RM_SM_SOURCE_DESC  SOURCE_DESC    ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,  ";
                    SQL = SQL + "        AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER,RM_SOURCE_MASTER  ,   RM_VENDOR_MASTER Transorter   ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "         and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)    and  m.RM_PO_RM_TYPE <>'ST'     ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";


                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='Y' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'  ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }

                    #endregion

                }
                else if (TypeId == "RTSREVER")
                {
                    #region "Raw Material Steel Reciept Verification"


                    SQL = SQL + "SELECT   ";
                    SQL = SQL + "     distinct   'RTSRE' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO,M.AD_FIN_FINYRID FINYR , ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE   ENTRYDATE,   ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,'' Item_Code, ";
                    SQL = SQL + "    '' ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE, ";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, 0 PAYTERM,    ";
                    SQL = SQL + "      0   AMOUNT,  0  UNMATCHBALANCE,    ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,   ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,   ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH   ,''  SOURCE_DESC      ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,      RM_VENDOR_MASTER Transorter,    ";
                    SQL = SQL + "  AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER ,RM_SOURCE_MASTER  ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE    ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)        ";
                    SQL = SQL + "    and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE and  m.RM_PO_RM_TYPE ='ST'  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='N' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'   ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }
                    #endregion
                }

                else if (TypeId == "RTSREAPP")
                {
                    #region "Raw Material Steel Reciept Approval"

                    SQL = SQL + "SELECT   ";
                    SQL = SQL + "     distinct   'RTSRE' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO,M.AD_FIN_FINYRID FINYR , ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE   ENTRYDATE,   ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,'' Item_Code, ";
                    SQL = SQL + "    '' ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE, ";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, 0 PAYTERM,    ";
                    SQL = SQL + "      0   AMOUNT,  0  UNMATCHBALANCE,    ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,   ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,   ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH   ,''  SOURCE_DESC      ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,     RM_VENDOR_MASTER Transorter,    ";
                    SQL = SQL + "      AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER ,RM_SOURCE_MASTER  ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE    ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "    and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+)  ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)        ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE and  m.RM_PO_RM_TYPE ='ST'  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='Y' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'   ";

                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }

                    #endregion

                }



                #endregion

                SQL = SQL + " )";

                if (mngrclass.UserName != "ADMIN")
                {
                    if (mngrclass.NotificationOverrideYN != "Y")
                    {
                        SQL = SQL + " where ACTIONTAG in ( ";
                        SQL = SQL + " SELECT  distinct  AD_AF_ID";
                        SQL = SQL + "  FROM ad_mail_approval_users";
                        SQL = SQL + "  WHERE AD_AF_ID IN (";
                        SQL = SQL + "  'RTMRCVER','RTMRCAP','RTSREAPP','RTSREVER' )";
                        SQL = SQL + "  AND UPPER (ad_um_userid) = '" + mngrclass.UserName + "' )";
                    }
                }
                SQL = SQL + "  ORDER BY DOCNO Asc ";


                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);
                dsVouchers.Tables.Add(dtMain);
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
            return dsVouchers;
        }


        public DataSet GetReceiptPendingAppDetailsWithFiltering( object mngrclassobj, string TypeId,
              string Item, string sFromDate, string sTodate, string Location, string Division,
              string CreatedBy, string Party )
        {
            SessionManager mngrclass = (SessionManager)mngrclassobj;
            try
            {
                SqlHelper clsSQLHelper = new SqlHelper();
                double dGrantedProjectCount = GetGrantedProjectCount(mngrclass.UserName);
                CultureInfo culture = new System.Globalization.CultureInfo("en-GB");
                string BatchFromDate = string.Empty;
                string BatchToDate = string.Empty;


                SQL = " SELECT";
                SQL = SQL + "   DOCTYPE, DOCNO,PONO   ,FINYR ,";
                SQL = SQL + "     ENTRYDATE,";
                SQL = SQL + " PARTYCODE,  PARTYNAME,Item_Code,ITEM_DESC,STATION_CODE, STATION_NAME,  PAYTERM, ";
                SQL = SQL + "   AMOUNT ,  UNMATCHBALANCE,";
                SQL = SQL + "     VENDOR_DOC_NO ,     TRNSPORTER_DOC_NO ,   TRNASPORTER_NAME, ";
                SQL = SQL + "   NARRATION,";
                SQL = SQL + "   CREATEDBY,   VERIFIEDBY,";
                SQL = SQL + "   APPROVEDBY, REMARKS, STATUS , ACTIONTAG,BRANCH_CODE,BRANCH,SOURCE_DESC from (";


                #region "Raw Material" 


                if (TypeId == "RTMRCVER")
                {
                    #region "Raw Material Material Reciept Verification"
                     

                    SQL = SQL + "SELECT  ";
                    SQL = SQL + "        'RTMRC' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO, M.AD_FIN_FINYRID FINYR , ";// M.RM_MRM_RECEIPT_DATE ENTRYDATE,  ";
                    SQL = SQL + "  M.RM_MRM_RECEIPT_DATE    ENTRYDATE,  ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,RM_RECEPIT_DETAILS.RM_RMM_RM_CODE Item_Code,";
                    SQL = SQL + "     RM_RMM_RM_DESCRIPTION ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE,";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, RM_MRD_SUPPLY_QTY PAYTERM,   ";
                    SQL = SQL + "        RM_MRD_WEIGH_QTY     AMOUNT,  RM_MRD_APPROVE_QTY  UNMATCHBALANCE,   ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,  ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,  ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH   ,RM_SM_SOURCE_DESC  SOURCE_DESC    ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,    RM_VENDOR_MASTER Transorter,  ";
                    SQL = SQL + "  AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER,RM_SOURCE_MASTER  ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  ";

                    SQL = SQL + "        and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+) ";

                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)       ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='N' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'  ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }


                    if (Item != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE='" + Item.Trim() + "' ";
                    }
                  

                    if (oDateTimeFilterType == "Date")
                    {

                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    }
                    else if (oDateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(oFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(oToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }


                    if (Location != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE='" + Location.Trim() + "' ";
                    }
                    if (Division != "0")
                    {
                        SQL = SQL + "        and m.AD_BR_CODE='" + Division.Trim() + "'     ";
                    }
                    if (CreatedBy != "0")
                    {
                        SQL = SQL + "        and RM_MRM_CREATEDBY='" + CreatedBy.Trim() + "'     ";
                    }
                    if (Party != "0")
                    {
                        SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = '" + Party.Trim() + "' ";
                    }

                    #endregion

                }

                else if (TypeId == "RTMRCAP")
                {
                    #region "Raw Material Material Reciept Approval"

                   

                    SQL = SQL + "SELECT  ";
                    SQL = SQL + "        'RTMRC' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO,M.RM_MPOM_ORDER_NO PONO, M.AD_FIN_FINYRID FINYR , ";// M.RM_MRM_RECEIPT_DATE ENTRYDATE,  ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE ENTRYDATE   ,  ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,RM_RECEPIT_DETAILS.RM_RMM_RM_CODE Item_Code,";
                    SQL = SQL + "     RM_RMM_RM_DESCRIPTION ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE,";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, RM_MRD_SUPPLY_QTY PAYTERM,   ";
                    SQL = SQL + "        RM_MRD_WEIGH_QTY     AMOUNT,  RM_MRD_APPROVE_QTY  UNMATCHBALANCE,   ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,  ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,  ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH ,RM_SM_SOURCE_DESC  SOURCE_DESC       ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,    RM_VENDOR_MASTER Transorter,  ";
                    SQL = SQL + "       AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER ,RM_SOURCE_MASTER ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO  "; 
                    SQL = SQL + "        and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+) ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE  ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)       ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='Y' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'  ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }


                    if (Item != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE='" + Item.Trim() + "' ";
                    }
                   

                    if (oDateTimeFilterType == "Date")
                    {

                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    }
                    else if (oDateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(oFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(oToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }

                    if (Location != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE='" + Location.Trim() + "' ";
                    }
                    if (Division != "0")
                    {
                        SQL = SQL + "        and m.AD_BR_CODE='" + Division.Trim() + "'     ";
                    }
                    if (CreatedBy != "0")
                    {
                        SQL = SQL + "        and RM_MRM_CREATEDBY='" + CreatedBy.Trim() + "'     ";
                    }
                    if (Party != "0")
                    {
                        SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = '" + Party.Trim() + "' ";
                    }



                    #endregion

                }


                else if (TypeId == "RTSREVER")
                {
                    #region "Raw Material Steel Reciept Verification"


                    SQL = SQL + "SELECT   ";
                    SQL = SQL + "     distinct   'RTSRE' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO,M.AD_FIN_FINYRID FINYR , ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE   ENTRYDATE,   ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,'' Item_Code, ";
                    SQL = SQL + "    '' ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE, ";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, 0 PAYTERM,    ";
                    SQL = SQL + "      0   AMOUNT,  0  UNMATCHBALANCE,    ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,   ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,   ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH  ,''  SOURCE_DESC       ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,     RM_VENDOR_MASTER Transorter,  ";
                    SQL = SQL + "  AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER ,RM_SOURCE_MASTER  ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE    ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)        ";
                    
                    SQL = SQL + "        and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+) ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE and  m.RM_PO_RM_TYPE ='ST'  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='N' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'   ";
                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }

                    

                    if (Item != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE='" + Item.Trim() + "' ";
                    }
                    

                    if (oDateTimeFilterType == "Date")
                    {

                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    }
                    else if (oDateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(oFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(oToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }

                    if (Location != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE='" + Location.Trim() + "' ";
                    }
                    if (Division != "0")
                    {
                        SQL = SQL + "        and m.AD_BR_CODE='" + Division.Trim() + "'     ";
                    }
                    if (CreatedBy != "0")
                    {
                        SQL = SQL + "        and RM_MRM_CREATEDBY='" + CreatedBy.Trim() + "'     ";
                    }
                    if (Party != "0")
                    {
                        SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = '" + Party.Trim() + "' ";
                    }

                    #endregion
                }

                else if (TypeId == "RTSREAPP")
                {
                    #region "Raw Material Steel Reciept Approval"

                    SQL = SQL + "SELECT   ";
                    SQL = SQL + "     distinct   'RTSRE' DOCTYPE, M.RM_MRM_RECEIPT_NO DOCNO, M.RM_MPOM_ORDER_NO PONO,M.AD_FIN_FINYRID FINYR , ";
                    SQL = SQL + " M.RM_MRM_RECEIPT_DATE   ENTRYDATE,   ";
                    SQL = SQL + "     m.RM_VM_VENDOR_CODE PARTYCODE,    coa.RM_VM_VENDOR_NAME PARTYNAME,'' Item_Code, ";
                    SQL = SQL + "    '' ITEM_DESC,RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE  STATION_CODE, ";
                    SQL = SQL + "    SALES_STN_STATION_NAME STATION_NAME, 0 PAYTERM,    ";
                    SQL = SQL + "      0   AMOUNT,  0  UNMATCHBALANCE,    ";
                    SQL = SQL + "        RM_MRD_VENDOR_DOC_NO  VENDOR_DOC_NO ,   RM_MRM_TRNSPORTER_DOC_NO TRNSPORTER_DOC_NO , Transorter.RM_VM_VENDOR_NAME TRNASPORTER_NAME, RM_RECEPIT_DETAILS.RM_MRM_VEHICLE_DESCRIPTION  narration,   ";

                    SQL = SQL + "        RM_MRM_CREATEDBY  CREATEDBY,  RM_MRM_RECEIPT_VERIFIED_BY VERIFIEDBY,   ";
                    SQL = SQL + "        RM_MRM_RECEIPT_APPROVE_BY APPROVEDBY,   ";
                    SQL = SQL + "        RM_MRM_REMARKS REMARKS,'' STATUS, '" + TypeId + "' ACTIONTAG,m.AD_BR_CODE BRANCH_CODE,AD_BR_NAME BRANCH   ,''  SOURCE_DESC      ";
                    SQL = SQL + "     FROM RM_RECEIPT_MASTER m, RM_RECEPIT_DETAILS ,  RM_VENDOR_MASTER coa,     RM_VENDOR_MASTER Transorter,  ";
                    SQL = SQL + "  AD_Branch_Master ,RM_RAWMATERIAL_MASTER,SL_STATION_MASTER ,RM_SOURCE_MASTER  ";
                    SQL = SQL + "        WHERE coa.RM_VM_VENDOR_CODE= m.RM_VM_VENDOR_CODE    ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_NO = RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_NO   ";
                    SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = coa.RM_VM_VENDOR_CODE   ";
                    SQL = SQL + "        and m.AD_BR_CODE=AD_Branch_Master.AD_BR_CODE(+)        ";

                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_MRM_TRANSPORTER_CODE =Transorter.RM_VM_VENDOR_CODE (+)   ";
                    SQL = SQL + "        and RM_RECEPIT_DETAILS.RM_SM_SOURCE_CODE= RM_SOURCE_MASTER.RM_SM_SOURCE_CODE(+) ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE=RM_RAWMATERIAL_MASTER.RM_RMM_RM_CODE and  m.RM_PO_RM_TYPE ='ST'  ";
                    SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE=SL_STATION_MASTER.SALES_STN_STATION_CODE  ";
                    SQL = SQL + "        AND m.RM_MRM_RECEIPT_VERIFIED ='Y' and m.RM_MRM_RECEIPT_APPROVED='N' and m.RM_MRM_APPROVED_STATUS='N'   ";

                    if (mngrclass.UserName != "ADMIN")
                    {
                        SQL = SQL + " AND   m.AD_BR_CODE  IN  ";
                        SQL = SQL + "   ( SELECT   AD_BR_CODE   FROM    AD_USER_GRANTED_BRANCH  WHERE AD_UM_USERID ='" + mngrclass.UserName + "' )";
                    }


                    

                    if (Item != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.RM_RMM_RM_CODE='" + Item.Trim() + "' ";
                    }
                     
                    if (oDateTimeFilterType == "Date")
                    {

                        SQL = SQL + " AND TO_DATE(TO_CHAR(RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE,'DD-MON-YYYY'))  BETWEEN '" + System.Convert.ToDateTime(sFromDate).ToString("dd-MMM-yyyy") + "' AND '" + System.Convert.ToDateTime(sTodate).ToString("dd-MMM-yyyy") + "'";


                    }
                    else if (oDateTimeFilterType == "Time")
                    {
                        SQL = SQL + "    AND   RM_RECEPIT_DETAILS.RM_MRM_RECEIPT_DATE_TIME  between  TO_DATE('" + System.Convert.ToDateTime(oFromDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                        SQL = SQL + "    AND  TO_DATE('" + System.Convert.ToDateTime(oToDateTime, culture).ToString("dd-MMM-yyyy hh:mm:ss tt") + "','DD-MM-YYYY HH:MI:SS AM')";

                    }

                    if (Location != "0")
                    {
                        SQL = SQL + "        AND RM_RECEPIT_DETAILS.SALES_STN_STATION_CODE='" + Location.Trim() + "' ";
                    }
                    if (Division != "0")
                    {
                        SQL = SQL + "        and m.AD_BR_CODE='" + Division.Trim() + "'     ";
                    }
                    if (CreatedBy != "0")
                    {
                        SQL = SQL + "        and RM_MRM_CREATEDBY='" + CreatedBy.Trim() + "'     ";
                    }
                    if (Party != "0")
                    {
                        SQL = SQL + "        AND m.RM_VM_VENDOR_CODE = '" + Party.Trim() + "' ";
                    }

                    #endregion

                }
                #endregion

                SQL = SQL + " )";

                if (mngrclass.UserName != "ADMIN")
                {
                    if (mngrclass.NotificationOverrideYN != "Y")
                    {
                        SQL = SQL + " where ACTIONTAG in ( ";
                        SQL = SQL + " SELECT  distinct  AD_AF_ID";
                        SQL = SQL + "  FROM ad_mail_approval_users";
                        SQL = SQL + "  WHERE AD_AF_ID IN (";
                        SQL = SQL + "  'RTMRCVER','RTMRCAP','RTSREAPP','RTSREVER'  )";
                        SQL = SQL + "  AND UPPER (ad_um_userid) = '" + mngrclass.UserName + "' )";
                    }
                }
                SQL = SQL + "  ORDER BY DOCNO Asc ";


                dtMain = clsSQLHelper.GetDataTableByCommand(SQL);
                dsVouchers.Tables.Add(dtMain);
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
            return dsVouchers;
        }

        #endregion

    }


    public class MaterialReceiptNotificationEntity
    {
        public string oVoucherType { get; set; }
        public string oVoucherNo { get; set; }
        public string oVoucherFinYr { get; set; }
        public DateTime oDocDate { get; set; }
        public string oVoucherApprovalType { get; set; } // this is verification or approval tag must be passed / rejected or etc.. 



    }

}
