import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import subprocess, os, sys

sys.path.insert(0, r'C:\Users\100700375\Documents\py_projects\MAD\src\data')
from data.setup import Setup
from data.mad import MAD
from data.timer import Timer


#carico i parametri di setup
setup= Setup()
#creo l'oggetto MAD per la connessione al db
db = MAD(setup.Credentials)
#apro la connessione al db
db.open_connection()
#carico in locale i parametri di query e arricchimento dati
min_date=setup.Parameters.min_date
plant_in=setup.Parameters.plant_in
status_samord_min=setup.Parameters.status_samord_min
status_cusreq_min=setup.Parameters.status_cusreq_min
days_on_time=int(setup.Parameters.days_on_time)

#SQL per i dati di samord
RetriveSamOrd_query=f"""SELECT
             SAMORD.IDSamReq as CusReq, SAMORD.IDSamOrd as OrdNum, CONCAT(SAMORD.IDSamReq,"-",SAMORD.IDSamOrd) as IDOrd,
             ORDSTS.StatoOrd as Status,
             ORDSTS.DesStato_ITA AS StatusDesc,
             IF(SAMORD.isStd=-1,'STD','DEV') AS Type,
             IF(ISNULL(SAMORD.StdSapCod) OR TRIM(SAMORD.StdSapCod)='' ,SAMORD.IDConverted,SAMORD.StdSapCod) AS PrdID,
             IF(ISNULL(SAMORD.StdSapDes) OR TRIM(SAMORD.StdSapDes)='',CONVERTED.DescrizioneConv,SAMORD.StdSapDes) AS PrdDesc,
             SAMORD.CostoTOT as TotCost,
             IF(SAMORD.IsStd=-1,1,0) as IsStdNr,
             IF(SAMORD.IsStd=0,1,0) as IsDevNr,
             IF(SAMORD.IsFree=-1,'Y','N') AS IsFree,
             IF(SAMORD.IsTest=-1,'Y','N') AS IsTest,
             CLIENTI.Cliente as CustomerName,
             SUBSTRING(Plant.Plant,1,3) as PlantName,
             SUBSTRING(BU.BusinesseUnit,1,2) as BU,
             USRREQ.EmployeeName as EmployeeReq,
             SAMORD.DateIns,
             SAMORD.DateDlvReq,
             SAMORD.DateDlvPlanned,
             SAMORD.DateEndPrd,
             IF(ISNULL(SAMORD.DateEndPrd),GREATEST(IF(ISNULL(SAMORD.DateDlvPlanned), GREATEST(SAMORD.DateDlvReq,CURRENT_DATE()),SAMORD.DateDlvPlanned),CURRENT_DATE()),SAMORD.DateEndPrd) as DateConfirmed
             FROM (tblsamorders AS SAMORD INNER JOIN tlkpplant AS Plant ON SAMORD.IDPlantOrd=Plant.IDPlant 
                   INNER JOIN ztblsamordsts AS ORDSTS ON SAMORD.StatoOrd=ORDSTS.StatoOrd 
                   ) 
             INNER JOIN (
                 tblsamrequests SAMREQ INNER JOIN tblclienti as CLIENTI ON SAMREQ.IDCliente=CLIENTI.IDCliente 
                                       INNER JOIN tlkpbu as BU ON SAMREQ.IDBU=BU.IDBU 
                                       INNER JOIN tblemployees AS USRREQ ON SAMREQ.IDEmpReq=USRREQ.EmployeeID
                         ) 
             ON SAMORD.IDSamReq=SAMREQ.IDSamReq 
             LEFT JOIN tblconverted as CONVERTED ON SAMORD.IDConverted=CONVERTED.IDConverted
             WHERE SAMORD.DateIns>='{min_date}' AND Plant.IDPlant IN {plant_in} AND ORDSTS.StatoOrd >{status_samord_min};""" 
#SQL per i dati di cusreq
RetriveCusReq_query=f"""SELECT 
      SAMREQ.IDSamReq as CusReq,
      CLIENTI.Cliente as CustomerName,
      SAMREQ.TitoloReq as Title,
      STATO.DesStato_ITA as StatusDesc,
      SAMREQ.StatoReq as Status,
      IF(SAMREQ.MADType=1,'STD','DEV') AS ReqType,
      IF(SAMREQ.MADType=1,1,0) as IsStdNr,
      IF(SAMREQ.MADType<>1,1,0) as IsDevNr,
      MAD.MADDesc as MAD,
      SUBSTRING(Plant.Plant,1,3) as PlantName,
      SUBSTRING(BU.BusinesseUnit,1,2) as BU,
      IF(SAMREQ.IsZZ=2,'Y','N') as IsZZ,    #2=ZZ, 3=Non ZZ
      CASE SAMREQ.IsNewPrd
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'New'
         WHEN 3 THEN 'SAP'
      END as IsNewPrd,
      CASE SAMREQ.IsConverted
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Converted'
         WHEN 3 THEN 'Foam'
      END as Conv_Foam,
      CASE SAMREQ.IsGreenList
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Y'
         WHEN 3 THEN 'N'
      END as InGreenList,
      CASE SAMREQ.IsStdMod
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Std_RM_Proc'
         WHEN 3 THEN 'New_RM_Proc'
      END as StdNew,
      CASE SAMREQ.IsBasic
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Basic'
         WHEN 3 THEN 'Custom'
      END as Specification,
      CASE SAMREQ.IsNewBU
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'New'
         WHEN 3 THEN 'Existing'
      END as NewBusiness,
      CASE SAMREQ.IsSample
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Y'
         WHEN 3 THEN 'N'
      END as SampleRequired,
      CASE SAMREQ.IsCost
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Y'
         WHEN 3 THEN 'N'
      END as CostRequired,
      SAMREQ.QtyYear*SAMREQ.PriceTgt as TO_TGT,
      SAMREQ.QtyYear*SAMREQ.PriceCur as TO_ACT,
      SAMREQ.GMTgt as MRG_TGT,
      SAMREQ.GMCur as MRG_ACT,
      CASE SAMREQ.IsSampleApproved
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Y'
         WHEN 3 THEN 'N'
      END as SampleApproved,
      CASE SAMREQ.IsRequestApproved
         WHEN 1 THEN 'NA'
         WHEN 2 THEN 'Y'
         WHEN 3 THEN 'N'
      END as RequestApproved,
      SAMREQ.DateIns,SAMREQ.DateAssessment,SAMREQ.DateMod,SAMREQ.DateSalesBegin,
      EMP2.EmployeeName as EmployeeReq,EMP.EmployeeName as EvalMngName
      FROM tblsamrequests as SAMREQ 
      INNER JOIN tlkpplant as Plant on SAMREQ.IDPlantReq= Plant.IDPlant 
      INNER JOIN tlkpbu as BU ON SAMREQ.IDBU=BU.IDBU 
      INNER JOIN ztblsamreqsts as STATO ON SAMREQ.StatoReq=STATO.StatoReq 
      INNER JOIN tblclienti as CLIENTI ON SAMREQ.IDCliente=CLIENTI.IDCliente 
      LEFT JOIN tlkpmad as MAD ON SAMREQ.IDMAD=MAD.IDMAD
      LEFT JOIN tblemployees as EMP ON SAMREQ.IDEmployeeEvalMngr =EMP.EmployeeID 
      LEFT JOIN tblemployees as EMP2 ON SAMREQ.IDEmpIns =EMP2.EmployeeID
      WHERE SAMREQ.DateIns>='{min_date}' AND Plant.IDPlant IN {plant_in} AND STATO.StatoReq >{status_cusreq_min};"""

#eseguo le due query
Timer.Display('Import SamOrd','[..]',cr=0)
#leggo e aggiungo delle colonne
df_SamOrd=(db.execute_query(RetriveSamOrd_query)
        .assign(otd_delay_days = lambda df_: (df_.DateConfirmed-df_.DateDlvPlanned).dt.days)
        .assign(ttm_delay_days = lambda df_: (df_.DateConfirmed-df_.DateDlvReq).dt.days) 
        .assign(LTR= lambda df_: np.where((df_.DateDlvReq-df_.DateIns).dt.days>=0,(df_.DateDlvReq-df_.DateIns).dt.days,0))
        .assign(LTPlan= lambda df_:  np.where((df_.DateDlvPlanned-df_.DateIns).dt.days>=0,(df_.DateDlvPlanned-df_.DateIns).dt.days,0))
        .assign(LTPrd= lambda df_:  np.where((df_.DateEndPrd-df_.DateIns).dt.days>=0,(df_.DateEndPrd-df_.DateIns).dt.days,0))
        .assign(LTConf=lambda df_:  np.where((df_.DateConfirmed-df_.DateIns).dt.days>=0,(df_.DateConfirmed-df_.DateIns).dt.days,0))
        .assign(RIT=lambda df_:df_.LTConf-df_.LTR)
        .assign(ANNO=lambda df_:df_.DateConfirmed.dt.year )
        .assign(MESE=lambda df_:df_.DateConfirmed.dt.month )
        .assign(TTM=lambda df_:np.select([df_.ttm_delay_days<=days_on_time],[100],0 ))
        .assign(OTD=lambda df_:np.select([df_.otd_delay_days<=days_on_time],[100],0 ))
        .sort_values(by=['DateIns'],ascending=[True])
          )
Timer.Display('Import SamOrd','OK')

Timer.Display('Import CusReq','[..]',cr=0)
df_Cusreq= (db.execute_query(RetriveCusReq_query)
                .assign(LTR= 2)    
                .assign(LTRis= lambda df_: (df_.DateAssessment.fillna(pd.Timestamp.today())-df_.DateIns).dt.days)
                .assign(RIT=lambda df_:df_.LTRis-df_.LTR)
                .assign(ANNO=lambda df_:df_.DateAssessment.fillna(pd.Timestamp.today()).dt.year )
                .assign(MESE=lambda df_:df_.DateAssessment.fillna(pd.Timestamp.today()).dt.month )
                .sort_values(by=['DateIns'],ascending=[True])
)
Timer.Display('Import CusReq','OK')
db.close_connection()

#salvo i dataframe su file excel
db.SaveFile(df_SamOrd,setup.Paths.SamOrd)
db.SaveFile(df_Cusreq,setup.Paths.CusReq)

#print(df_SamOrd)
