

################################################################################
################### reasignacion de perfiles ####################################
################################################################################
  
  args <- commandArgs(TRUE)
  
  if(length(args)==0){
    print("Sin argumentos")
  }else{
    CodPais <- args[1]  
    Campania <- args[2]
    Palanca <- args[3]
    centroidess <- args[4]
    modelo <- args[5]
    print(CodPais)
    print(Campania)
    print(Palanca)
    print(centroidess)
    print(modelo)
  }
  
  ##########
  
  
  
  #install.packages("RODBC")
  #install.packages("stringr")
  library(RODBC)
  library(stringr)
  
  CodPais <- 'CO'
  Campania <- '201709'
  Palanca <- '0'
  centroidess <- 'CentroidesSV.RData'
  modelo <- 'ModeloSV.RData'
  
  #Conexión
  canal<-odbcConnect("DMANALITICOCLOUD", uid="usrdm", pwd="dm$2Admin4")
  
  
  ###función de Reasignación ###########################################
  
  clusters <- function(x, centers) {
    tmp <- sapply(seq_len(nrow(x)),
                  function(i) apply(centers, 1,
                                    function(v) sum((x[i, ]-v)^2)))
    max.col(-t(tmp)) }
  
  setwd("d:/Users/gnunuvero/Desktop/Nueva carpeta/modelos No supervisados C08")
  
  
  load(centroidess)
  load(modelo)
  ###load("DATASV.RData")
  
  
  ScripPerf<-sprintf("SELECT CodPais, AnioCampanaExposicion, AnioCampanaProceso, PkEbelista,CodEbelista, PPU_Lbel as PPU_Lb,PPU_Esika as PPU_Ek,PPU_Cyzone as PPU_Cy,PPU_CP,PPU_FG,PPU_MQ,PPU_TC,PPU_TF,LEN(CodEbelista) as LenCodEbelista FROM [MDL_PerfilInput] WHERE CodPais='%s'and AnioCampanaProceso='%s' and PkPalanca=%s", CodPais,Campania,Palanca)
  print(ScripPerf)
  ###datopred1<-sqlQuery(canal,"SELECT CodPais, AnioCampanaExposicion, AnioCampanaProceso, PkEbelista,CodEbelista, PPU_Lbel as PPU_Lb,PPU_Esika as PPU_Ek,PPU_Cyzone as PPU_Cy,PPU_CP,PPU_FG,PPU_MQ,PPU_TC,PPU_TF,LEN(CodEbelista) as LenCodEbelista from [dbo].[MDL_PerfilInput]  where aniocampanaproceso between  '201705' and '201707' and CodPais = 'CO'")
  PPU_O<-sqlQuery(canal,ScripPerf)
  PPU_O<-PPU_O[order(PPU_O$CodEbelista),]
  i<-max(PPU_O$LenCodEbelista)
  PPU_O$CodEbelista<-str_pad(PPU_O$CodEbelista, i, pad = "0")
  
  
  ScripPerf<-sprintf("SELECT CodPais, AnioCampanaExposicion, AnioCampanaProceso,PkEbelista,CodEbelista,PUP_Lbel as PUP_Lb,PUP_Esika as PUP_Ek,PUP_Cyzone as PUP_Cz,PUP_CP,PUP_FG,PUP_MQ,PUP_TC,PUP_TF,LEN(CodEbelista) as LenCodEbelista  FROM [MDL_PerfilInput] WHERE CodPais='%s'and AnioCampanaProceso='%s' and PkPalanca=%s", CodPais,Campania,Palanca)
  PUP_O<-sqlQuery(canal,ScripPerf)
  ##datopred2<-sqlQuery(canal,"SELECT CodPais, AnioCampanaExposicion, AnioCampanaProceso,PkEbelista,CodEbelista,PUP_Lbel as PUP_Lb,PUP_Esika as PUP_Ek,PUP_Cyzone as PUP_Cz,PUP_CP,PUP_FG,PUP_MQ,PUP_TC,PUP_TF,LEN(CodEbelista) as LenCodEbelista  FROM [MDL_PerfilInput] where aniocampanaproceso between  '201705' and '201707' and CodPais = 'CO'")
  PUP_O<-sqlQuery(canal,ScripPerf)
  PUP_O<-PUP_O[order(PUP_O$PkEbelista),]
  i<-max(PUP_O$LenCodEbelista)
  PUP_O$CodEbelista<-str_pad(PUP_O$CodEbelista, i, pad = "0")
  
  SumMarca_PUP<-data.frame(PUP_O$PUP_Cz+PUP_O$PUP_Ek+PUP_O$PUP_Lb)
  SumCategoria_PUP<-data.frame(PUP_O$PUP_TF+PUP_O$PUP_FG+PUP_O$PUP_MQ+PUP_O$PUP_CP+PUP_O$PUP_TC)
  
  Pc_cz_PUP<- PUP_O$PUP_Cz/(SumMarca_PUP)
  Pc_Ek_PUP<- PUP_O$PUP_Ek/(SumMarca_PUP)
  Pc_Lb_PUP<- PUP_O$PUP_Lb/(SumMarca_PUP)
  Pc_Cp_PUP<- PUP_O$PUP_CP/(SumCategoria_PUP)
  Pc_Fg_PUP<- PUP_O$PUP_FG/(SumCategoria_PUP)
  Pc_Mq_PUP<- PUP_O$PUP_MQ/(SumCategoria_PUP)
  Pc_Tc_PUP<- PUP_O$PUP_TC/(SumCategoria_PUP)
  Pc_Tf_PUP<- PUP_O$PUP_TF/(SumCategoria_PUP)
  
  DataPerfil_PUP<-cbind(PUP_O$CodPais,PUP_O$AnioCampanaExposicion,PUP_O$AnioCampanaProceso,PUP_O$PkEbelista,PUP_O$CodEbelista, Pc_Lb_PUP, Pc_Ek_PUP, Pc_cz_PUP,
                        Pc_Cp_PUP, Pc_Fg_PUP, Pc_Mq_PUP, Pc_Tc_PUP, Pc_Tf_PUP)
  
  names(DataPerfil_PUP)<-c("CodPais","AnioCampanaExposicion","AnioCampanaProceso","PkEbelista","CodEbelista","Pc_Lb_PUP","Pc_Ek_PUP","Pc_cz_PUP",
                           "Pc_Cp_PUP","Pc_Fg_PUP","Pc_Mq_PUP","Pc_Tc_PUP","Pc_Tf_PUP")
  
  rm(list=c('Pc_Lb_PUP', 'Pc_Ek_PUP', 'Pc_cz_PUP', 'Pc_Cp_PUP', 'Pc_Fg_PUP', 'Pc_Mq_PUP', 'Pc_Tc_PUP', 'Pc_Tf_PUP', 'SumMarca_PUP', 'SumCategoria_PUP'))
  
  
  #Ratios de PPU
  #PPU_O[PPU_O$PPU_Lb!=0,2]
  Pc_Lb_PPU<-(PPU_O[6]-apply(PPU_O[6],2,min))/(apply(PPU_O[6],2,max)-apply(PPU_O[6],2,min))
  Pc_Ek_PPU<-(PPU_O[6]-apply(PPU_O[7],2,min))/(apply(PPU_O[6],2,max)-apply(PPU_O[7],2,min))
  Pc_cz_PPU<-(PPU_O[8]-apply(PPU_O[8],2,min))/(apply(PPU_O[8],2,max)-apply(PPU_O[8],2,min))
  Pc_Cp_PPU<-(PPU_O[9]-apply(PPU_O[9],2,min))/(apply(PPU_O[9],2,max)-apply(PPU_O[9],2,min))
  Pc_Fg_PPU<-(PPU_O[10]-apply(PPU_O[10],2,min))/(apply(PPU_O[10],2,max)-apply(PPU_O[10],2,min))
  Pc_Mq_PPU<-(PPU_O[11]-apply(PPU_O[11],2,min))/(apply(PPU_O[11],2,max)-apply(PPU_O[11],2,min))
  Pc_Tc_PPU<-(PPU_O[12]-apply(PPU_O[12],2,min))/(apply(PPU_O[12],2,max)-apply(PPU_O[12],2,min))
  Pc_Tf_PPU<-(PPU_O[13]-apply(PPU_O[13],2,min))/(apply(PPU_O[13],2,max)-apply(PPU_O[13],2,min))
  
  DataPerfil_PPU<-cbind(PUP_O$CodPais,PUP_O$AnioCampanaExposicion,PUP_O$AnioCampanaProceso,PPU_O$PkEbelista,PPU_O$CodEbelista, Pc_Lb_PPU, Pc_Ek_PPU, Pc_cz_PPU,
                        Pc_Cp_PPU, Pc_Fg_PPU, Pc_Mq_PPU, Pc_Tc_PPU, Pc_Tf_PPU)
  
  names(DataPerfil_PPU)<-c("CodPais","AnioCampanaExposicion","AnioCampanaProceso","PKEbelista","CodEbelista","Pc_Lb_PPU","Pc_Ek_PPU","Pc_cz_PPU",
                           "Pc_Cp_PPU","Pc_Fg_PPU","Pc_Mq_PPU","Pc_Tc_PPU","Pc_Tf_PPU")
  
  rm(list=c('Pc_Lb_PPU', 'Pc_Ek_PPU', 'Pc_cz_PPU', 'Pc_Cp_PPU', 'Pc_Fg_PPU', 'Pc_Mq_PPU', 'Pc_Tc_PPU', 'Pc_Tf_PPU'))
  
  
  #### CONCATENACIÓN ####
  
  DataPerfil<-data.frame(cbind(DataPerfil_PUP,DataPerfil_PPU[6:13]))
  
  
  datapredi<-data.frame(DataPerfil[,c(6,8:12,18)])
  all.equal(fit[["cluster"]], centroides)
  Perfil1<-data.frame(clusters(datapredi, fit[["centers"]]))
  
  
  ouputpredic<-cbind(DataPerfil[1:13],PPU_O[6:13],Perfil1)
  
  names(ouputpredic)<-c("CodPais","AnioCampanaExposicion","AnioCampanaProceso","PkEbelista","CodEbelista","Pc_Lb_PUP","Pc_Ek_PUP","Pc_cz_PUP",
                        "Pc_Cp_PUP","Pc_Fg_PUP","Pc_Mq_PUP","Pc_Tc_PUP","Pc_Tf_PUP","Lb_PPU","Ek_PPU","cz_PPU",
                        "Cp_PPU","Fg_PPU","Mq_PPU","Tc_PPU","Tf_PPU","Perfil")
  


#canal<-odbcConnect("DMANALITICOCLOUD", uid="usrdm",pwd="dm$2Admin4")
#scriptdelete<-sprintf("delete from OutputFinal_TEMP_FINAL where CampMax = '%s'", Campania)
#xdata< sqlQuery(canal,scriptdelete)
#sqlSave(canal, OutputFinal, "OutputFinal_TEMP_FINAL", rownames=T, append=T)

#write.csv(OutputFinal,"//fscorp_app/FTPUsuarios/Carlos Reyes Ortega/OutputFinal.csv",quote = FALSE,row.names=FALSE,col.names=FALSE)
write.table(ouputpredic,"//fscorp_app/FTPUsuarios/Carlos Reyes Ortega/OutputFinal.csv",row.names=FALSE,col.names=FALSE,sep=",",quote = FALSE) 


Finish<-"Finish"
print(Finish)



