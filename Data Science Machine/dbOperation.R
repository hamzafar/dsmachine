# 
# require(RMySQL)
# 
# 
# user <- 'root'
# pass <- c()
# dbname <- 'data_science_machine'
# host <- '127.0.0.1'
# 
# mydb = dbConnect(MySQL(), user=user, password=pass, 
#                  dbname=dbname, host=host)
# 

make_features <- function(Ek = '', El = '', Fun ='count'){
       qry <- c(); log <- data.frame()
       entityLoop <- 1
       
       
       
       #'
       #'iterate to select multiple entities
       #'
       
       while(entityLoop == 1){
              featureLoop <- 1;
              
              print(dbListTables(mydb))
              
              # print("Used Features: ")
              
              
              Ek <- readline(prompt = "select the feature Entity:\t")
              El <- readline(prompt = "select the relational Entity:\t")
              # log$entity <- Ek
              # Ek <- 'vendor_ety'
              # El <- 'resource_ety'
              
              relation <- readline(prompt = 'Forward (1) & backward (0):\t')
              # log$relation <- relation
              # relation <- 0
              
              #'
              #' iterate to select multiple features
              #'
              
              while(featureLoop == 1){
                     
                     print(dbListFields(mydb, El))
                     print("Related Entities Used Before:")
                     
                     relatedFeature <- readline('select feature:\t')
                     # log$relatedFeature <- relatedFeature
                     
                     k <- readline(prompt = 'join key: \t')
                     # logKey <- k
                     
                     # k <- 'vendorid'
                     
                     if( relation == '1'){
                            
                            # log$Fun <- ''
                            qry <- paste('select ', Ek ,'.*, ', El, '.', relatedFeature, ' from ', Ek, 
                                         ' left join ', El, ' on ', 
                                         Ek, '.', k, '=', El, '.', k, ';' , sep ='')
                            
                     }
                     if (relation == '0'){
                            
                            print(paste(
                                   'Type of the feautre:', get_feature_type(
                                          El, relatedFeature
                                   )
                            ))
                            
                            Fun <- readline('sum, avg, std, count: ? \t')
                            # log$Fun <- Fun
                            
                            qry <- paste('select ', Ek ,'.*, ', Fun, '(', El, '.', relatedFeature, ') from ', Ek, 
                                         ' left join ', El, ' on ', 
                                         Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
                            
                            #               paste('select ', Ek ,'.*, ', El, '.', relatedFeature, ' from ', Ek, 
                            #                     ' left join ', El, ' on ', 
                            #                     Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
                     }
                     
                     #               rs <- dbSendQuery(mydb, qry)
                     #               
                     #                      vendor <- fetch(rs,n=-1)
                     #                      vendor
                     #                      
                     # print(qry)
                     
                     # insert_feature_db(qry, Ek)
                     
                     featureLoop <- readline(prompt = 'More Features (yes=1, No=0):\t')
              }
              entityLoop <- readline(prompt = 'More Entities (yes=1, No=0):\t')
       }
}

insert_feature_db <- function(qry, Ek){
       
       nam <- paste(Ek, 'features', sep = '_')
       
       
       if (is.element(nam, dbListTables(mydb))){
              tblNam <- nam; temp <- nam
              
              rs <- dbSendQuery(mydb, qry)
              
              tblNam <- fetch(rs,n=-1)
              
              col <- colnames(tblNam)[length(colnames(tblNam))]
              
              temp <- dbReadTable(mydb,nam)
              temp[[col]] <- tblNam[[col]]
              
              # print(head(temp))
              
              dbWriteTable(mydb,name = nam, value = temp, overwrite = T, 
                           row.names =F)
              
              rm(tblNam)
              rm(temp)
              
              print('feature added')
       }
       else{
              createQry <- paste0('create table ', nam, ' as ', qry)
              dbSendQuery(mydb,createQry)
              print('feature created')
              
       }
       
}

get_feature_type <- function(entity, feature=''){
       feature_info <- dbSendQuery(mydb, paste0('DESCRIBE ', entity, ';'))
       db_info <- fetch(feature_info, n=-1)
       # db_info
       
       db_info[db_info$Field==feature,]$Type
       
}