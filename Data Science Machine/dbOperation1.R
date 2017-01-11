# 
# require(RMySQL)
# library(plyr)
# 
# user <- 'root'
# pass <- c()
# dbname <- 'data_science_machine'
# host <- '127.0.0.1'
# 
# mydb = dbConnect(MySQL(), user=user, password='', 
#                  dbname=dbname, host=host)
# 

make_features <- function(Ek = '', El = '', Fun ='count'){
       qry <- c()
       entityLoop <- 1
       log <- data.frame( "Entity" = character(), "Related_Entity" = character(),
                          "Related_Feature" = character(), "Relation" = integer(),
                          "Fun" = character(), "Query" = character(),
                          stringsAsFactors=FALSE) 
       
       #'
       #'iterate to select multiple entities
       #'
       
       while(entityLoop == 1){
              featureLoop <- 1;
              
              print(dbListTables(mydb))
              
              # print("Used Features: ")
              
              
              Ek <- readline(prompt = "select the feature Entity:\t")
              
              print (paste('Related Entitis of', Ek, ':'))
              print(unique(log[log$Related_Entity==El]$Related_Entity))
              
              El <- readline(prompt = "select the relational Entity:\t")
              # Ek <- 'vendor_ety'
              # El <- 'resource_ety'
              
              relation <- readline(prompt = 'Forward (1) & backward (0):\t')
              # relation <- 0
              
              #'
              #' iterate to select multiple features
              #'
              
              while(featureLoop == 1){
                     
                     print(dbListFields(mydb, El))
                     print("Related Entities Used Before:")
                     print( log[log$Entity==Ek & log$Related_Entity==El]$Related_Feature)
                     
                     relatedFeature <- readline('select feature:\t')
                     
                     k <- readline(prompt = 'join key: \t')
                     
                     # k <- 'vendorid'
                     
                     if( relation == '1'){
                            
                            Fun <- ''
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
                            
                            
                            qry <- paste('select ', Ek ,'.*, ', Fun, '(', El, '.', relatedFeature, ') as ',
                                         Fun, '_', relatedFeature, ' from ', Ek, ' left join ', El, ' on ', 
                                         Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
                            
                            
                            #                             qry <- paste('select ', Ek ,'.*, ', Fun, '(', El, '.', relatedFeature, ') from ', Ek, 
                            #                                          ' left join ', El, ' on ', 
                            #                                          Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
                     }
                     
                     insert_feature_db(qry, Ek)
                     
                     featureLoop <- readline(prompt = 'More Features (yes=1, No=0):\t')
                     
                     log[nrow(log)+1,] <- c(Ek, El, relatedFeature, 
                                            relation, Fun, qry)
                     #print(log)
              }
              entityLoop <- readline(prompt = 'More Entities (yes=1, No=0):\t')
       }
}

insert_feature_db <- function(qry, Ek){
       
       nam <- paste(Ek, 'features', sep = '_')
       
       if (is.element(nam, dbListTables(mydb))){
              print('exciting feature')
              
              createQry <- paste0('create table temp as ', qry)
              # print(createQry)
              dbSendQuery(mydb,createQry)
              
              # readline()
              k <- dbListFields(mydb, nam)[1]
              col <- dbListFields(mydb, 'temp')
              col <-col[length(col)]
              
              joinQry <- paste('select ', nam ,'.*, ', 'temp.', col, ' from ', nam, 
                               ' left join temp on ', 
                               nam, '.', k, '=', 'temp.', k, ';' , sep ='')
              print(joinQry)
              
              readline()
              rs <- dbSendQuery(mydb, joinQry)
              temp <- fetch(rs,n=-1)
              
              print(colnames(temp))
              readline()
              
              dbWriteTable(mydb,name = nam, value = temp,
                           overwrite = T, row.names = F)
              
              rm(temp)
              dbSendQuery(mydb,'Drop table temp')
              
              print('feature added')
       }       
       
       #        if (is.element(nam, dbListTables(mydb))){
       #               print('exciting feature')
       #               
       #               tblNam <- nam; temp <- nam
       #               
       #               rs <- dbSendQuery(mydb, qry)
       #               
       #               print(qry)
       #               
       #               tblNam <- fetch(rs,n=-1)
       #               
       #               print('pulse1')
       #               
       #               col <- colnames(tblNam)[length(colnames(tblNam))]
       #               
       #               
       #               temp <- dbReadTable(mydb,nam)
       #               
       #               print(c(dim(temp), dim(tblNam)))
       #               print(colnames(temp))
       #               print(colnames(tblNam))
       #               
       #               
       #               temp[[col]] <- tblNam[[col]]
       #               
       #               print('pulse')
       #               
       #               # print(head(temp))
       #               
       #               dbWriteTable(mydb,name = nam, value = temp, overwrite = T, 
       #                            row.names =F)
       #               
       #               rm(tblNam)
       #               rm(temp)
       #               
       #               print('feature added')
       #        }
       else{
              print('new')
              
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

handle_missing_values <- function(feature){
       
}