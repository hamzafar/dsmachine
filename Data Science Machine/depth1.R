require(dplyr)
require(RMySQL)

user <- 'root'
pass <- c()
dbname <- 'data_science_machine'
host <- '127.0.0.1'
# 
# mydb = dbConnect(MySQL(), user=user, password='', 
#                  dbname=dbname, host=host)
# 

relatedVal <- c()

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
              
              
              entity_depth(Ek = Ek, El=El)
              k <- get_joinKey(Ek, El)
       
              for (i in dbListFields(mydb,El))
              {
                     if (i  %in% get_idies()){}
                     else{
                            print('all related features to ek')
                            print(i)   
                            
                            relatedFeature <- i
                            
                                   
                            if (relation == '0'){
                                   
                                   print(paste(
                                          'Type of the feautre:', get_feature_type(
                                                 El, relatedFeature
                                          )
                                   ))
                                   
                                   # Fun <- readline('sum, avg, std, count: ? \t')
                                   Fun <- 'count'
                                   
                                   qry <- rfeat(Ek = Ek, El = El, Fun = Fun,
                                                relatedFeature = relatedFeature, k = k)
                            }
                            
                     insert_feature_db(qry, Ek)}  
                     }
                     
              
              readline('pulse All Features Created')
              
              print(relatedVal)
              
              readline("pulse searched entities")
              
                            
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
                            qry <- dfeat(Ek = Ek, El = El,
                                         relatedFeature = relatedFeature, k = k)
                     }
                     if (relation == '0'){
                            
                            print(paste(
                                   'Type of the feautre:', get_feature_type(
                                          El, relatedFeature
                                   )
                            ))
                            
                            Fun <- readline('sum, avg, std, count: ? \t')
                            # log$Fun <- Fun
                            
                            qry <- rfeat(Ek = Ek, El = El, Fun = Fun,
                                         relatedFeature = relatedFeature, k = k)                            
                            
                     }
                     
                     insert_feature_db(qry, Ek)
                     
                     featureLoop <- readline(prompt = 'More Features (yes=1, No=0):\t')
                     
                     log[nrow(log)+1,] <- c(Ek, El, relatedFeature, 
                                            relation, Fun, qry)
                     #print(log)
              }
              efeatLoop <- readline(prompt = 'EFEAT (yes=1, No=0):\t')
              if(efeatLoop ==1){
                     nam <- paste(Ek, 'features', sep = '_')
                     for (i in dbListFields(mydb, nam)){
                            efeat(nam, i)
                     }
              }
              entityLoop <- readline(prompt = 'More Entities (yes=1, No=0):\t')
              
              rm(relatedVal)
       }
}

dfeat <- function(Ek, El, relatedFeature, k){
       paste('select ', Ek ,'.*, ', El, '.', relatedFeature, ' from ', Ek, 
             ' left join ', El, ' on ', 
             Ek, '.', k, '=', El, '.', k, ';' , sep ='')
       
}

rfeat <- function(Ek, Fun, El, relatedFeature, k){
       paste('select ', Ek ,'.*, ', Fun, '(', El, '.', relatedFeature, ') as ',
             Fun, '_', relatedFeature, ' from ', Ek, ' left join ', El, ' on ', 
             Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
}

efeat <-function(Ek,relatedFeature){
       
       #        Ek <- 'test'
       #        relatedFeature <- 'count_resource_type'
       
       countQry <- paste0('select ', relatedFeature, ', count(', relatedFeature,
                          ') as a from ', Ek, ' group by ', relatedFeature, ';')
       
       
       rs <- dbSendQuery(mydb, countQry)
       count <- fetch(rs,n=-1)
       
       
       repVal <- filter(count, a==max(a))[[1]]
       
       #        print(length(repVal))
       #        print(repVal)
       #        print(repVal[1])
       #        
       #        print(class(repVal[1]))
       #        
       #        print(repVal[1]== 'numeric')
       #        
       #        if(repVal[1]== 'numeric')
       #        {
       #               updQry <- paste0('update ', Ek, ' set ', relatedFeature, ' = ', repVal[1],
       #                                ' where ', relatedFeature, ' is null ;')
       #        }
       
       
       updQry <- paste0("update ", Ek, " set ", relatedFeature, " = '", repVal[1],
                        "' where ", relatedFeature, " is null ;")
       
       #        print(updQry)
       # readline()
       dbSendQuery(mydb, updQry)
       print(paste(Ek,'feature', relatedFeature, 'Updated'))
       
}

insert_feature_db <- function(qry, Ek){
       
       nam <- paste(Ek, 'features', sep = '_')
       
       if (is.element(nam, dbListTables(mydb))){
              createQry <- paste0('create table temp as ', qry)
              dbSendQuery(mydb,createQry)
              
              k <- dbListFields(mydb, nam)[1]
              col <- dbListFields(mydb, 'temp')
              col <-col[length(col)]
              
              joinQry <- paste('select ', nam ,'.*, ', 'temp.', col, ' from ', nam, 
                               ' left join temp on ', 
                               nam, '.', k, '=', 'temp.', k, ';' , sep ='')
              
              rs <- dbSendQuery(mydb, joinQry)
              temp <- fetch(rs,n=-1)
              
              dbWriteTable(mydb,name = nam, value = temp,
                           overwrite = T, row.names = F)
              
              rm(temp)
              dbSendQuery(mydb,'Drop table temp')
              
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

entity_depth <- function(Ek, El, depth = El){
       
       #'
       #'first of call we will fetch all the table names
       #'
       
       dbTables <- c('vendor_ety', 'resource_ety', 'project_ety',
                     'school_ety', 'teacher_ety')
       
       for (i in dbTables){
              if(Ek == i || El == i)
              {
                     #' 
                     #' this will skip when both tables will be matched
              }
              
              else{
                     for (j in dbListFields(mydb,El))
                            for (k in dbListFields(mydb,i)){
                                   if (j == k){
                                          
                                          
                                          print(c(j,k))
                                          
                                          relatedVal <<- append(depth,i)
                                          depth <- append(depth, i)
                                          
                                          #'
                                          #'here it will call itself to check 
                                          #'related etitis
                                          #'
                                          
                                          entity_depth(Ek = El, El =  i, depth = depth)
                                          
                                   }
                            }
              }
       }
       
       
}

get_idies <- function(){
       
       pk <- c()
       
       dbTables <- c('vendor_ety', 'resource_ety', 'project_ety',
                     'school_ety', 'teacher_ety')
       
       for (i in dbTables){
              
              idQry <- paste0("SHOW INDEX FROM ", dbname, ".", i,
                              " WHERE `Key_name` = 'PRIMARY';")
              
              id <- fetch(dbSendQuery(mydb,idQry), n=-1)
              pk <- c(pk, id$Column_name)
       }
       pk
}

get_joinKey <- function(Ek,El){
#        Ek <- 'vendor_ety'
#        El <- 'resource_ety'
       joinBy <- c()
       
       for(i in dbListFields(mydb, Ek)){
              for (j in dbListFields(mydb, El)){
                     if(i==j){
                            # print(c(i,j))
                            joinBy <- i
                     }
              }
       }
       joinBy
}



