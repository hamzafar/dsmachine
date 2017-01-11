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
       iterEntity <- 0
       iterEntity1 <- 0
       
       dbSendQuery(mydb, "drop table if exists temprel;")
       dbSendQuery(mydb, "drop table if exists tempB;")
       
       
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
              
              tempEk <- Ek
              
              print (paste('Related Entitis of', Ek, ':'))
              print(unique(log[log$Related_Entity==El]$Related_Entity))
              
              El <- readline(prompt = "select the relational Entity:\t")
              # Ek <- 'vendor_ety'
              # El <- 'resource_ety'
              # entity_realations <- find_relation(Ek, El)
              
              #'
              #' perform depth here
              #'
              
              entity_depth(Ek = Ek, El = El)
              
              
              # k <- get_joinKey(Ek, El)
              
              # print(paste('Join By', k, sep = ':'))
              
              
              d <- as.numeric(readline(prompt = "value of depth:\t"))
              #               print(d)
              #               print(relatedVal[d+1])
              #               readline(prompt = "pulse depth entity")
              
              while(d >= 0){
                     # for (i in dbListFields(mydb,El))
                     
                     # relation <- readline(prompt = 'Forward (1) & backward (0):\t')
                     
                     
                     
                     if(iterEntity1 == 0){
                            iteEty <- relatedVal[d+1]
                     }
                     else{
                            iteEty <- 'tempB'
                     }
                            
                     
                     for (i in dbListFields(mydb,iteEty))
                     {
                            
                            print(c("fetaure Iterate",i))
                            
                            # readline("pulse check at d=0")
                            
                            if(iterEntity == 1) {
                                   Ek <- tempEk
                                   El <- 'tempB'
                                   
                                   print(Ek)
                                   print(El)
                                   readline('pulse entities changed')
                                   iterEntity <- 0
                                   
                                   }
                            
                            
                            if (i  %in% get_idies()){}
                            else{
                                   print(paste('all related features to ek', i, sep = ':'))
                                   print('')
                                   
                                   # d <- as.numeric(readline(prompt = "value of depth"))
                                   
#                                    if (d == 0){
#                                           h <- Ek
#                                           z <- El
#                                           
#                                           Ek <- Ek
#                                           El <- El
#                                           
#                                           
#                                    }
                                   
                                   if (d == 0){
                                          
                                          Ek <- tempEk
                                          
                                          a <- fetch(dbSendQuery(mydb, 'show tables like "temprel"'), n=-1)
                                          
                                          # print(a)
                                          
                                          if(dim(a)[1] == 0){  
                                                 El <- El
                                          }
                                          else{
                                                 
                                                 Ek <- a[[1]]
                                                 El <- El
                                          }
                                          
                                          h <- Ek
                                          z <- El
                                          
#                                           print(Ek)
#                                           print(El)
                                          
                                          readline(prompt = "depth zero")
                                          
                                   }
                                   
                                   
                                   else{
                                          
                                          nam <- 'temprel'
                                          if (is.element(nam, dbListTables(mydb))){
                                                 h <- nam
                                                 z <- relatedVal[d+1]       
                                          }
                                          else{
                                                 h <- relatedVal[d]
                                                 if(iterEntity1 == 1) {
                                                        z <- El
                                                        iterEntity <- 0
                                                 }
                                          
                                                 else{
                                                 z <- relatedVal[d+1]       
                                                 }
                                          }
                                          
                                          Ek <- h
                                          El <- z
                                   }
                                   
                                   k <- get_joinKey(Ek, El)
                                     
                                   
                                   # relation <- find_relation(h,z)
                                   relation <- find_relation(Ek,El)
                                   print(paste("relation between", Ek, El, ": ", relation))
                                   rm(h);rm(z)
                                   # readline(prompt = 'pulse relation obtaine')
                                   
                                   #                                    print(k)
                                   #                                    readline(prompt = 'pulse foreign key')
                                   
                                   
                                   print(c("EK: ", Ek, "El: ", El))
                                   
                                   relatedFeature <- i
                                   
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
                                          
                                          # Fun <- readline('sum, avg, std, count: ? \t')
                                          Fun <- 'count'
                                          
                                          qry <- rfeat(Ek = Ek, El = El, Fun = Fun,
                                                       relatedFeature = relatedFeature,
                                                       k = k)
                                   }
                                   
                                   
                                   
                                   #                                    print(qry)
                                   #                                    
                                   #                                    readline(prompt = 'pulse check query')
                                   
                                   
                                   #'
                                   #'insert into tempData table if d> 0
                                   #'
                                   
                                   
                                   
#                                    print(qry)
#                                    readline(prompt = 'query check')
                                   # if(d != 0)temp_depth_db(qry)
                                   temp_depth_db(qry)
                            }  
                     }
                     
                     #'
                     #'insert into feature when d == 0
                     #'
                     
                     
                     
                     
                     #'
                     #'delete tempB if exists
                     #'create table tempB as temprel, 
                     #'delete temprel
                     #'
                     
                     
                     readline(prompt = "pulse temporary tables star")
                     
                     dbSendQuery(mydb, "drop table if exists tempB;")
                     
                     dbSendQuery(mydb, "create table tempB as select * from temprel;")
                     
                     dbSendQuery(mydb, "drop table if exists temprel;")
                     
                     readline(prompt = "pulse temporary tables end")
                     
                     
                     
                     readline('pulse All Features Created')
                     d <- d-1
                     iterEntity <- 1
                     iterEntity1 <- 1
              }
              
              
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




dfeat1<- function(Ek, El, relatedFeature, k, d=0){
       readline('pulse dfeat function')
       
       dfeatQry <- c()
       if(d==0){
              dfeatQry <- paste('select ', Ek ,'.*, ', El, '.', relatedFeature,
                                ' from ', Ek, ' left join ', El, ' on ', 
                                Ek, '.', k, '=', El, '.', k, ';' , sep ='')
       }
       if(d==1){
              
       }
       
       dfeatQry
}

rfeat1 <- function(Ek, Fun, El, relatedFeature, k, d = 0){
       
       readline(prompt = "pulse rfeat function")
       
       rfeatQry <- c()
       
       if(d==0){
              rfeatQry <- paste('select ', Ek ,'.*, ', Fun, '(', El, '.', relatedFeature, ') as ',
                                Fun, '_', relatedFeature, ' from ', Ek, ' left join ', El, ' on ', 
                                Ek, '.', k, '=', El, '.', k, ' group by ',  Ek, '.', k, ';' , sep ='')
       }
       
       if(d==1){
              a <- get_joinKey(Ek, relatedVal[d])
              b <- get_joinKey(relatedVal[d], relatedVal[d+1])
              
              col_name <- paste0(Fun, '_', relatedFeature)
              
              
              rfeatQry <- paste0("select v.*, x.", col_name," from ", Ek, 
                                 " v left join ( select r.*, count(p.", relatedFeature, ") as ", col_name,
                                 " from ", relatedVal[d], " r left join ", relatedVal[d+1], " p on r.", 
                                 b," = p.", b, " )as x on v.", a, " = x.", a, ";")
              
              
              #               rfeatQry <- paste0("select v.*, x.a from ", Ek, 
              #                                  " v left join ( select r.*, count(p.", relatedFeature, ") as a from ",
              #                                  relatedVal[d], " r left join ", relatedVal[d+1], " p on r.", b," = p.", b,
              #                                  " )as x on v.", a, " = x.", a, ";")
       }
       
       print(rfeatQry)
       
       readline(prompt = 'pulse refeatQry')
       rfeatQry
       
}

dfeat <- function(Ek, El, relatedFeature, k){
       paste('select ', Ek ,'.*, ', El, '.', relatedFeature, ' from ', Ek, 
             ' left join ', El, ' on ', 
             Ek, '.', k, '=', El, '.', k, ';' , sep ='')
       
}

rfeat <- function(Ek, Fun, El, relatedFeature, k){
       
       print(c("pulse rfeat:  ", Ek, El))
       
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
              dbSendQuery(mydb,'Drop table if exists temp;')
              
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
              dbSendQuery(mydb,'Drop table if exists temp;')
              
              print('feature added')
       }       
       else{
              createQry <- paste0('create table ', nam, ' as ', qry)
              dbSendQuery(mydb,createQry)
              print('feature created')
              
       }
       
}

temp_depth_db <- function(qry){
       
       nam <- 'temprel'
       
       if (is.element(nam, dbListTables(mydb))){
              dbSendQuery(mydb,'Drop table if exists temp;')
              
              createQry <- paste0('create table temp as ', qry)
              
#               print(createQry)
#               readline(prompt = "pulse temp update")
              
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
              dbSendQuery(mydb,'Drop table if exists temp;')
              
              print('feature added')
       }       
       
       else{
              creteTempQry <- paste0('create table temprel as ', qry)
              dbSendQuery(mydb, creteTempQry)
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
                                          
                                          #                                           print(c(j,k))
                                          #                                           print('')
                                          
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
       
#        print(Ek)
#        print(El)
       
       for(i in dbListFields(mydb, Ek)){
              for (j in dbListFields(mydb, El)){
                     if(i==j){
                            # print(c(i,j))
                             # print(i)     
                            
                            if (i %in% get_id(Ek) || i %in% get_id(El)){
                                   joinBy <- i
                                   break     
                            }
                            
                            idies <- get_idies()
                            if(i %in% idies){
                                   joinBy <- i
                                   break     
                            }
                     }
              }
       }
       # print (joinBy)
       
       
       
       
       joinBy
}

get_id<- function(ety){
       idQry <- paste0("SHOW INDEX FROM ", dbname, ".", ety,
                       " WHERE `Key_name` = 'PRIMARY';")
       
       id <- fetch(dbSendQuery(mydb,idQry), n=-1)
       pk <- id$Column_name
       pk
}

get_joinKey1 <- function(Ek,El){
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


find_relation1 <-function(){
       
       entity_realations <- data.frame(Ek= character(), El = character(),
                                       rel = numeric(), stringsAsFactors = F)
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='vendor_ety', El ='resource_ety', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='resource_ety', El ='vendor_ety', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='resource_ety', El ='project_ety', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project_ety', El ='resource_ety', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project_ety', El ='school_ety', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project_ety', El ='teacher_ety', rel= 1))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='school_ety', El ='project_ety', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='teacher_ety', El ='project_ety', rel= 0))
       
       entity_realations       
}

find_relation <- function (Ek, El){
       
       val <- c()
       
#        print(Ek)
#        print(El)
       
       joinBy <- get_joinKey(Ek,El)
       
       # print(joinBy)
       
       relationQry <- paste("select", joinBy, ",count(*) c from", 
                            Ek, "group by", joinBy, "having c > 1;")
       
#        print(relationQry)
#        readline(prompt = "+++++pulse relationalQry++++")
       
       result <- fetch(dbSendQuery(mydb,relationQry), n=-1)
       
       
       if(dim(result)[1] == 0) {
              val <- 0
       }
       else{
              val <- 1
       }
       
       val
       # SELECT vendorid, COUNT(*) c FROM vendor_ety GROUP BY vendorid HAVING c > 1;
       
       
}





test <- function(Ek, El, relatedFeature, d=1){
       
       
       if(d == 1){
              
              a <- get_joinKey(Ek, relatedVal[d])
              b <- get_joinKey(relatedVal[d], relatedVal[d+1])
              
              
              paste0("select v.*, x.a from ", Ek, 
                     " v left join ( select r.*, count(p.", relatedFeature, ") as a from ",
                     relatedVal[d], " r left join ", relatedVal[d+1], " p on r.", b," = p.", b,
                     " )as x on v.", a, " = x.", a, " limit 1;")
       }
}



# create temporary table test as select * from vendor_ety;
# drop table test if exists;

