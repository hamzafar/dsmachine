setwd("G:/predictify/Data Science Machine")

require(data.table)
require(dplyr)

customer <- read.csv("customer.csv", sep = ',')
product <- read.csv('product.csv', sep = ',')
order <- read.csv('order.csv', sep = ',')
orderProduct <- read.csv('orderproduct.csv', sep = ',')


head(make_features(product,'cost',1,2))


make_features <- function(baseEntity, baseCol, fwdEty, bckwdEty, FUN = sum){
       
       Ev <-  baseEntity
       
       #' this is join for combining the base entity and the forward entity
       #' we just added the two by key values, where we found the direct relationship
       #' 
       #' DFEAT
       
       for (i in range(1: fwdEty)){
              Ef <- forward_entity()
              Ev <- left_join(Ev, Ef)
       }
       
       #' information about base column, so to apply rfeat accordingly
       #' here we have base column 'cost'
       #' 
       #' RFEAT
       
       for (i in range(1: bckwdEty)){
              
              Eb <- backward_entity(i)
              
              getList <- function(key, data){
                     value = data[[key]]       
                     
                     mylist = list()
                     mylist[[key]] = value
                     
                     mylist
              }
              
              bylist <- getList(intersect(colnames(Eb), colnames(Ev)), Ev)
              colList <- getList(baseCol, Ev)
              
              Ev <- (left_join(
                     aggregate(colList, by = bylist, FUN), Eb
              ))
              
              print(head(Ev))       
       }
       
       
       Ev
}

forward_entity <- function(){
       orderProduct
}
backward_entity <- function(bi){
       d <- data.frame()
       if (bi==1){
              d <- order
       }
       if (bi == 2){
              d <- customer
       }
       d
}
