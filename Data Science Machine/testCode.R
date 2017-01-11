


qry <- 'select v.vendorid, v.vendor_name, r.item_unit_price
from vendor_ety v left join resource_ety r on 
v.vendorid = r.vendorid;'


qry <- 'select v.vendorid, v.vendor_name, sum(r.item_unit_price) as total
from vendor_ety v left join resource_ety r on 
v.vendorid = r.vendorid group by v.vendorid;'


rs <- dbSendQuery(mydb, qry)

vendor <- fetch(rs,n=-1)


head(vendor)


dbClearResult(dbListResults(mydb)[[1]])










entity <- read.csv("donor_entity.csv", sep = ',',
                   na.strings = "NA", stringsAsFactors = F)

dbWriteTable(mydb, name = 'donation', value = donation_entity)



#' CREATE TABLE table SELECT * FROM table2 GROUP BY (id); 
#' DELETE FROM table_name WHERE some_column IS NULL;
#' ALTER TABLE table_name ADD PRIMARY KEY(id);
#' 
#' 
#' ALTER TABLE tbl ADD id INT PRIMARY KEY AUTO_INCREMENT;
#' alter table authors ADD UNIQUE(name_first(20));
#' 
#' SELECT 
# T1.c1, T1.c2, T2.c1, T2.c2
# FROM
# T1
# LEFT JOIN
# T2 ON T1.c1 = T2.c1;




rm(entity)
















setwd("G:/predictify/Data Science Machine")

require(data.table)
require(dplyr)
bi <- 0

customer <- read.csv("customer.csv", sep = ',')
product <- read.csv('product.csv', sep = ',')
order <- read.csv('order.csv', sep = ',')
orderProduct <- read.csv('orderproduct.csv', sep = ',')

head(make_features(product,'cost',1,2))

make_features <- function(baseEntity, baseCol, fwdEty, bckwdEty){
       
       Ev <-  baseEntity
       
       #' for all backward and forward feature, iterate below
       #' LOOP
       
       
       
       
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
              
              # print(Eb)
              print(col_id)
              print(colnames(Ev))
              
              
              getList <- function(key, data){
                     value = data[[key]]       
                     
                     mylist = list()
                     mylist[[key]] = value
                     
                     mylist
              }
              
              bylist <- getList(intersect(colnames(Eb), colnames(Ev)), Ev)
              colList <- getList(baseCol, Ev)
              
              Ev <- (left_join(
                     aggregate(colList,by = bylist, FUN = sum), Eb
              ))
              
              print(head(Ev))
       }
       #' END
       
       Ev
}

head ( left_join(aggregate(list(baseCol = Ev[[baseCol]]),
                          by = list(order_id = Ev$order_id), FUN = sum),
                Eb
))



key = "order_id"
value = Ev$order_id

bylist = list()
bylist[[key]] = value



(left_join(
       aggregate(list(baseCol = Ev[[baseCol]]),
               by = bylist, FUN = sum), Eb
))



head(aggregate(list(baseCol = Ev[[baseCol]]),
               by = bylist, FUN = sum))



forward_entity <- function(){
       orderProduct
}
backward_entity <- function(bi){
       d <- data.frame()
       if (bi==1){
              d <- order
              print(head(d))
       }
       if (bi == 2){
              d <- customer
              print(head(d))
       }
       d
}


make_features(product,'cost')



make_features1 <- function(){
       Ev <- data.frame()
       # Ev <- cbind(Ev, baseEntity)
       Ev <-  product
       
       #' for all backward and forward feature, iterate below
       #' LOOP
       
       Ef <- forward_entity()
       Eb <- backward_entity(1)
       
       #' this is join for combining the base entity and the forward entity
       #' we just added the two by key values, where we found the direct relationship
       
       Ev <- left_join(Ev, Ef)
       
       #' information about base column, so to apply rfeat accordingly
       #' here we have base column 'cost'
       
       
       
       Ev <- left_join(aggregate(list(cost = Ev$cost),
                                 by = list(order_id = Ev$order_id), FUN = sum),
                       Eb
       )
       
       #' END
       
       Ev
} 

"select vendor_ety.*, count(resource_ety.item_number) as count_item_number from 
vendor_ety left join resource_ety on vendor_ety.vendorid=resource_ety.vendorid group by vendor_ety.vendorid;"


t1 <- 'vendor_ety'
t2 <- 'resource_ety'
t3 <- 'project_ety'
a <- 'vendorid'
b <- 'projectid'

SELEC * FROM t1 LEFT JOIN (t2 LEFT JOIN t3 ON t2.b=t3.b) ON t1.a=t2.a;


nesJoin <- paste0('select * from ', t1, ' left join ( ', t2, ' left join ', t3, ' on ', t2, '.', b, 
                  ' = ', t3, '.', b, ' ) on ', t1, '.', a, ' = ', t2, '.', a, ';')


re <- dbSendQuery(mydb, nesJoin)

result <- fetch(re, n=-1)



select * from vendor_ety left join (
       resource_ety left join project_ety on resource_ety.projectid = project_ety.projectid 
       )
on vendor_ety.vendorid = resource_ety.vendorid;

