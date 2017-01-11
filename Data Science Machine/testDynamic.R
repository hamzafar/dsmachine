
setwd("G:/predictify/KDD-2014/entities")
require(dplyr)

vendor_entity <- read.csv('vendor_entity.csv', sep = ',', na.strings = "NA", 
                          stringsAsFactors = F)
resource_entity <- read.csv('resource_entity.csv', sep = ',', na.strings = "NA",
                            stringsAsFactors = F)

vendor_entity <- vendor_entity[1:50000,]
resource_entity <- resource_entity[1:50000,]


getList('item_unit_price', resource_entity)


left_jo


getList <- function(key, data){
       value = data[[key]]       
       
       mylist = list()
       mylist[[key]] = value
       
       mylist
}

