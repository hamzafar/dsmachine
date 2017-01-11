require(dplyr)

Ek <- 'teacher'
all <- c('vendor', 'resource', 'project', 'school',
         'donor', 'donation', 'outcome', 'essay', 'teacher')
total <- 0

feature_demo <- function(Ek,all, Ev = NULL){
       
       temp_all <- all[-grep(Ek, all)]
       
       # readline(prompt = "pulse function starting")
       
       # Ev <- paste(Ev, Ek)
       
       
       Ev[length(Ev)+1] <- Ek
#        print(paste('Ev :', Ev))
       cat('\n')
       
       Eb <- backward_enties(Ek, temp_all)
       Ef <- forward_entities(Ek, temp_all)
       
       # print(paste('Ek :', Ek))
#        
#        print(paste('Eb :', Eb))
       for (i in Eb){
              
              # print (paste('backward: ', i))
              # readline(prompt = 'pulse backward loop')
              
              feature_demo(Ek = i, all = all, Ev = Ev)
              
              rfeat(i)
              cat('\n')
              
       }
       
       # print(paste('Ef :', Ef))
       for(i in Ef){
              
              # print(paste('grep the value: ', grep(i, Ev, value = T)))
              
              if(i %in% Ev){
                     # readline(prompt = "pulse i am forward")
              }
                     
              else
              {
                     # print (paste('forward: ', i))
                     # readline(prompt = 'pulse forward loop')
                     
                     feature_demo(Ek = i, all = all, Ev = Ev)
                     
                     dfeat(i)
                     cat('\n')}
       }
       
}

find_relation1 <-function(){
       
       entity_realations <- data.frame(Ek= character(), El = character(),
                                       rel = numeric(), stringsAsFactors = F)
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='vendor', El ='resource', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='resource', El ='vendor', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='resource', El ='project', rel= 1))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='resource', rel= 0))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='school', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='teacher', rel= 1))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='school', El ='project', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='teacher', El ='project', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='donor', El ='donation', rel= 0))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='donation', El ='donor', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='donation', El ='project', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='donation', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='outcome', El ='project', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='outcome', rel= 0))
       
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='essay', El ='project', rel= 1))
       entity_realations <- rbind(entity_realations, 
                                  data.frame(Ek ='project', El ='essay', rel= 0))
       
       
       
       
       
       
       entity_realations       
}

backward_enties <- function(target_entity, all){
       
       backward_entities <- filter(find_relation1(), rel == 0,
                                   Ek == target_entity)$El
       
       as.vector(backward_entities)
       
} 
forward_entities <- function (target_entity, all){
       
       forward_entities <- filter(find_relation1(), rel == 1,
                                  Ek == target_entity)$El
       
       as.vector(forward_entities)
}


rfeat <- function(targetEty){
       print(targetEty)
       qty <- filter(feature_no(), Ek == targetEty)$no
       total <<- total +qty
       
       print(qty)
}

dfeat <- function(targetEty){
       print(targetEty)
       qty <- filter(feature_no(), Ek == targetEty)$no
       total <<- total +qty
       
       print(qty)
}


feature_no1 <- function(){
       
       entity_features <- data.frame(Ek= character(), no = numeric(),
                                     stringsAsFactors = F)
       
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='vendor', no = 13))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='resource', no = 20))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='school', no = 17))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='project', no = 12))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='teacher', no = 8))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='donation', no = 21))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='donor', no = 4))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='outcome', no = 13))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='essay', no = 9))
       
       entity_features
}

feature_no <- function(){
       
       entity_features <- data.frame(Ek= character(), no = numeric(),
                                     stringsAsFactors = F)
       
       entity_features <- rbind(entity_features, 
                                  data.frame(Ek ='vendor', no = 2))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='resource', no = 8))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='school', no = 17))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='project', no = 12))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='teacher', no = 8))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='donation', no = 17))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='donor', no = 5))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='outcome', no = 12))
       entity_features <- rbind(entity_features, 
                                data.frame(Ek ='essay', no = 5))
       
       entity_features
}


filter(feature_no(), Ek == 'resource')$no


for (i in all){
       cat('entity: \t', i, '\n') 
       
}