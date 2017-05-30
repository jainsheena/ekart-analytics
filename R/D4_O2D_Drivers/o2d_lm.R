library(hts)
library(dplyr)
library(glmnet)

sample_data_frame <- sqlQuery(hive_connect, "select
                              count(A.fulfill_item_unit_id) as units,
                              A.approve_delivery_o2d_cdays_dd as O2D, 
                              A.fulfill_item_unit_deliver_actual_date_key as deliver_date_key,
                              B.week_num_in_year as delivered_week,
                              Case when shipment.shipment_carrier = 'FSD' then 'FSD' else '3PL' end as shipment_carrier,
                              A.order_item_service_profile as service_profile,
                              A.new_lzn as lzn,
                              A.fulfill_item_unit_dispatch_service_tier as dispatch_service_tier,
                              shipment.service_tier as shipment_service_tier,
                              A.fulfill_item_unit_lpe_tier as lpe_tier,
                              A.order_item_approve_date_key as approve_date_key,
                              order_item_reserve_actual_date_key as reservation_date_key,
                              order_item_reserve_actual_time_key as reservation_time_key,
                              shipment.shipment_first_out_for_pickup_date_key as out_for_pickup_date_key,
                              C.day_of_week as pickup_day,
                              A.fulfill_item_unit_dispatch_actual_date_key as dispatch_date_key,
                              A.fulfill_item_unit_dispatch_actual_time_key as dispatch_time_key,
                              shipment.shipment_first_consignment_create_date_key as mh_out_date_key,
                              shipment.shipment_first_consignment_create_time_key as mh_out_time_key,
                              shipment.fsd_first_dh_received_date_key as dh_in_date_key,
                              shipment.fsd_first_dh_received_time_key as dh_in_time_key,
                              shipment.fsd_first_ofd_date_key as first_ofd_date_key,
                              J.day_of_week as first_ofd_day,
                              A.fulfill_item_unit_promise_tier as promise_tier,
                              shipment.number_of_air_hops as number_of_air_hops,
                              shipment.payment_type as payment_type,
                              H.city_tier as city_tier,
                              tpl.vendor_service_type as vendor_service_type
                              from bigfoot_external_neo.scp_fulfillment__fulfillment_speed_vas_new_hive_fact A
                              LEFT JOIN bigfoot_external_neo.scp_oms__date_dim_fact I on A.order_item_approve_date_key = I.date_dim_key
                              left join 
                              (select * from bigfoot_external_neo.scp_ekl__shipment_hive_90_fact where shipment_carrier = 'FSD') shipment 
                              on shipment.vendor_tracking_id=A.order_item_unit_tracking_id AND A.order_item_unit_shipment_id = shipment.merchant_reference_id
                              LEFT join bigfoot_external_neo.scp_oms__date_dim_fact C on shipment_first_out_for_pickup_date_key = C.date_dim_key
                              LEFT JOIN bigfoot_external_neo.scp_oms__date_dim_fact J on fsd_first_ofd_date_key = J.date_dim_key
                              left join bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim H on A.fulfill_item_unit_destination_pincode_key=H.logistics_geo_hive_dim_key   
                              LEFT JOIN bigfoot_external_neo.scp_oms__date_dim_fact B on A.fulfill_item_unit_deliver_actual_date_key = B.date_dim_key
                              left join bigfoot_external_neo.sp_product__product_categorization_hive_dim D on A.product_id_key=D.product_categorization_hive_dim_key 
                              left join bigfoot_external_neo.scp_fulfillment__fulfillment_tpl_shipment_intermediate_fact tpl ON tpl.vendor_tracking_id=A.order_item_unit_tracking_id
                              where B.week_num_in_year in (15,16,17,18,19,20) and B.year = 2017 and A.order_item_type <> 'digital' and analytic_business_unit in ('ElectronicDevices','ElectronicAccessory','HomeAndFurniture','Large','LifeStyle','Mobile')
                              group by 
                              A.approve_delivery_o2d_cdays_dd,
                              A.fulfill_item_unit_deliver_actual_date_key,
                              B.week_num_in_year,
                              Case when shipment.shipment_carrier = 'FSD' then 'FSD' else '3PL' end,
                              A.order_item_service_profile,
                              A.new_lzn,
                              A.fulfill_item_unit_dispatch_service_tier,
                              shipment.service_tier,
                              A.fulfill_item_unit_lpe_tier,
                              A.order_item_approve_date_key,
                              order_item_reserve_actual_date_key,
                              order_item_reserve_actual_time_key,
                              shipment.shipment_first_out_for_pickup_date_key,
                              C.day_of_week,
                              A.fulfill_item_unit_dispatch_actual_date_key,
                              A.fulfill_item_unit_dispatch_actual_time_key,
                              shipment.shipment_first_consignment_create_date_key,
                              shipment.shipment_first_consignment_create_time_key,
                              shipment.fsd_first_dh_received_date_key,
                              shipment.fsd_first_dh_received_time_key,
                              shipment.fsd_first_ofd_date_key,
                              J.day_of_week,
                              A.fulfill_item_unit_promise_tier,
                              shipment.number_of_air_hops,
                              shipment.payment_type,
                              H.city_tier,
                              tpl.vendor_service_type")
data<-sample_data_frame
#Removing NAs from O2D
data<-data[complete.cases(data$o2d),]
data<-data[complete.cases(data$approve_date_key),]

#Convert relevant dates to Date format
data$approve_date<-as.Date(as.character(data$approve_date_key), format="%Y%m%d")
data$dispatch_date <- as.Date(as.character(data$dispatch_date_key), format="%Y%m%d")
data$out_for_pickup_date<-as.Date(as.character(data$out_for_pickup_date_key),format="%Y%m%d")
data$dh_in_date<-as.Date(as.character(data$dh_in_date_key), format="%Y%m%d")
data$first_ofd_date<-as.Date(as.character(data$first_ofd_date_key), format="%Y%m%d")
data$reservation_date<-as.Date(as.character(data$reservation_date_key), format="%Y%m%d")
data$mh_out_date<-as.Date(as.character(data$mh_out_date_key), format="%Y%m%d")
data$deliver_date<-as.Date(as.character(data$deliver_date_key), format="%Y%m%d")

#modifying DH in, reserved, shipped dates
data$DH_bag_reach_date = ifelse((((data$shipment_service_tier == "Same Day Delivery" | data$shipment_service_tier == "Next Day Delivery") & (data$dh_in_time_key)/100 > 15) | ((data$shipment_service_tier == "Standard Delivery" | data$shipment_service_tier == "Economy Delivery") & (data$dh_in_time_key)/100 > 9)), (data$dh_in_date + 1), data$dh_in_date)
data$DH_bag_reach_date = as.Date(data$DH_bag_reach_date, origin = "1970-01-01")
data$BagReach2OFD<-as.numeric(difftime(data$first_ofd_date,data$DH_bag_reach_date ,units="days")) #same day OFD
data$OFD2D<-as.numeric(difftime(data$deliver_date,data$first_ofd_date,units="days"))  #FAD
#data$R2D<-as.numeric(difftime(data$dispatch_date,data$reserved_date,units="days"))  
#data$D2S<-as.numeric(difftime(data$MH_out_date,data$dispatch_date,units="days"))
data$reserved_date_new = as.Date(ifelse(data$reservation_time_key < 600, as.Date(data$reservation_date) - 1, as.Date(data$reservation_date)), format = "%Y-%m-%d", origin="1970-01-01")
#data$dispatch_date_new = as.Date(ifelse(data$dispatch_hour < 6, as.Date(data$dispatch_date) - 1, as.Date(data$dispatch_date)), format = "%Y-%m-%d", origin="1970-01-01")
data$ship_date_new = as.Date(ifelse(data$mh_out_time_key < 600, as.Date(data$mh_out_date) - 1, as.Date(data$mh_out_date)), format = "%Y-%m-%d", origin="1970-01-01")
#data$R2D_new<-as.numeric(difftime(data$dispatch_date_new,data$reserved_date_new,units="days"))
data$R2S_new<-as.numeric(difftime(data$ship_date_new,data$reserved_date_new,units="days"))  
#data$R2D<-as.numeric(difftime(data$dispatch_date,data$reserved_date,units="days"))

data$D4_flag<-ifelse(data$o2d<=4,1,0)
#data$D2_flag<-ifelse(data$o2d<=2,1,0) #incase D2 or D5 needs to be computed
#data$D5_flag<-ifelse(data$o2d<=5,1,0)

#service tiers, airhops, mode etc
data$dispatch_service_tier = ifelse(data$service_profile == "FBF", "FBF",ifelse(data$dispatch_service_tier =="EXPRESS","EXPRESS","REGULAR"))
data$lzn<-ifelse(data$lzn=="L1" | data$lzn=="L2" | data$lzn=="Z1","LZ1", ifelse(data$lzn == "N1" | data$lzn=="N2", "N", ifelse(data$lzn=="Z2", "Z2", "Missing")))
#data$shipment_service_tier = ifelse(data$shipment_service_tier %in% c("Next Day Delivery","Same Day Delivery"), "Express Delivery",data$shipment_service_tier)

data$number_of_air_hops[is.na(data$number_of_air_hops)]<-0;
data$air_surface = ifelse(data$number_of_air_hops > 0, "air", "surface")
data$mode_service_tier = paste(data$shipment_service_tier, "-", data$air_surface, sep = "")
data$city_tier = ifelse(data$city_tier == "Metro", "Metro", ifelse(data$city_tier == "Tier 1A" | data$city_tier == "Tier 1B", "Tier 1", "Tier 2+"))

#making negatives as zero
data$BagReach2OFD = ifelse(data$BagReach2OFD < 0, 0,data$BagReach2OFD)
data$R2S_new = ifelse(data$R2S_new < 0, 0,data$R2S_new)
data$OFD2D = ifelse(data$OFD2D < 0, 0,data$OFD2D)
#data$R2D_new = ifelse(data$R2D_new < 0, 0,data$R2D_new)

#making flags for FAD, D0_con, D0_OFD
data$D0_del = ifelse(data$OFD2D == 0, 1, 0)
#data$D0_dis = ifelse(data$R2D_new == 0, 1, 0)
#data$D0_dis_12_12 = ifelse(data$R2D == 0, 1, 0)
data$D0_con = ifelse(data$R2S_new == 0, 1, 0)
#data$D0_con_12_12 = ifelse(data$R2S == 0, 1, 0)
data$D0_OFD = ifelse(data$BagReach2OFD == 0, 1, 0)

#generating weekly numbers now
backup_data<-data
#overall<-data.frame(week=c(15))
#fbf<-data.frame(week=c(15))
#non_fbf<-data.frame(week=c(15))

#fbf_data = subset(data, data$service_profile == "FBF")
#nfbf_data = subset(data, data$service_profile == "NON_FBF")

a<-function(data){
 
overall<-data.frame(week=c(15))
fbf<-data.frame(week=c(15))
non_fbf<-data.frame(week=c(15))

#fbf & non_fbf split
fbf_data = subset(data, data$service_profile == "FBF")
nfbf_data = subset(data, data$service_profile == "NON_FBF")

data_tpl<-filter(data,shipment_carrier!="FSD" & vendor_service_type == "independent")
overall$tpl_ind_offload<-sum(data_tpl$units)/sum(data$units)
overall$D4_flag<-aggregate(data$units, by=list(data$D4_flag),FUN=sum)[2,2]/sum(data$units)
overall$volume<-sum(data$units)

data_tpl<-filter(fbf_data,shipment_carrier!="FSD" & vendor_service_type == "independent") 
fbf$tpl_ind_offload<-sum(data_tpl$units)/sum(fbf_data$units)
fbf$D4_flag<-aggregate(fbf_data$units, by=list(fbf_data$D4_flag),FUN=sum)[2,2]/sum(fbf_data$units)
fbf$volume<-sum(fbf_data$units)

data_tpl<-filter(nfbf_data,shipment_carrier!="FSD" & vendor_service_type == "independent") 
non_fbf$tpl_ind_offload<-sum(data_tpl$units)/sum(nfbf_data$units)
non_fbf$D4_flag<-aggregate(nfbf_data$units, by=list(nfbf_data$D4_flag),FUN=sum)[2,2]/sum(nfbf_data$units)
non_fbf$volume<-sum(nfbf_data$units)

#now only fsd
data<-filter(data,shipment_carrier=="FSD")
fbf_data<-filter(fbf_data,shipment_carrier=="FSD")
nfbf_data<-filter(nfbf_data,shipment_carrier=="FSD")
overall$fbf_mix<-aggregate(data$units, by=list(data$service_profile),FUN=sum)[1,2]/sum(data$units)
overall$express<-aggregate(data$units, by=list(data$dispatch_service_tier),FUN=sum)[1,2]/sum(data$units)
overall$FBF_D0_con<-aggregate(fbf_data$units, by=list(fbf_data$D0_con),FUN=sum)[2,2]/sum(data$units)
overall$D0_ofd<-aggregate(data$units, by=list(data$D0_OFD),FUN=sum)[2,2]/sum(data$units)
overall$FAD<-aggregate(data$units, by=list(data$D0_del),FUN=sum)[2,2]/sum(data$units)
overall$NDD<-aggregate(data$units, by=list(data$promise_tier),FUN=sum)[1,2]/sum(data$units)
overall$regular_air<-aggregate(data$units, by=list(data$mode_service_tier),FUN=sum)[which(aggregate(data$units, by=list(data$mode_service_tier),FUN=sum)$Group.1=="Standard Delivery-air"),2]/sum(data$units)
overall$regular_surface<-aggregate(data$units, by=list(data$mode_service_tier),FUN=sum)[which(aggregate(data$units, by=list(data$mode_service_tier),FUN=sum)$Group.1=="Standard Delivery-surface"),2]/sum(data$units)
overall$economy<-aggregate(data$units, by=list(data$shipment_service_tier),FUN=sum)[1,2]/sum(data$units)
overall$Metro<-aggregate(data$units, by=list(data$city_tier),FUN=sum)[1,2]/sum(data$units)
overall$Tier1<-aggregate(data$units, by=list(data$city_tier),FUN=sum)[2,2]/sum(data$units)
overall$Tier2<-aggregate(data$units, by=list(data$city_tier),FUN=sum)[3,2]/sum(data$units)
overall$nfbf_lz1<-aggregate(nfbf_data$units, by=list(nfbf_data$lzn),FUN=sum)[1,2]/sum(data$units)

#generating for fbf
fbf$lz1<-aggregate(fbf_data$units, by=list(fbf_data$lzn),FUN=sum)[1,2]/sum(fbf_data$units)
fbf$z2<-aggregate(fbf_data$units, by=list(fbf_data$lzn),FUN=sum)[4,2]/sum(fbf_data$units)
fbf$D0_con<-aggregate(fbf_data$units, by=list(fbf_data$D0_con),FUN=sum)[2,2]/sum(fbf_data$units)
fbf$D0_ofd<-aggregate(fbf_data$units, by=list(fbf_data$D0_OFD),FUN=sum)[2,2]/sum(fbf_data$units)
fbf$FAD<-aggregate(fbf_data$units, by=list(fbf_data$D0_del),FUN=sum)[2,2]/sum(fbf_data$units)
fbf$ndd<-aggregate(fbf_data$units, by=list(fbf_data$promise_tier),FUN=sum)[1,2]/sum(fbf_data$units)
fbf$regular_air<-aggregate(fbf_data$units, by=list(fbf_data$mode_service_tier),FUN=sum)[5,2]/sum(fbf_data$units)
fbf$regular_surface<-aggregate(fbf_data$units, by=list(fbf_data$mode_service_tier),FUN=sum)[6,2]/sum(fbf_data$units)
fbf$Metro<-aggregate(fbf_data$units, by=list(fbf_data$city_tier),FUN=sum)[1,2]/sum(fbf_data$units)
fbf$Tier1<-aggregate(fbf_data$units, by=list(fbf_data$city_tier),FUN=sum)[2,2]/sum(fbf_data$units)
fbf$Tier2<-aggregate(fbf_data$units, by=list(fbf_data$city_tier),FUN=sum)[3,2]/sum(fbf_data$units)

#generating for non_fbf
non_fbf$express<-aggregate(nfbf_data$units, by=list(nfbf_data$dispatch_service_tier),FUN=sum)[1,2]/sum(nfbf_data$units)
non_fbf$D0_ofd<-aggregate(nfbf_data$units, by=list(nfbf_data$D0_OFD),FUN=sum)[2,2]/sum(nfbf_data$units)
non_fbf$FAD<-aggregate(nfbf_data$units, by=list(nfbf_data$D0_del),FUN=sum)[2,2]/sum(nfbf_data$units)
non_fbf$regular_air<-aggregate(nfbf_data$units, by=list(nfbf_data$mode_service_tier),FUN=sum)[which(aggregate(nfbf_data$units, by=list(nfbf_data$mode_service_tier),FUN=sum)$Group.1=="Standard Delivery-air"),2]/sum(nfbf_data$units)
non_fbf$regular_surface<-aggregate(nfbf_data$units, by=list(nfbf_data$mode_service_tier),FUN=sum)[which(aggregate(nfbf_data$units, by=list(nfbf_data$mode_service_tier),FUN=sum)$Group.1=="Standard Delivery-surface"),2]/sum(nfbf_data$units)
non_fbf$eco<-aggregate(nfbf_data$units, by=list(nfbf_data$shipment_service_tier),FUN=sum)[1,2]/sum(nfbf_data$units)
#non_fbf$sunday_pickup<-aggregate(nfbf_data$units, by=list(nfbf_data$pickup_day),FUN=sum)[1,2]/(sum(aggregate(nfbf_data$units, by=list(nfbf_data$pickup_day),FUN=sum)[-1,]$x)/6)
#non_fbf$sunday_attempt<-aggregate(nfbf_data$units, by=list(nfbf_data$first_ofd_day),FUN=sum)[1,2]/(sum(aggregate(nfbf_data$units, by=list(nfbf_data$first_ofd_day),FUN=sum)[-1,]$x)/6)
non_fbf$Metro<-aggregate(nfbf_data$units, by=list(nfbf_data$city_tier),FUN=sum)[1,2]/sum(nfbf_data$units)
non_fbf$Tier1<-aggregate(nfbf_data$units, by=list(nfbf_data$city_tier),FUN=sum)[2,2]/sum(nfbf_data$units)
non_fbf$Tier2<-aggregate(nfbf_data$units, by=list(nfbf_data$city_tier),FUN=sum)[3,2]/sum(nfbf_data$units)
non_fbf$nfbf_lz1<-aggregate(nfbf_data$units, by=list(nfbf_data$lzn),FUN=sum)[1,2]/sum(nfbf_data$units)

return(list(overall,fbf,non_fbf))
}


overall<-data.frame()
fbf<-data.frame()
non_fbf<-data.frame()
del_date<-min(data$deliver_date)
while(del_date<=max(data$deliver_date)){
f<-a(filter(data,deliver_date==del_date)) 
ifelse(nrow(overall)==0,overall<-as.data.frame(f[1]),overall<-rbind(overall,as.data.frame(f[1])))
ifelse(nrow(fbf)==0,fbf<-as.data.frame(f[2]),fbf<-rbind(fbf,as.data.frame(f[2])))
ifelse(nrow(non_fbf)==0,non_fbf<-as.data.frame(f[3]),non_fbf<-rbind(non_fbf,as.data.frame(f[3])))
del_date=del_date+1               
}

#speed_overall$FBF_LZ1_Z2<-sum(speed_overall$FBF_LZ1,speed_overall$FBF_Z2)
set.seed(51)
#train<-overall[1:62,]
train<-sample_frac(non_fbf, 0.7) # change the name of the data set overall, fbf, non_fbf
sid<-as.numeric(rownames(train)) 
test<-non_fbf[-sid,] # change the name of the data set overall, fbf, non_fbf
test_backup = test

#Overall
x=model.matrix(D4_flag ~ economy +  nfbf_lz1 + express + FBF_D0_con + FAD + NDD + tpl_ind_offload + Tier2, train)
y = train$D4_flag
xtest=model.matrix(D4_flag ~ economy +  nfbf_lz1 + express + FBF_D0_con + FAD + NDD + tpl_ind_offload + Tier2, test)


plot(speed_overall$D4_units, log(speed_overall$tier2), pch=19)
abline(lm(speed_overall$D4_units ~ log(speed_overall$tier2)), col="red") 

#FBF
x=model.matrix(D4_flag ~  FAD  + D0_con + ndd +  tpl_ind_offload  + Tier2 
               + regular_surface + regular_air ,train)
y = train$D4_flag
xtest=model.matrix(D4_flag ~  FAD  + D0_con + ndd +  tpl_ind_offload  + Tier2 
                   + regular_surface + regular_air, test)

plot(speed_overall$FBF_D4_units, speed_overall$FBF_tier2, pch=19)
abline(lm(speed_overall$FBF_D4_units ~ speed_overall$FBF_tier2), col="red") 

#NFBF
x = model.matrix(D4_flag ~ express + FAD + eco + Tier2 + nfbf_lz1 +tpl_ind_offload+Metro
                 , train)
y = train$D4_flag
xtest=model.matrix(D4_flag ~ express + FAD + eco + Tier2 + nfbf_lz1 + tpl_ind_offload +Metro, test)

#common  for alll
fit=glmnet (x,y,alpha=0.7, lambda=cv.glmnet(x, y)$lambda.min)
test$objective<-predict(fit, newx=xtest, s = "lambda.min")
test$predicted_D4 <- test$objective
test$prediction_error <- test$D4_flag - test$predicted_D4
accuracy(as.numeric(test$D4_flag), test$predicted_D4)
test$prediction_error
fit
fit$a0 #Intercept
fit$beta #Coeffs 
 
