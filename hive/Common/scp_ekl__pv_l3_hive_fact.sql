INSERT OVERWRITE TABLE pv_l3_hive_fact 
select distinct
liq.inventory_item_id,
liq.PV_Bucket,
liq.pv_id, 
liq.quantity,
liq.Initial_PV_Reasons,
liq.Initial_PV_Sub_Reasons,
liq.Initial_PV_Reasons_desc,
liq.Initial_PV_Sub_Reasons_desc,
liq.initial_pv_done_by,
liq.Detailed_PV_Reasons,
liq.Detailed_PV_Sub_Reasons,
liq.Detailed_PV_Reasons_desc,
liq.Detailed_PV_Sub_Reasons_desc,
liq.Detailed_pv_done_by,
liq.RE_PV_Reasons,
liq.RE_PV_Sub_Reasons,
liq.RE_PV_Reasons_desc,
liq.RE_PV_Sub_Reasons_desc,
liq.RE_PV_done_by,
liq.external_inspection_Reasons,
liq.external_inspection_Sub_Reasons,
liq.external_inspection_Reasons_desc,
liq.external_inspection_Sub_Reasons_desc,
liq.external_inspection_done_by,
liq.created_at,
liq.qc_ticket_item_created_date_key,
liq.qc_ticket_item_created_time_key,
liq.Initial_PV_updated_at,
liq.Initial_PV_updated_date_key,
liq.initial_pv_updated_time_key,
liq.Detailed_PV_updated_at,
liq.Detailed_PV_updated_date_key,
liq.Detailed_pv_updated_time_key,
liq.RE_PV_updated_at,
liq.RE_PV_updated_date_key,
liq.RE_PV_updated_time_key,
liq.external_inspection_updated_at,
liq.external_inspection_updated_date_key,
liq.external_inspection_updated_time_key,
liq.ti_entity_id,
liq.pi_id_min ,
liq.pi_id_max , 
liq.putlist_id_min , 
liq.putlist_id_max ,
liq.mri_movement_request_item_id_min ,
liq.mri_movement_request_item_id_max ,
liq.putlist_creation_time_min ,
liq.putlist_creation_date_min_key,
liq.putlist_creation_time_min_key,
liq.putlist_creation_time_max,
liq.putlist_creation_date_max_key,
liq.putlist_creation_time_max_key,
liq.putlist_created_by,
liq.mr_source_type_min,
liq.mr_destination_type_min,
liq.mr_source_type_max,
liq.mr_destination_type_max,
liq.mis_shipment_filter,
liq.prexo_filter,
liq.inventory_item_storage_location_id,
liq.inventory_item_storage_location_id_key,
liq.inventory_item_quantity,
liq.inventory_item_created_timestamp,
liq.inventory_item_created_date_key,
liq.inventory_item_created_time_key,
liq.inventory_item_updated_at,
liq.inventory_item_updated_date_key,
liq.inventory_item_updated_time_key,
liq.inventory_item_warehouse_id,
liq.inventory_item_wid,
liq.inventory_item_grn_id,
liq.inventory_item_wh_serial_number_id ,
liq.wsn_display_id, 
liq.inventory_item_storage_location_type, 
liq.serial_number,
liq.Free_text_comment,
liq.Add_access_comment,
liq.prexo_imei_sl_number_received,
liq.prexo_brand_received,
liq.prexo_model_received,
liq.prexo_initial_imei_sl_number_captured,
liq.prexo_initial_brand_captured,
liq.prexo_initial_model_captured,
liq.product_detail_hive_dim_key,
liq.inventory_item_storage_location_min_id        ,
liq.inventory_item_storage_location_max_id        ,
liq.inventory_item_storage_location_min_id_key    ,
liq.inventory_item_storage_location_max_id_key    ,
liq.wh_storage_location_min                       ,
liq.wh_storage_location_max                       ,
liq.variance_inventory_item_id,
liq.variance_reason_dim_key,
liq.variance_quantity,
liq.variance_created_at,
liq.variance_created_date_key,
liq.variance_created_time_key,
liq.variance_wid,
liq.variance_warehouse_id,
liq.variance_id,
liq.variance_created_by,
liq.variance_storage_location_dim_key,
liq.variance_warehouse_dim_key,
liq.iiv_product_detail_hive_dim_key,
liq.shipment_item_id,
liq.shipment_display_id,
liq.shipment_id,
liq.shipment_return_warehouse_id,
liq.shipment_type,-- need to check
liq.shipment_destination_type, -- need to check
liq.shipment_tracking_id,
liq.shipment_box_used_packing_box_id,
liq.wh_rvp_rto_flag,
liq.shipment_status, -- Newly Added
liq.shipment_received_date_key,
liq.shipment_item_status,
liq.shipment_item_quantity,
liq.shipment_courier_name, -- need to check
liq.shipment_item_is_excess,
liq.shipment_is_tampered,
liq.warehouse_id,
liq.shipment_item_order_item_id,
liq.shipment_item_quantity_received               ,
liq.shipment_item_quantity_lost_during_dispatch   ,
liq.shipment_item_quantity_packed                 ,
liq.shipment_item_product_key                     ,
liq.shipment_item_updated_timestamp               ,
liq.shipment_item_updated_date_key                ,
liq.shipment_item_updated_time_key                ,
liq.shipment_item_cancelled_timestamp             ,
liq.shipment_item_cancelled_date_key              ,
liq.shipment_item_cancelled_time_key              ,
liq.shimpment_created_timestamp                   ,
liq.shimpment_created_date_key                    ,
liq.shimpment_created_time_key                    ,
liq.shipment_qc_done_timestamp                    ,
liq.shipment_qc_done_date_key                     ,
liq.shipment_qc_done_time_key                     ,
liq.shipment_updated_timestamp                    ,
liq.shipment_updated_date_key                     ,
liq.shipment_updated_time_key                     ,
liq.shipment_cancelled_timestamp                  ,
liq.shipment_cancelled_date_key                   ,
liq.shipment_cancelled_time_key                   ,
liq.shipment_rto_received_timestamp               ,
liq.shipment_rto_received_date_key                ,
liq.shipment_rto_received_time_key                ,
liq.shipment_rvp_received_timestamp               ,
liq.shipment_rvp_received_date_key                ,
liq.shipment_rvp_received_time_key                ,
liq.shipment_rto_mh_scan_timestamp                ,
liq.shipment_rto_mh_scan_date_key                 ,
liq.shipment_rto_mh_scan_time_key                 ,
liq.shipment_rvp_mh_scan_timestamp                ,
liq.shipment_rvp_mh_scan_date_key                 ,
liq.shipment_rvp_mh_scan_time_key                 ,
liq.shipment_dispatched_timestamp                 ,
liq.shipment_dispatched_date_key                  ,
liq.shipment_dispatched_time_key                  ,
liq.shipment_warehouse_dim_key                    ,
liq.shipment_ekart_dispatch_by_date               ,
liq.shipment_ekart_dispatch_by_date_key           ,
liq.shipment_ekart_dispatch_by_time_key           ,
liq.shipment_ff_dispatch_by_date                  ,
liq.shipment_ff_dispatch_by_date_key              ,
liq.shipment_ff_dispatch_by_time_key              ,
liq.shipment_packed_timestamp                     ,
liq.shipment_packed_date_key                      ,
liq.shipment_packed_time_key                      ,
liq.return_item_id                ,
liq.return_id                     ,
liq.order_item_id                 ,
liq.return_item_quantity          ,
liq.return_item_shipment_id       ,
liq.return_item_tracking_id       ,
liq.courier_name                  ,
if(courier_name is null,
if(liq.shipment_courier_name is null,if(s.shipment_carrier is null,'No Courier found',s.shipment_carrier),liq.shipment_courier_name),if(liq.courier_name in ('flipkartlogistics','flipkartlogistics-cod'),'EKL','3PL')) as return_courier_bucket,
liq.return_quantity_recvd_in_wh    ,
liq.return_quantity_rvp_done       ,
liq.return_item_notional_value    ,
liq.return_reason                  ,
liq.return_quantity_qc_passed      ,
liq.return_quantity_qc_failed      ,
liq.oms_qc_reason                  ,
liq.oms_qc_comment                 ,
liq.tenant_id                      ,
liq.seller_id                      ,
liq.seller_bucket,
liq.return_sub_reason               ,
liq.exchange_fsn_key                ,
liq.exchange_fsn,
liq.return_pickup_promise_date      ,
liq.external_order_id               ,
liq.return_status                   ,
liq.return_to_address_id            ,
liq.return_customer_will_send       ,
liq.return_action                   ,
liq.return_type                     ,
liq.return_request_channel          ,
liq.return_amount                   ,
liq.return_comments                 ,
liq.returned_product_id_key         ,
liq.return_request_date             , 
liq.return_reason_bucket_key                  ,
liq.return_courier_name_key                   ,
liq.seller_id_key                             ,
liq.return_pickup_promise_date_key            ,
liq.return_product_listing_id_key             ,
liq.return_request_date_key                   ,
liq.order_id                        ,        
liq.order_external_id               ,           
liq.order_item_selling_price        ,           
liq.order_item_seller_id            ,           
liq.order_item_seller_id_key        ,           
liq.order_item_quantity             ,                    
liq.order_shipping_address_id_key   ,           
liq.return_item_product_title             ,
liq.return_item_product_id              ,
liq.return_item_product_id_key      ,
liq.return_item_category_id         ,
liq.order_item_type,
liq.order_item_date,
--liq.account_id,
pb.packing_box_name                          ,
pb.packing_box_is_valid,
pb.packing_box_is_active,
pb.packing_box_created_at,
pb.packing_box_created_date_key,
pb.packing_box_updated_at,
pb.packing_box_updated_date_key,
pb.packing_box_category,
s.ekl_fin_zone               ,
NULL  as initial_capured_serial_number,
liq.product_id_key,
liq.product_fsn,
liq.product_listing_id,
liq.product_id,
liq.packing_box_used_name,
liq.warehouse_company  ,
if(liq.wh_storage_location_max='external_liquidation_damaged_bulk','external_liquidation_damaged_area',
if(liq.wh_storage_location_max='external_liquidation_non_damaged_bulk','external_liquidation_non_damaged_area',
if(liq.wh_storage_location_max='fraud_bulk','fraud_area',
if(liq.wh_storage_location_max='refurbishment_area','refurbishment_area',
if(liq.wh_storage_location_max='product_exchange_bulk','product_exchange_area',
if(liq.wh_storage_location_max='verification_pending_area','verification_pending_area',
if(liq.wh_storage_location_max='returns_supplier_return_bulk','returns_supplier_return_area',
if(liq.wh_storage_location_max='refinishing_required_bulk','refinishing',
if(liq.wh_storage_location_max='return_qc_pass_bulk','store',
if(liq.wh_storage_location_max='return_bd_bulk','returns_area',
if(liq.wh_storage_location_max='disposal_area','disposal_bulk',
if(liq.wh_storage_location_max='seller_unidentified_area','seller_unidentified_area',
if(liq.wh_storage_location_max='seller_return_bulk','seller_return_area',
if(liq.wh_storage_location_max='product_missing_bulk','product_missing_bulk',liq.mr_destination_type_max
))))))))))))))
as destination_area,
case when shipment_return_warehouse_id like '%L' then 1 else 0 end as ls_large,
liq.reservation_order_item_id,
--liq.reservation_inventory_item_id,
liq.pv_max_numdate,
liq.pv_max_num_date_key,
liq.pv_max_num_time_key,
lookupkey('product_id',liq.product_fsn) as product_categorization_dim_key,
liq.shipmentid as shipmentid,
liq.fin_inv_location_min as fin_inv_location_min,
liq.fin_inv_location_max as fin_inv_location_max,
if(liq.return_type is null,liq.wh_rvp_rto_flag,liq.return_type) as ri_return_type,
------------------------------------------
s.shipment_id as ekl_unique_key,
s.ekl_shipment_type as ekl_shipment_type,
s.shipment_current_status  as shipment_current_status,
s.shipment_carrier as shipment_carrier,
s.seller_type as seller_type,
s.first_mh_tc_receive_date_key as first_mh_tc_receive_date_key,
s.first_mh_tc_receive_time_key as first_mh_tc_receive_time_key,
s.last_mh_tc_receive_date_key as last_mh_tc_receive_date_key,
s.last_mh_tc_receive_time_key as last_mh_tc_receive_time_key,
s.first_mh_tc_outscan_date_key as first_mh_tc_outscan_date_key,
s.first_mh_tc_outscan_time_key as first_mh_tc_outscan_time_key,
s.last_mh_tc_outscan_date_key as last_mh_tc_outscan_date_key,
s.last_mh_tc_outscan_time_key as last_mh_tc_outscan_time_key,
s.first_dh_outscan_date_key as first_dh_outscan_date_key,
s.first_dh_outscan_time_key as first_dh_outscan_time_key,
s.last_dh_outscan_date_key as last_dh_outscan_date_key,
s.last_dh_outscan_time_key as last_dh_outscan_time_key,
s.fsd_first_dh_received_date_key as fsd_first_dh_received_date_key,
s.fsd_first_dh_received_time_key as fsd_first_dh_received_time_key,
s.fsd_last_dh_received_date_key as fsd_last_dh_received_date_key,
s.fsd_last_dh_received_time_key as fsd_last_dh_received_time_key,
s.shipment_delivered_at_date_key as shipment_delivery_date_key,
s.shipment_delivered_at_time_key as shipment_delivery_time_key,
s.shipment_weight as shipment_weight,
s.cs_notes as cs_notes,
s.hub_notes as hub_notes,
s.fsd_number_of_ofd_attempts as fsd_number_of_ofd_attempts,
s.payment_type as payment_type,
s.shipment_value as shipment_value,
s.reject_reason as reject_reason,
s.reject_sub_reason as reject_sub_reason,
s.fsd_last_ofd_date_key as fsd_last_attempted_date_key,
s.fsd_last_ofd_time_key as fsd_last_attempted_time_key,
s.shipment_first_consignment_co_loader as shipment_first_consignment_co_loader,
s.shipment_last_consignment_co_loader as shipment_last_consignment_co_loader,
s.reverse_shipment_type as reverse_shipment_type,
s.shipment_first_received_dh_id_key ,
s.shipment_last_received_dh_id_key,
s.shipment_first_received_mh_id_key,
s.shipment_last_received_mh_id_key,
s.reverse_pickup_hub_id_key,
s.fe_name_key,
s.customer_address_id_key,
t15.account_id,
--lookupkey('account_id',s5.account_id) as account_hive_dim_key,
s.destination_pincode_key,
s.source_pincode_key,
erp.tl_verification_flag,
erp.tl_cs_override_flag,
erp.tl_checkedby,
erp.tl_ldap_id,
erp.tl_pickup_creation_date_key,
erp.tl_pickup_creation_time_key,
erp.fe_verification_flag,
erp.fe_cs_override_flag,
erp.fe_checkedby,
erp.fe_ldap_id,
erp.fe_pickup_creation_date_key,
erp.fe_pickup_creation_time_key,
if(liq.return_to_address_id is null,liq.shipment_return_warehouse_id,liq.return_to_address_id) as return_warehouse,
liq.return_numdate,
liq.return_numdate_key,
concat(t16.account_first_name,'_',t16.account_last_name) as customer_name,
t16.account_primary_phone as customer_phone,
t16.account_primary_email as customer_email,
--if(shipment_status in ('received','returned'),shimpment_created_date_key,shimpment_created_date_key) as shipment_received_date_key,
if(shipment_status='received',shipment_rvp_received_date_key,if(shipment_status='returned',shipment_rto_received_date_key,shipment_updated_date_key)) as shipment_rto_rvp_received_date_key,
liq.number1 as number1,
liq.number2 as number2,
liq.number3 as number3,
liq.Prexo_initial_vertical_captured as prexo_initial_vertical_captured,
liq.Prexo_initial_vertical_type_captured as prexo_initial_vertical_type_captured,
liq.Prexo_initial_title_captured as prexo_initial_title_captured,
liq.Prexo_initial_box_psn_captured as prexo_initial_box_psn_captured,
lookupkey('agent_id',smart.agent_id) as smart_agent_key,
smart.rvp_help_line_dialed,
lookup_date(smart.tl_timestamp) as sm_v3_tl_date_key,
lookup_time(smart.tl_timestamp) as sm_v3_tl_time_key,
smart.cs_override,
smart.shipment_status_reason,
smart.outer_tl_result,
smart.outer_fe_result,
smart.reason_code,
smart.team_leader_ldap,
smart.return_type,
smart.shipment_status_code,
smart.shipment_value,
lookup_date(smart.fe_timestamp) as v3_sm_fe_date_key,
lookup_time(smart.fe_timestamp) as v3_sm_fe_time_key,
smart.inaccuracy,
smart.outer_inaccuracy,
smart.pk_wo_cs_or_afr_fail,
smart.reverse_misshipment,
smart.imei_mismatch,
smart.checks_inaccurate,
(case when (smart.outer_inaccuracy=1 or smart.pk_wo_cs_or_afr_fail=1 or smart.imei_mismatch=1) and liq.wh_storage_location_max NOT IN ('verification_pending_area','return_qc_pass_bulk') then 1 else 0 end) as final_inaccuracy,
smart.checks,
smart.check_results,
smart.ofp_attempts as ofp_attempts,
smart.ismandatory_list,
smart.fe_result_list,
smart.tl_result_list,
smart.sub_item_name,
smart.tl_inaccuracy,
smart.tl_outer_inaccuracy,
(case when (smart.tl_outer_inaccuracy=1 or smart.pk_wo_cs_or_afr_fail=1 or smart.imei_mismatch=1) and liq.wh_storage_location_max NOT IN ('verification_pending_area','return_qc_pass_bulk') then 1 else 0 end) as tl_final_inaccuracy,

VR1.variance_type,
VR1.variance_reason_type,
VR1.variance_reason_description,
PD1.product_detail_cms_vertical,
PD1.product_detail_fsn,
PD1.product_listing_dim_fsp,
PD1.product_detail_cms_brand,
PD1.product_detail_seller_id,
PD1.product_detail_product_title,
PD1.product_categorization_dim_business_unit,
PD1.product_categorization_dim_super_category,
PD1.product_categorization_dim_sub_category,
PD1.product_categorization_dim_category



from bigfoot_external_neo.scp_ekl__qc_liquidation_l2_hive_fact liq 

left outer join bigfoot_external_neo.scp_warehouse__fc_packing_box_l0_hive_fact pb on (liq.warehouse_company=pb.packing_box_company and liq.shipment_box_used_packing_box_id=pb.packing_box_id)
left outer join bigfoot_external_neo.scp_ekl__shipment_hive_90_fact s on (s.ekl_shipment_type in ('rvp','unapproved_rto','approved_rto') and liq.shipmentid=s.vendor_tracking_id) 
left outer join bigfoot_external_neo.cp_user__address_hive_dim t15 on (s.customer_address_id_key=t15.address_hive_dim_key)
left outer join bigfoot_external_neo.cp_user__account_hive_dim t16 on t15.account_id=t16.account_id
left outer join bigfoot_external_neo.scp_warehouse__fc_variance_reason_hive_dim as VR1 on VR1.fc_variance_reason_hive_dim_key = liq.variance_reason_dim_key
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as PD1 on PD1.fc_product_detail_hive_dim_key = liq.product_id_key

left outer join
(
select return_shipment_id,
max(if(t20.checkedby='ERP',verification_status,null)) as tl_verification_flag,
max(if(t20.checkedby='ERP',cs_override,null)) as tl_cs_override_flag,
max(if(t20.checkedby='ERP',checkedby,null)) as tl_checkedby,
max(if(t20.checkedby='ERP',agent_id,null)) as tl_ldap_id,
max(if(t20.checkedby='ERP',pickup_creation_date_key,null)) as tl_pickup_creation_date_key,
max(if(t20.checkedby='ERP',pickup_creation_time_key,null)) as tl_pickup_creation_time_key,
max(if(t20.checkedby='LM_APP',verification_status,null)) as fe_verification_flag,
max(if(t20.checkedby='LM_APP',cs_override,null)) as fe_cs_override_flag,
max(if(t20.checkedby='LM_APP',checkedby,null)) as fe_checkedby,
max(if(t20.checkedby='LM_APP',agent_id,null)) as fe_ldap_id,
max(if(t20.checkedby='LM_APP',pickup_creation_date_key,null)) as fe_pickup_creation_date_key,
max(if(t20.checkedby='LM_APP',pickup_creation_time_key,null)) as fe_pickup_creation_time_key
from bigfoot_external_neo.scp_ekl__smartpickup_l1_fact t20 
where t20.reverse_type IN ('RTO','RVP') and t20.pickup_creation_date_key>'20160101' 
group by return_shipment_id
) erp on (s.vendor_tracking_id=erp.return_shipment_id)

left outer join
bigfoot_external_neo.scp_ekl__smart_pickup_l2_hive_fact smart
on liq.shipmentid=smart.shipment_id ;

