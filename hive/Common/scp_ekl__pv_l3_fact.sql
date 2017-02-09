INSERT OVERWRITE TABLE pv_l3_fact
select 
inventory_item_id                                      ,
pv_bucket                                              ,
pv_id                                                  ,
quantity                                               ,
initial_pv_reasons                                     ,
initial_pv_sub_reasons                                 ,
initial_pv_reasons_desc                                ,
initial_pv_sub_reasons_desc                            ,
initial_pv_done_by                                     ,
detailed_pv_reasons                                    ,
detailed_pv_sub_reasons                                ,
detailed_pv_reasons_desc                               ,
detailed_pv_sub_reasons_desc                           ,
detailed_pv_done_by                                    ,
re_pv_reasons                                          ,
re_pv_sub_reasons                                      ,
re_pv_reasons_desc                             	        ,
re_pv_sub_reasons_desc                                 ,
re_pv_done_by                                          ,
external_inspection_reasons                            ,
external_inspection_sub_reasons                        ,
external_inspection_reasons_desc                       ,
external_inspection_sub_reasons_desc                   ,
external_inspection_done_by                            ,
created_at                                             ,
qc_ticket_item_created_date_key                        ,
qc_ticket_item_created_time_key                        ,
initial_pv_updated_at                                  ,
initial_pv_updated_date_key                            ,
initial_pv_updated_time_key                            ,
detailed_pv_updated_at                                 ,
detailed_pv_updated_date_key                           ,
detailed_pv_updated_time_key                           ,
re_pv_updated_at                                       ,
re_pv_updated_date_key                                 ,
re_pv_updated_time_key                                 ,
external_inspection_updated_at                         ,
external_inspection_updated_date_key                   ,
external_inspection_updated_time_key                   ,
ti_entity_id                                           ,
pi_id_min                                              ,
pi_id_max                                              ,
putlist_id_min                                         ,
putlist_id_max                                         ,
mri_movement_request_item_id_min                       ,
mri_movement_request_item_id_max                       ,
putlist_creation_time_min                              ,
putlist_creation_date_min_key                          ,
putlist_creation_time_min_key                          ,
putlist_creation_time_max                              ,
putlist_creation_date_max_key                          ,
putlist_creation_time_max_key                          ,
putlist_created_by                                     ,
mr_source_type_min                                     ,
mr_destination_type_min                                ,
mr_source_type_max                                     ,
mr_destination_type_max                                ,
mis_shipment_filter                                    ,
prexo_filter                                           ,
inventory_item_storage_location_id                     ,
inventory_item_storage_location_id_key                 ,
inventory_item_quantity                                ,
inventory_item_created_timestamp                       ,
inventory_item_created_date_key                        ,
inventory_item_created_time_key                        ,
inventory_item_updated_at                              ,
inventory_item_updated_date_key                        ,
inventory_item_updated_time_key                        ,
inventory_item_warehouse_id                            ,
inventory_item_wid                                     ,
inventory_item_grn_id                                  ,
inventory_item_wh_serial_number_id                     ,
wsn_display_id                                         ,
inventory_item_storage_location_type                   ,
serial_number                                          ,
free_text_comment                                      ,
add_access_comment                                     ,
prexo_imei_sl_number_received                          ,
prexo_brand_received                                   ,
prexo_model_received                                   ,
prexo_initial_imei_sl_number_captured                  ,
prexo_initial_brand_captured                           ,
prexo_initial_model_captured                           ,
product_detail_hive_dim_key                            ,
inventory_item_storage_location_min_id                 ,
inventory_item_storage_location_max_id                 ,
inventory_item_storage_location_min_id_key             ,
inventory_item_storage_location_max_id_key             ,
wh_storage_location_min                                ,
wh_storage_location_max                                ,
variance_inventory_item_id                             ,
variance_reason_dim_key                                ,
variance_quantity                                      ,
variance_created_at                                    ,
variance_created_date_key                              ,
variance_created_time_key                              ,
variance_wid                                           ,
variance_warehouse_id                                  ,
variance_id                                            ,
variance_created_by                                    ,
variance_storage_location_dim_key                      ,
variance_warehouse_dim_key                             ,
iiv_product_detail_hive_dim_key                        ,
shipment_item_id                                       ,
shipment_display_id                                    ,
shipment_id                                            ,
shipment_return_warehouse_id                           ,
shipment_type                                          ,
shipment_destination_type                              ,
shipment_tracking_id                                   ,
shipment_box_used_packing_box_id                       ,
wh_rvp_rto_flag                                        ,
shipment_status                                        ,
shipment_received_date_key                             ,
shipment_item_status                                   ,
shipment_item_quantity                                 ,
shipment_courier_name                                  ,
shipment_item_is_excess                                ,
shipment_is_tampered                                   ,
warehouse_id                                           ,
shipment_item_order_item_id                            ,
shipment_item_quantity_received                        ,
shipment_item_quantity_lost_during_dispatch            ,
shipment_item_quantity_packed                          ,
shipment_item_product_key                              ,
shipment_item_updated_timestamp                        ,
shipment_item_updated_date_key                         ,
shipment_item_updated_time_key                         ,
shipment_item_cancelled_timestamp                      ,
shipment_item_cancelled_date_key                       ,
shipment_item_cancelled_time_key                       ,
shimpment_created_timestamp                            ,
shimpment_created_date_key                             ,
shimpment_created_time_key                             ,
shipment_qc_done_timestamp                             ,
shipment_qc_done_date_key                              ,
shipment_qc_done_time_key                              ,
shipment_updated_timestamp                             ,
shipment_updated_date_key                              ,
shipment_updated_time_key                              ,
shipment_cancelled_timestamp                           ,
shipment_cancelled_date_key                            ,
shipment_cancelled_time_key                            ,
shipment_rto_received_timestamp                        ,
shipment_rto_received_date_key                         ,
shipment_rto_received_time_key                         ,
shipment_rvp_received_timestamp                        ,
shipment_rvp_received_date_key                         ,
shipment_rvp_received_time_key                         ,
shipment_rto_mh_scan_timestamp                         ,
shipment_rto_mh_scan_date_key                          ,
shipment_rto_mh_scan_time_key                          ,
shipment_rvp_mh_scan_timestamp                         ,
shipment_rvp_mh_scan_date_key                          ,
shipment_rvp_mh_scan_time_key                          ,
shipment_dispatched_timestamp                          ,
shipment_dispatched_date_key                           ,
shipment_dispatched_time_key                           ,
shipment_warehouse_dim_key                             ,
shipment_ekart_dispatch_by_date                        ,
shipment_ekart_dispatch_by_date_key                    ,
shipment_ekart_dispatch_by_time_key                    ,
shipment_ff_dispatch_by_date                           ,
shipment_ff_dispatch_by_date_key                       ,
shipment_ff_dispatch_by_time_key                       ,
shipment_packed_timestamp                              ,
shipment_packed_date_key                               ,
shipment_packed_time_key                               ,
return_item_id                                         ,
return_id                                              ,
order_item_id                                          ,
return_item_quantity                                   ,
return_item_shipment_id                                ,
return_item_tracking_id                                ,
courier_name                                           ,
return_courier_bucket                                  ,
return_quantity_recvd_in_wh                            ,
return_quantity_rvp_done                               ,
return_item_notional_value                             ,
return_reason                                          ,
return_quantity_qc_passed                              ,
return_quantity_qc_failed                              ,
oms_qc_reason                                          ,
oms_qc_comment                                         ,
tenant_id                                              ,
seller_id                                              ,
seller_bucket                                          ,
return_sub_reason                                      ,
exchange_fsn_key                                       ,
exchange_fsn                                           ,
return_pickup_promise_date                             ,
external_order_id                                      ,
return_status                                          ,
return_to_address_id                                   ,
return_customer_will_send                              ,
return_action                                          ,
return_type                                            ,
return_request_channel                                 ,
return_amount                                          ,
return_comments                                        ,
returned_product_id_key                                ,
return_request_date                                    ,
return_reason_bucket_key                               ,
return_courier_name_key                                ,
seller_id_key                                          ,
return_pickup_promise_date_key                         ,
return_product_listing_id_key                          ,
return_request_date_key                                ,
order_id                                               ,
order_external_id                                      ,
order_item_selling_price                               ,
order_item_seller_id                                   ,
order_item_seller_id_key                               ,
order_item_quantity                                    ,
order_shipping_address_id_key                          ,
return_item_product_title                              ,
return_item_product_id                                 ,
return_item_product_id_key                             ,
return_item_category_id                                ,
order_item_type                                        ,
order_item_date                                        ,
--account_id                                             ,
packing_box_name                                       ,
packing_box_is_valid                                   ,
packing_box_is_active                                  ,
packing_box_created_at                                 ,
packing_box_created_date_key                           ,
packing_box_updated_at                                 ,
packing_box_updated_date_key                           ,
packing_box_category                                   ,
ekl_fin_zone                                           ,
initial_capured_serial_number,
product_id_key,
product_fsn,
product_listing_id,
product_id       ,
packing_box_used_name,
warehouse_company,
destination_area,
is_large,
reservation_order_item_id,
pv_max_numdate,
pv_max_num_date_key,
pv_max_num_time_key,
product_categorization_dim_key,
shipmentid,
fin_inv_location_min,
fin_inv_location_max,
ri_return_type,
ekl_unique_key,
ekl_shipment_type,
shipment_current_status,
shipment_carrier,
seller_type,
first_mh_tc_receive_date_key,
first_mh_tc_receive_time_key,
last_mh_tc_receive_date_key,
last_mh_tc_receive_time_key,
first_mh_tc_outscan_date_key,
first_mh_tc_outscan_time_key,
last_mh_tc_outscan_date_key,
last_mh_tc_outscan_time_key,
first_dh_outscan_date_key,
first_dh_outscan_time_key,
last_dh_outscan_date_key,
last_dh_outscan_time_key,
fsd_first_dh_received_date_key,
fsd_first_dh_received_time_key,
fsd_last_dh_received_date_key,
fsd_last_dh_received_time_key,
shipment_delivery_date_key,
shipment_delivery_time_key,
shipment_weight,
cs_notes,
hub_notes,
fsd_number_of_ofd_attempts,
payment_type,
shipment_value,
reject_reason,
reject_sub_reason,
fsd_last_attempted_date_key,
fsd_last_attempted_time_key,
shipment_first_consignment_co_loader,
shipment_last_consignment_co_loader,
reverse_shipment_type,
shipment_first_received_dh_id_key,
shipment_last_received_dh_id_key,
shipment_first_received_mh_id_key,
shipment_last_received_mh_id_key,
reverse_pickup_hub_id_key,
fe_name_key,
customer_address_id_key,
account_id,
destination_pincode_key,
source_pincode_key,
tl_verification_flag,
tl_cs_override_flag,
tl_checkedby,
tl_ldap_id,
tl_pickup_creation_date_key,
tl_pickup_creation_time_key,
fe_verification_flag,
fe_cs_override_flag,
fe_checkedby,
fe_ldap_id,
fe_pickup_creation_date_key,
fe_pickup_creation_time_key,
return_warehouse,
return_numdate,
return_numdate_key,
customer_name,
customer_phone,
customer_email,
shipment_rto_rvp_received_date_key,
number1,
number2,
number3,
prexo_initial_vertical_captured,
prexo_initial_vertical_type_captured,
prexo_initial_title_captured,
prexo_initial_box_psn_captured,
v3_smart_agent_key,
v3_rvp_help_line_dialed,
v3_sm_tl_date_key,
v3_sm_tl_time_key,
v3_cs_override,
v3_shipment_status_reason,
v3_outer_tl_result,
v3_outer_fe_result,
v3_reason_code,
v3_team_leader_ldap,
v3_return_type,
v3_shipment_status_code,
v3_shipment_value,
v3_sm_fe_date_key,
v3_sm_fe_time_key,
v3_inner_inaccuracy,
v3_outer_inaccuracy,
v3_pk_wo_cs_or_afr_fail,
v3_reverse_misshipment,
v3_imei_mismatch,
v3_checks_inaccurate,
v3_final_inaccuracy,
v3_checks,
v3_check_result,
ofp_attempts as v3_ofp_attempts,
ismandatory_list as v3_ismandatory_list,
fe_result_list,
tl_result_list,
sub_item_name,
tl_inaccuracy as v3_tl_inaccuracy,
tl_outer_inaccuracy as v3_tl_outer_inaccuracy,
tl_final_inaccuracy as v3_tl_final_inaccuracy


from bigfoot_external_neo.scp_ekl__pv_l3_hive_fact;