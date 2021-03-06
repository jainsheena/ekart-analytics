INSERT OVERWRITE TABLE fc_packaging_metrics_l2_hive_fact
select
sihf.warehouse_company as warehouse_company , 
sihf.shipment_item_id as shipment_item_id , 
sihf.shipment_item_product_id as shipment_item_product_id , 
sihf.shipment_item_product_key as shipment_item_product_key , 
sihf.shipment_item_quantity as shipment_item_quantity , 
sihf.shipment_id as shipment_id , 
sihf.shipment_destination_type as shipment_destination_type , 
sihf.shipment_display_id as shipment_display_id , 
sihf.shipment_warehouse_id as shipment_warehouse_id , 
sihf.shipment_status as shipment_status , 
sihf.shipment_updated_timestamp as shipment_updated_timestamp , 
sihf.shipment_updated_date_key as shipment_updated_date_key , 
sihf.shipment_updated_time_key as shipment_updated_time_key , 
sihf.shipment_dispatched_timestamp as shipment_dispatched_timestamp , 
sihf.shipment_dispatched_date_key as shipment_dispatched_date_key , 
sihf.shipment_dispatched_time_key as shipment_dispatched_time_key , 
sihf.shipment_dispatched_by as shipment_dispatched_by , 
sihf.is_non_wsr_fbf_shipment as is_non_wsr_fbf_shipment , 
sihf.shipment_warehouse_dim_key as shipment_warehouse_dim_key , 
sihf.shipment_entity_type as shipment_entity_type , 
sihf.box_ideal_packing_box_id as box_ideal_packing_box_id , 
sihf.box_bag_serial_number as box_bag_serial_number , 
sihf.shipment_packed_timestamp as shipment_packed_timestamp , 
sihf.shipment_packed_date_key as shipment_packed_date_key , 
sihf.shipment_packed_time_key as shipment_packed_time_key , 
sihf.shipment_packed_by as shipment_packed_by , 
sihf.packing_box_used_internal_name as packing_box_used_internal_name , 
sihf.packing_box_suggested_internal_name as packing_box_suggested_internal_name , 
shf.shipment_tracking_id as shipment_tracking_id , 
shf.shipment_packing_box_used as shipment_packing_box_used , 
shf.shipment_volume as shipment_volume , 
shf.shipment_packing_box_used_bucket as shipment_packing_box_used_bucket , 
shf.distinct_fsn_count as distinct_fsn_count , 
shf.distinct_cms_vertical_count as distinct_cms_vertical_count , 
shf.distinct_wid_count as distinct_wid_count , 
shf.shipment_is_fragile as shipment_is_fragile , 
shf.shipment_quantity as shipment_quantity , 
shf.shipment_packing_box_suggested_name as shipment_packing_box_suggested_name , 
shf.shipment_packing_box_suggested_volume as shipment_packing_box_suggested_volume , 
shf.shipment_packing_box_suggested_bucket as shipment_packing_box_suggested_bucket , 
shf.shipment_box_used_packing_box_id as shipment_box_used_packing_box_id , 
shf.shipment_box_suggested_packing_box_id as shipment_box_suggested_packing_box_id , 
shf.is_packing_box_suggested as is_packing_box_suggested , 
shf.is_packing_box_used as is_packing_box_used , 
shf.is_suggested_packing_box_valid as is_suggested_packing_box_valid , 
shf.is_used_packing_box_valid as is_used_packing_box_valid , 
shf.used_packing_box_volume_order as used_packing_box_volume_order , 
shf.suggested_packing_box_volume_order as suggested_packing_box_volume_order , 
shf.used_packing_bucket_volume_order as used_packing_bucket_volume_order , 
shf.suggested_packing_bucket_volume_order as suggested_packing_bucket_volume_order , 
shf.is_box_suggested_new as is_box_suggested_new , 
shf.is_box_used_new as is_box_used_new , 
shf.is_exact_adhered as is_exact_adhered , 
shf.is_mobile_tablet_category as is_mobile_tablet_category , 
shf.is_mobile_adherence as is_mobile_adherence , 
shf.is_packed_in_lower_volume as is_packed_in_lower_volume , 
shf.is_bucket_adhered as is_bucket_adhered,
-- new columns addition for outer packaging
shf.box_suggested_outer_packing_box_id as box_suggested_outer_packing_box_id , 
shf.packing_box_suggested_outer_name as packing_box_suggested_outer_name , 
shf.packing_box_suggested_outer_bucket as packing_box_suggested_outer_bucket , 
shf.is_outer_packing_box_suggested as is_outer_packing_box_suggested , 
shf.is_outer_suggested_packing_box_valid as is_outer_suggested_packing_box_valid , 
shf.suggested_outer_packing_box_volume_order as suggested_outer_packing_box_volume_order , 
shf.suggested_outer_packing_bucket_volume_order as suggested_outer_packing_bucket_volume_order , 
shf.is_box_outer_suggested_new as is_box_outer_suggested_new , 
shf.box_outer_used_packing_box_id as box_outer_used_packing_box_id , 
shf.packing_box_used_outer_name as packing_box_used_outer_name , 
shf.packing_box_used_outer_bucket as packing_box_used_outer_bucket , 
shf.is_outer_packing_box_used as is_outer_packing_box_used , 
shf.is_outer_used_packing_box_valid as is_outer_used_packing_box_valid , 
shf.used_outer_packing_box_volume_order as used_outer_packing_box_volume_order , 
shf.used_outer_packing_bucket_volume_order as used_outer_packing_bucket_volume_order , 
shf.is_box_outer_used_new as is_box_outer_used_new , 
--shf.shipment_dispatched_date_key as shipment_dispatched_date_key , 
shf.is_exact_adhered_outer as is_exact_adhered_outer , 
shf.is_mobile_adherence_outer as is_mobile_adherence_outer , 
shf.is_packed_in_lower_volume_outer as is_packed_in_lower_volume_outer , 
shf.is_bucket_adhered_outer as is_bucket_adhered_outer ,
--22 new columns for packaging cost
shf.box_usage as box_usage , 
shf.sb_usage as sb_usage , 
shf.label_usage as label_usage , 
shf.tape_usage as tape_usage , 
shf.invoice_pouch_usage as invoice_pouch_usage , 
shf.2pl_usage as 2pl_usage , 
shf.gift_paper_usage as gift_paper_usage , 
shf.extra_tape_usage as extra_tape_usage , 
shf.bubble_usage as bubble_usage , 
shf.void_filler_usage as void_filler_usage , 
shf.shrink_wrap_usage as shrink_wrap_usage , 
(shf.box_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as box_usage_value , 
(shf.sb_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as sb_usage_value , 
(shf.label_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as label_usage_value , 
(shf.tape_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as tape_usage_value , 
(shf.invoice_pouch_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as invoice_pouch_usage_value , 
(shf.2pl_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as 2pl_usage_value , 
(shf.gift_paper_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as gift_paper_usage_value , 
(shf.extra_tape_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as extra_tape_usage_value , 
(shf.bubble_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as bubble_usage_value , 
(shf.void_filler_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as void_filler_usage_value , 
(shf.shrink_wrap_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as shrink_wrap_usage_value ,
(shf.total_packaging_usage_value* (sihf.shipment_item_quantity / shf.shipment_quantity)) as total_packaging_usage_value ,
case when (is_box_outer_suggested_new<>1 or is_box_outer_used_new<>1)
 then 'Not in Adherence Baseline' 
when is_exact_adhered_outer=1
then 'Exact Adhered' 
when (is_packed_in_lower_volume_outer=1 or is_mobile_adherence_outer=1 or is_exact_adhered_outer=1)
then 'Considered Adherence'
when (is_packed_in_lower_volume_outer<>1 and is_mobile_adherence_outer<>1 and is_exact_adhered_outer<>1)
then 'Not Adhered'
end as adherance,
shf.shipment_outer_packing_box_suggested_volume as shipment_outer_packing_box_suggested_volume,
shf.shipment_product_volume as shipment_product_volume
from 
bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact sihf
left join bigfoot_external_neo.scp_warehouse__fc_shipment_l1_hive_fact shf
on (sihf.warehouse_company=shf.warehouse_company)  and (sihf.shipment_display_id =shf.shipment_display_id )
where sihf.shipment_destination_type='customer'
