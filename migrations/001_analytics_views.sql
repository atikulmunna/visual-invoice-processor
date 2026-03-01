-- Analytics views for PostgreSQL ledger backend.
-- Apply in Supabase SQL Editor (or psql) after data exists in `ledger_records`.

create or replace view public.ledger_records_flat as
select
  lr.id,
  lr.processed_at_utc,
  lr.status as row_status,
  lr.drive_file_id,
  lr.file_hash,
  lr.record_json ->> 'document_type' as document_type,
  lr.record_json ->> 'vendor_name' as vendor_name,
  lr.record_json ->> 'vendor_tax_id' as vendor_tax_id,
  lr.record_json ->> 'invoice_number' as invoice_number,
  lr.record_json ->> 'invoice_date' as invoice_date,
  lr.record_json ->> 'due_date' as due_date,
  lr.record_json ->> 'currency' as currency,
  (lr.record_json ->> 'subtotal')::numeric as subtotal,
  (lr.record_json ->> 'tax_amount')::numeric as tax_amount,
  (lr.record_json ->> 'total_amount')::numeric as total_amount,
  lr.record_json ->> 'payment_method' as payment_method,
  (lr.record_json ->> 'model_confidence')::numeric as model_confidence,
  (lr.record_json ->> 'validation_score')::numeric as validation_score,
  coalesce((lr.record_json ->> 'needs_review')::boolean, false) as needs_review,
  lr.metadata_json ->> 'document_id' as document_id,
  lr.metadata_json ->> 'used_provider' as used_provider
from public.ledger_records lr;


create or replace view public.ledger_line_items_flat as
select
  lr.id as ledger_id,
  lr.metadata_json ->> 'document_id' as document_id,
  lr.drive_file_id,
  lr.record_json ->> 'vendor_name' as vendor_name,
  lr.record_json ->> 'invoice_number' as invoice_number,
  lr.record_json ->> 'invoice_date' as invoice_date,
  lr.record_json ->> 'currency' as currency,
  li.ordinality as line_no,
  li.item ->> 'description' as description,
  (li.item ->> 'quantity')::numeric as quantity,
  (li.item ->> 'unit_price')::numeric as unit_price,
  (li.item ->> 'line_total')::numeric as line_total,
  li.item ->> 'category' as category
from public.ledger_records lr
cross join lateral jsonb_array_elements(lr.record_json -> 'line_items') with ordinality as li(item, ordinality);


create or replace view public.ledger_daily_summary as
select
  date_trunc('day', processed_at_utc)::date as processing_date,
  count(*) as records_total,
  count(*) filter (where row_status = 'STORED') as stored_total,
  count(*) filter (where needs_review = true) as needs_review_total,
  sum(total_amount) as total_amount_sum,
  avg(model_confidence) as avg_model_confidence,
  avg(validation_score) as avg_validation_score
from public.ledger_records_flat
group by 1
order by 1 desc;

