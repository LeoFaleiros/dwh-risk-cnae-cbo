-- CID-10 codes and descriptions derived from CAT data
-- Source format: "B34.2 Infecc p/Coronavirus Ne" -> split on first space
-- Note: SIM stores CID without dots ("B342") -- different format, cross-source
-- join would require normalization and is out of scope for this staging layer.

select distinct
    split_part(trim(cid_10), ' ', 1)                                    as cid_codigo,
    trim(substring(cid_10 from position(' ' in cid_10) + 1))            as cid_descricao

from {{ source('raw', 'cat_microdados') }}
where cid_10 is not null
  and trim(cid_10) != ''
  and position(' ' in trim(cid_10)) > 0
  and left(trim(cid_10), 1) != '{'  -- exclude '{\u00f1 class}' (source placeholder for unclassified)
