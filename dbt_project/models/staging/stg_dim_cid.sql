-- CID-10 codes and descriptions from official DATASUS source files.
-- Loaded from input/CID/CID-10-SUBCATEGORIAS.CSV and CID-10-CATEGORIAS.CSV
-- (cp1252 encoding, semicolon-delimited) via ingestion/sources/cid_loader.py.
--
-- Code format:
--   dotted subcategory  A00.0  (4-char original A000 → dot inserted before last char)
--   3-char category     A00    (fallback for records with category-level code only)
--
-- Note: SIM stores CID without dots ("B342") -- different format, cross-source
-- join would require normalization and is out of scope for this staging layer.

select
    cid_codigo,
    cid_descricao
from {{ source('raw', 'dim_cid') }}
where cid_codigo    is not null
  and cid_descricao is not null
