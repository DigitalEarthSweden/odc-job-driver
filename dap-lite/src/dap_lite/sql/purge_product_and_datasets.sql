CREATE OR REPLACE FUNCTION bnp.purge_product(product_name TEXT)
RETURNS VOID AS $$
DECLARE
    my_product_id agdc.dataset_type.id%TYPE;
    index_name TEXT;
BEGIN
    -- Set the schema explicitly
    SET search_path = 'agdc';

    -- Get the product ID
    SELECT id INTO my_product_id FROM agdc.dataset_type WHERE name = product_name;
    IF my_product_id IS NULL THEN
        RAISE EXCEPTION 'Product "%" does not exist', product_name;
    END IF;

    -- Delete lineage records
    WITH datasets AS (
        SELECT id FROM agdc.dataset WHERE dataset_type_ref = my_product_id
    )
    DELETE FROM agdc.dataset_source
    USING datasets
    WHERE agdc.dataset_source.dataset_ref = datasets.id
       OR agdc.dataset_source.source_dataset_ref = datasets.id;

    -- Delete dataset locations
    WITH datasets AS (
        SELECT id FROM agdc.dataset WHERE dataset_type_ref = my_product_id
    )
    DELETE FROM agdc.dataset_location
    USING datasets
    WHERE agdc.dataset_location.dataset_ref = datasets.id;

    -- Delete datasets
    DELETE FROM agdc.dataset WHERE dataset_type_ref = my_product_id;

    -- Delete product-specific Explorer records
    DELETE FROM cubedash.dataset_spatial WHERE dataset_type_ref = my_product_id;
    DELETE FROM cubedash.region WHERE dataset_type_ref = my_product_id;
    DELETE FROM cubedash.time_overview WHERE product_ref = my_product_id;
    DELETE FROM cubedash.product WHERE id = my_product_id;

    -- Remove product lineage references in Explorer
    UPDATE cubedash.product
    SET derived_product_refs = array_remove(derived_product_refs, my_product_id::smallint)
    WHERE derived_product_refs @> ARRAY[my_product_id::smallint];

    UPDATE cubedash.product
    SET source_product_refs = array_remove(source_product_refs, my_product_id::smallint)
    WHERE source_product_refs @> ARRAY[my_product_id::smallint];

    -- Delete product-specific OWS records
    DELETE FROM wms.product_ranges WHERE wms.product_ranges.id = my_product_id;
    DELETE FROM wms.sub_product_ranges WHERE wms.sub_product_ranges.product_id = my_product_id;

    -- Delete the product itself
    DELETE FROM agdc.dataset_type WHERE agdc.dataset_type.id = my_product_id;

    -- Drop dynamic indexes
    FOR index_name IN
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'dataset'
          AND indexname LIKE ('dix_' || product_name || '%')
    LOOP
        EXECUTE FORMAT('DROP INDEX IF EXISTS %I', index_name);
    END LOOP;

    -- Refresh Explorer materialized views
    REFRESH MATERIALIZED VIEW CONCURRENTLY cubedash.mv_dataset_spatial_quality;

    -- Log the success
    RAISE NOTICE 'Product "%" and its datasets have been successfully purged.', product_name;
END;
$$ LANGUAGE plpgsql;


-- run as 