# This can be put in a table if we go "bigger"
PROCESSORS = [
    {
        "id": 1,
        "processor_name": "BNP-Sentinel2_L2A_Processor",
        "source_product_name": "S2A_MSIL1C",
        "target_product_name": "S2A_MSIL2A_{date}_N{baseline}_R{orbit}_T{tile_id}",  # noqa
        "sensor_name": "MSI",
        "baseline": "D001",  # DES baseline,
        "priority": 10,
        "parameters": {"cloud_threshold": 0.2},
        "required_bands": [
            "B01",
            "B02",
            "B03",
            "B04",
            "B08",
        ],  # Kolla med Tobias
        "retry_limit": 3,
        "error_handling_policy": "retry",
        "output_format": "COG",
        "output_bucket": "s3://output-bucket/Sentinel2/L2A",
        "description": "Processor for converting Sentinel-2 MSI L1C to L2A.",
        "processor_version": "1.0.0",
        "l2a_schema_version": "2.12",
        "created_at": "2024-11-20T12:00:00",
        "updated_at": "2024-11-20T12:00:00",
        "enabled": True,
    },
]
