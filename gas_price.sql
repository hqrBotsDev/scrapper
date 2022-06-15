CREATE TABLE gas_price (
id INT GENERATED ALWAYS AS IDENTITY,
gas_timestamp TIMESTAMP WITH TIME ZONE,
last_block NUMERIC(20),
safe_gas NUMERIC(40,20),
norm_gas NUMERIC(40,20),
fast_gas NUMERIC(40,20),
base_fee NUMERIC(40,20),
gas_used_ratio VARCHAR(255)
);
