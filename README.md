Create based Rust reimplementation from Ruby (antlr-gemerator)[https://github.com/teritorio/overpass_parser-rb].

## Use

```rust
use overpass_parser::parse_query;

let tree = parse_query("[out:json]...")
```

## SQL

Postgres/PostGIS, OPE Schema
```sql
CREATE OR REPLACE TEMP VIEW node AS
SELECT id, version, created, tags, NULL::bigint[] AS nodes, NULL::jsonb AS members, geom, objtype AS osm_type FROM osm_base WHERE objtype = 'n';

CREATE OR REPLACE TEMP VIEW way AS
SELECT id, version, created, tags, nodes, NULL::jsonb AS members, geom, objtype AS osm_type FROM osm_base WHERE objtype = 'w';

CREATE OR REPLACE TEMP VIEW relation AS
SELECT id, version, created, tags, NULL::bigint[] AS nodes, members, geom, objtype AS osm_type FROM osm_base WHERE objtype = 'r';

CREATE OR REPLACE TEMP VIEW nwr AS
SELECT id, version, created, tags, nodes, members, geom, objtype AS osm_type FROM osm_base;

CREATE OR REPLACE TEMP VIEW area AS
SELECT id + 3600000000 AS id, version, created, tags, NULL::bigint[] AS nodes, NULL::jsonb AS members, geom, 'a' AS osm_type FROM osm_base_areas
UNION ALL
SELECT id, version, created, tags, NULL::bigint[] AS nodes, NULL::jsonb AS members, geom, 'w' AS osm_type FROM osm_base WHERE objtype = 'w' AND ST_Dimension(geom) = 2;
```

DuckDB/Spatial, Quackosm schema
```sql
CREATE OR REPLACE TEMP VIEW node AS
SELECT split_part(feature_id, '/', 2)::bigint AS id, NULL::int AS version, NULL::timestamp AS created, tags, NULL::bigint[] AS nodes, NULL::json AS members, ST_GeomFromWKB(geometry) AS geom, feature_id[1] AS osm_type FROM 'landes_nofilter_noclip_compact.parquet' WHERE feature_id < 'o';

CREATE OR REPLACE TEMP VIEW way AS
SELECT split_part(feature_id, '/', 2)::bigint AS id, NULL::int AS version, NULL::timestamp AS created, tags, NULL::bigint[] AS nodes, NULL::json AS members, ST_GeomFromWKB(geometry) AS geom, feature_id[1] AS osm_type FROM 'landes_nofilter_noclip_compact.parquet' WHERE feature_id > 'w';

CREATE OR REPLACE TEMP VIEW relation AS
SELECT split_part(feature_id, '/', 2)::bigint AS id, NULL::int AS version, NULL::timestamp AS created, tags, NULL::bigint[] AS nodes, NULL::json AS members, ST_GeomFromWKB(geometry) AS geom, feature_id[1] AS osm_type FROM 'landes_nofilter_noclip_compact.parquet' WHERE feature_id > 'o' AND feature_id < 's';

CREATE OR REPLACE TEMP VIEW nwr AS
SELECT split_part(feature_id, '/', 2)::bigint AS id, NULL::int AS version, NULL::timestamp AS created, tags, NULL::bigint[] AS nodes, NULL::json AS members, ST_GeomFromWKB(geometry) AS geom, feature_id[1] AS osm_type FROM 'landes_nofilter_noclip_compact.parquet';

CREATE OR REPLACE TEMP VIEW area AS
SELECT split_part(feature_id, '/', 2)::bigint + CASE feature_id[1] WHEN 'r' THEN 3600000000 ELSE 0 END AS id, NULL::int AS version, NULL::timestamp AS created, tags, NULL::bigint[] AS nodes, NULL::json AS members, ST_GeomFromWKB(geometry) AS geom, CASE feature_id[1] WHEN 'w' THEN 'w' ELSE 'a' END AS osm_type FROM 'landes_nofilter_noclip_compact.parquet' wHERE feature_id > 'm' AND list_contains(['POLYGON', 'MULTIPOLYGON'], ST_GeometryType(ST_GeomFromWKB(geometry)));
```

### Cli

```sh
echo '[out:json][timeout:25];
area(7009125)->.a;
nwr.a["tourism"="information"];
out center meta;
' | ./target/debug/overpass2sql postgres
```

```sh
echo '[out:json][timeout:25];
area(7009125)->.a;
nwr.a["tourism"="information"];
out center meta;
' | ./target/debug/overpass2sql duckdb | duckdb
```


## License
Licensed under the MIT license. See LICENSE.txt for details.
