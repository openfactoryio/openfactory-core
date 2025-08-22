# ksqlDB Topologies

This folder contains the ksqlDB scripts used to create the initial streams and tables required by **OpenFactory**.

## OpenFactory Assets by `ASSET_UUID`

The script [001-assets.sql](001-assets.sql) defines the topologies related to OpenFactory assets based on the source data (keyed by `ASSET_UUID`).

The script is documented [here](assets.md).

## OpenFactory Assets by Unified Namespace (`UNS_ID`)

The script [002-assets-uns.sql](002-assets-uns.sql) defines the topologies for OpenFactory assets using Unified Namespace classification (keyed by `UNS_ID`).

The script is documented [here](assets_uns.md).
