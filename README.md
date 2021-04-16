# fafalytics

Tools to analyse [FAF](http://faforever.com) games.

## Overview

fafalytics is meant to help create pipelines of the following structure:

![Alt text](https://g.gravizo.com/source/overview?https%3A%2F%2Fraw.githubusercontent.com%2Fyaniv-aknin%2Ffafalytics%2Fmaster%2FREADME.md)

<details> 
<summary></summary>
overview
  digraph G {
    fetch_json -> load_metadata
    fetch_replay -> feature_extraction
    feature_extraction -> feature_engineering
    load_metadata -> feature_engineering
    feature_engineering -> export_store
    export_store -> model_training
    export_store -> visualize
    export_store -> load_bigquery
  }
overview
</details>

And verbally...:
 * Get JSON dumps of [Game](https://github.com/FAForever/faf-java-api/blob/28128cca6def4fd4e6fb4fae77cea79d6b1ff926/src/main/java/com/faforever/api/data/domain/Game.java#L38) models from api.faforever.com
 * Get replay files from content.faforever.com
 * Load the metadata from JSON into a nosql datastore (currently Redis)
 * Extract features from replay files into the nosql datastore
 * Calculate new features within the datastore (e.g., `duration = end_time - start_time`)
 * Export store into CSV/Parquet
 * Use the exported data in Pandas etc to train models, directly visualize results, or load into BigQuery (for use with Datastudio etc)

This is the plan anyway. :) Initially, most of the work will be around the `feature_extraction` phase, using semi-manually fetched JSONs/Replays.

## Usage

WARNING: This section describes features that don't exist yet, consider it documentation driven development.

```
$ fafalytics datastore start
$ fafalytics load dump1.json dump2.json
$ fafalytics extract ../dumps/replays/*.fafreplay
```

## Thanks

Based in part on work by [Askaholic](https://github.com/Askaholic) (and with special thanks for answering my questions), [norraxx](https://github.com/norraxx), [fafafaf](https://github.com/fafafaf), and others.
