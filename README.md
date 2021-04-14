# fafalytics

Tools to analyse [FAF](http://faforever.com) games.

## Overview

fafalytics is meant to help create pipelines of the following structure:

![Alt text](https://g.gravizo.com/source/overview?https%3A%2F%2Fraw.githubusercontent.com%2Fyaniv-aknin%2Ffafalytics%2Fmaster%2FREADME.md)

<details> 
<summary></summary>
overview
  digraph G {
    fetch_json -> feature_extraction
    fetch_replay -> feature_extraction
    feature_extraction -> dump_parquet
    dump_parquet -> model_training
    dump_parquet -> visualize
    dump_parquet -> load_bigquery
  }
overview
</details>

And verbally...:
 * Get JSON dumps of [Game](https://github.com/FAForever/faf-java-api/blob/28128cca6def4fd4e6fb4fae77cea79d6b1ff926/src/main/java/com/faforever/api/data/domain/Game.java#L38) models from api.faforever.com
 * Get replay files from content.faforever.com
 * Extract features from JSON dumps or replay files into a nosql datastore (currently Redis)
 * Dump store into CSV/Parquet
 * Use the dumped files to train models, directly visualize results, or load into BigQuery (for use with Datastudio etc)

This is the plan anyway. :) Initially, most of the work will be around the `feature_extraction` phase, using semi-manually fetched JSONs/Replays.

## Usage

WARNING: This section describes features that don't exist yet, consider it documentation driven development.

```
$ fafalytics datastore start
$ fafalytics extract base dump1.json dump2.json
$ fafalytics extract replay_header ../dumps/replays/*.fafreplay
```

## Thanks

Based in part on work by [Askaholic](https://github.com/Askaholic) (and with special thanks for answering my questions), [norraxx](https://github.com/norraxx), [fafafaf](https://github.com/fafafaf), and others.
