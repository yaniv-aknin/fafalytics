# fafalytics

Suite of CLI tools to help analyse [FAF](http://faforever.com) games.

## Overview

This package has a single multi-command executable called `fafalytics`. It's
meant to help aspiring game analysts follow this process:

![Alt text](https://g.gravizo.com/source/overview?https%3A%2F%2Fraw.githubusercontent.com%2Fyaniv-aknin%2Ffafalytics%2Fmaster%2FREADME.md)

<details> 
<summary></summary>
overview
  digraph G {
    "datastore start" -> "fetch games"
    "datastore start" -> "fetch replay-urls"
    "fetch games" -> "load"
    "fetch replay-urls" -> unpack
    "fetch replay-urls" -> extract
    unpack -> extract
    extract -> export
    load -> export
    export -> colab
    export -> bigquery
    colab, bigquery [shape=box]
    log
  }
overview
</details>

Each of these is a `fafalytics` subcommand, other than [Colab][] and
[BigQuery][] which are external tools. Obviously you don't *have* to use these,
after the `export` step do whatever you want with the data.

[Colab]: https://colab.research.google.com
[BigQuery]: https://cloud.google.com/bigquery

You can learn more about each of these commands using the `--help` argument, but
basically the logical flow is something like...:
 * Start the fafalytics document store (currently Redis)
 * Get JSON dumps of [Game models][] models from api.faforever.com
 * Load the metadata from the JSON dumps into the datastore
 * Get a list of replay-urls and download them somehow (e.g. `| xargs wget`)
 * Optionally unpack the replays
   Pre-unpacking the replays makes them ~10x faster to read on subsequent
   reads, which is probably useful if you're hacking on the code
 * Extract features from (possibly unpacked) replay files into the datastore
 * Export store into CSV/Parquet
 * Use the exported data in BigQuery/Pandas/etc

[Game models]: https://github.com/FAForever/faf-java-api/blob/28128cca6def4fd4e6fb4fae77cea79d6b1ff926/src/main/java/com/faforever/api/data/domain/Game.java#L38

One last command that merits some discussion is `fafalytics log`. Since this is
a multi-tool sometimes used as a short-lived CLI program and sometimes in a
multiprocessing pipeline-like environment, I built a lightweight logging
mechanism that logs output to the datastore. The `log` command prints whats in
the datastore log, which can come in handy when running 100 `fafalytics`
instances in parallel to extract 500K games over an hour or so.

## Usage

```
$ fafalytics datastore start
$ fafalytics fetch games /tmp/fafalytics
$ fafalytics load /tmp/fafalytics/*.json
$ fafalytics fetch replay-urls /tmp/fafalytics | xargs wget
$ fafalytics extract /tmp/fafalytics/*.fafreplay
$ fafalytics export curated /tmp/fafalytics/result.parquet
```

## Thanks

Based in part on work by [Askaholic](https://github.com/Askaholic) (and with special thanks for answering my questions), [norraxx](https://github.com/norraxx), [spooky](https://github.com/spooky), and others.
