import datetime
import pandas as pd
import numpy

THREE_MINUTES_IN_TICKS = 10*60*3
SIX_HOURS_IN_TICKS = 10*60*60*6
GAMES_AFTER = datetime.datetime(2005, 1, 1)
GAMES_BEFORE = datetime.datetime(2025, 1, 1)
# I use the same rating bins FtXCommando uses for map pools;
# https://forum.faforever.com/topic/148/matchmaker-pools-thread/2
# https://faforever.zulipchat.com/#narrow/stream/203478-general/topic/.28no.20topic.29/near/237224940
RATING_BINS = [numpy.NINF, 300, 800, 1300, 1800, numpy.inf]

def clean_dataframe(df, min_duration=THREE_MINUTES_IN_TICKS, max_duration=SIX_HOURS_IN_TICKS, min_rating=-250, max_rating=3000, games_after=GAMES_AFTER, games_before=GAMES_BEFORE):
    # add convenience values
    df['meta.duration'] = pd.to_timedelta(df['durations.ticks'] / 10, unit='seconds')
    for prefix in ('player1.player.', 'player2.player.'):
        df[prefix + 'faf_rating.before'] = df[prefix + 'trueskill_mean_before'] - 3 * df[prefix + 'trueskill_deviation_before']
        df[prefix + 'faf_rating.after'] = df[prefix + 'trueskill_mean_after'] - 3 * df[prefix + 'trueskill_deviation_after']
        df[prefix + 'faf_rating.bucket'] = pd.cut((df[prefix + 'faf_rating.before']+df[prefix+'faf_rating.after'])/2, bins=RATING_BINS)
    df['meta.rating.delta'] = (df['player1.player.faf_rating.before']-df['player2.player.faf_rating.before']).abs().astype('int32')
    df['meta.rating.mean'] = ((df['player1.player.faf_rating.before']+df['player2.player.faf_rating.before'])/2).astype('int32')
    df['meta.rating.bucket'] = pd.cut(df['meta.rating.mean'], bins=RATING_BINS)

    # filter unreasonable values
    df = df[
        (df['durations.database.start'] > games_after) &
        (df['durations.database.start'] < games_before) &
        (df['durations.ticks'] > min_duration) &
        (df['durations.ticks'] < max_duration) &
        (df['player1.player.faf_rating.before'] > min_rating) &
        (df['player1.player.faf_rating.before'] < max_rating) &
        (df['player2.player.faf_rating.before'] > min_rating) &
        (df['player2.player.faf_rating.before'] < max_rating)
    ]

    # move player keys to suffix; https://stackoverflow.com/q/67393474/459852
    def rename_column(column, *keys):
        for key in keys:
            if key not in column:
                continue
            column = column.replace(('%s.' % key), '') + ('.%s' % key)
        return column
    df.columns = [rename_column(c, 'player1', 'player2') for c in df.columns]
    return df
