import datetime
import pandas as pd

THREE_MINUTES_IN_TICKS = 10*60*3
SIX_HOURS_IN_TICKS = 10*60*60*6
GAMES_AFTER = datetime.datetime(2005, 1, 1)
GAMES_BEFORE = datetime.datetime(2025, 1, 1)

def clean_dataframe(df, min_duration=THREE_MINUTES_IN_TICKS, max_duration=SIX_HOURS_IN_TICKS, min_rating=-250, max_rating=3000, games_after=GAMES_AFTER, games_before=GAMES_BEFORE):
    # add convenience values
    for prefix in ('player1.player.', 'player2.player.'):
        df[prefix + 'faf_rating.before'] = df[prefix + 'trueskill_mean_before'] - 3 * df[prefix + 'trueskill_deviation_before']
    df['meta.rating_delta'] = (df['player1.player.faf_rating.before']-df['player2.player.faf_rating.before']).abs().astype('int32')
    df['meta.rating_mean'] = ((df['player1.player.faf_rating.before']+df['player2.player.faf_rating.before'])/2).astype('int32')
    df['meta.duration'] = pd.to_timedelta(df['durations.ticks'] / 10, unit='seconds')

    # filter unreasonable values
    return df[
        (df['durations.database.start'] > games_after) &
        (df['durations.database.start'] < games_before) &
        (df['durations.ticks'] > min_duration) &
        (df['durations.ticks'] < max_duration) &
        (df['player1.player.faf_rating.before'] > min_rating) &
        (df['player1.player.faf_rating.before'] < max_rating) &
        (df['player2.player.faf_rating.before'] > min_rating) &
        (df['player2.player.faf_rating.before'] < max_rating)
    ]
