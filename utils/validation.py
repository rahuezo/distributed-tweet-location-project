def is_mover(locations): 
    return False if len(locations) == 1 else (len(locations) == len(set(locations)))


def is_potential_mover(db, user_id, cnt=1): 
    count = int(db.select('SELECT COUNT(user_id) FROM users_fips WHERE user_id={uid}'.format(uid=user_id)).fetchone()[0]) 
    return count > cnt


def in_range(value, current_range): 
    return current_range[0] <= int(value) < current_range[1]

def is_processed_file(f, stash): 
    return f in stash

def is_weather(tweet_text):
    keywords = ['arid', 'aridity',
                    'autumnal', 'balmy',
                    'barometric', 'blizzard', 'blizzards', 'blustering', 'blustery',
                    'blustery', 'breeze', 'breezes', 'breezy', 'celsius',
                    'chilled', 'chillier', 'chilliest', 'chilly',
                    'cloud', 'cloudburst', 'cloudbursts', 'cloudier', 'cloudiest',
                    'clouds', 'cloudy', 'cold','cold,','cold.', 'colder', 'coldest', 'cooled', 'cooling',
                    'cools', 'cumulonimbus',
                    'cumulus', 'cyclone', 'cyclones', 'damp', 'damp', 'damper', 'damper',
                    'dampest', 'dampest', 'deluge', 'dew','dew.','dew,', 'dews', 'downdraft', 'downdrafts',
                    'downpour', 'downpours', 'drier', 'driest', 'drizzle',
                    'drizzled', 'drizzles', 'drizzly', 'drought', 'droughts', 'dry','dry,','dry.', 'dryline',
                    'farenheit', 'fahrenheit', 'flood', 'flooded', 'flooding', 'floods', 'flurries',
                    'flurry', 'fog','fog.','fog,', 'fogbow', 'fogbows', 'fogged', 'fogging', 'foggy', 'fogs',
                    'forecast', 'forecasted', 'forecasting', 'forecasts', 'freeze', 'freezes',
                    'freezing', 'frigid', 'frost', 'frostier', 'frostiest', 'frosts', 'frosty',
                    'froze', 'frozen', 'gale','gale,','gale.', 'gales', 'galoshes', 'gust', 'gusting', 'gusts',
                    'gusty', 'haboob', 'haboobs', 'hail', 'hailed', 'hailing', 'hails', 'haze',
                    'hazes', 'hazy', 'heat', 'heated', 'heating', 'hoarfrost', 'hot','hot.','hot,',
                    'hotter', 'hottest', 'humid', 'humidity', 'hurricane', 'hurricanes', 'icy', 'inclement', 'landspout', 'landspouts',
                    'lightning', 'lightnings', 'macroburst', 'macrobursts',
                    'meteorologic', 'meteorologist', 'meteorologists', 'meteorology', 'microburst',
                    'microbursts', 'microclimate', 'microclimates', 'millibar', 'millibars', 'mist',
                    'misted', 'mists', 'misty', 'moist', 'moisture', 'monsoon', 'monsoons', 'mugginess',
                    'muggy', "nor'easter", "nor'easters", 'noreaster', 'noreasters',
                    'overcast', 'parched', 'parching', 'precipitation', 'rain', 'rainboots', 'rainbow', 'rainbows',
                    'raincoat', 'raincoats', 'rained', 'rainfall', 'rainier', 'rainiest', 'raining', 'rains', 'rainy',
                    'sandstorm', 'sandstorms', 'scorcher', 'scorching', 'shower', 'showering', 'showers',
                    'sleet', 'slicker', 'slickers', 'slush', 'smog', 'smoggier', 'smoggiest',
                    'smoggy', 'snow', 'snowed', 'snowier', 'snowiest', 'snowing', 'snowmageddon', 'snowpocalypse',
                    'snows', 'snowy', 'sprinkle', 'sprinkling', 'squall', 'squalls', 'squally',
                    'storm', 'stormed', 'stormier', 'stormiest', 'storming', 'storms', 'stormy', 'stratocumulus', 'stratus',
                    'subtropical', 'summery', 'sun','sun,','sun.', 'sunnier', 'sunniest', 'sunny', 'temperate', 'temperature',
                    'tempest', 'thaw', 'thawed', 'thawing', 'thaws', 'thermometer', 'thunder', 'thundering',
                    'thunderstorm', 'thunderstorms', 'tornadic', 'tornado', 'tornadoes', 'tropical', 'troposphere',
                    'tsunami', 'turbulent', 'twister', 'twisters', 'typhoon', 'typhoons', 'umbrella', 'umbrellas', 'vane', 'warm',
                    'warmed', 'warms', 'weather', 'wet','wet,','wet.', 'wetter', 'wettest',
                    'wind', 'windchill', 'windchills', 'windier', 'windiest', 'windspeed', 'windy', 'wintery', 'wintry']

    # if one or more of the keywords is in the tweet text, this tweet is about weather
    
    results = 0
        
    for kw in keywords:
        if " {0}".format(kw) in tweet_text.lower():
            results += 1
    return 1 if results > 0 else 0
