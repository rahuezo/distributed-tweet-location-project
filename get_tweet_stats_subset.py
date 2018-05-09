from utils.database import Database

import tkFileDialog as fd
import random
import csv, os


def choose_n(l, n=100): 
    chosen = set()

    while len(chosen) < n: 
        chosen.add(random.choice(l))

    return chosen


def get_records_subset(sample_size=10000): 
    databases = fd.askopenfilenames(title="Choose 10 databases with stats by user")
    output_csv_path = fd.askdirectory(title="Choose csv output directory")

    with open(os.path.join(output_csv_path, '1000 user sample.csv', 'wb')) as csv_file: 
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(['User ID', 'Date', 'Fips', 'tmax', 'prcp', 'humidity', 'Total Tweets', 'Weather Tweets'])

        for db_file in databases: 
            db = Database(db_file)
            chosen_users = choose_n([user[0] for user in db.select('SELECT DISTINCT user_id FROM aggregated_tweet_data LIMIT {}',format(sample_size))])

            for uid in chosen_users: 
                records = db.select('SELECT * FROM aggregated_tweet_data WHERE user_id={}'.format(uid))
                writer.writerows(records)

        db.connection.close()

get_records_subset()