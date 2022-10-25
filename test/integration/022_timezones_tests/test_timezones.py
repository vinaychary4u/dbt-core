from datetime import datetime
from pytz import timezone
from test.integration.base import DBTIntegrationTest, use_profile



class TestTimezones(DBTIntegrationTest):

    @property
    def schema(self):
        return "timezones_022"

    @property
    def models(self):
        return "models"

    @property
    def profile_config(self):
        return {
            'test': {
                'outputs': {
                    'dev': {
                        'type': 'postgres',
                        'threads': 1,
                        'host': self.database_host,
                        'port': 5432,
                        'user': "root",
                        'pass': "password",
                        'dbname': 'dbt',
                        'schema': self.unique_schema()
                    },
                },
                'target': 'dev'
            }
        }

    @property
    def query(self):
        return """
            select
              run_started_at_est,
              run_started_at_utc
            from {schema}.timezones
        """.format(schema=self.unique_schema())

    @use_profile('postgres')
    def test_postgres_run_started_at(self):
        # run with time checks
        start_time = datetime.now(timezone("UTC"))
        results = self.run_dbt(['run'])
        stop_time = datetime.now(timezone("UTC"))

        # sanity check
        self.assertEqual(len(results), 1)
        result = self.run_sql(self.query, fetch='all')[0]
        est, utc = map(datetime.fromisoformat, result)
        self.assertTrue(start_time < est < stop_time)
        self.assertTrue(start_time < utc < stop_time)
        self.assertEqual(str(est), str(utc.astimezone(timezone("America/New_York"))))
        self.assertEqual(str(utc), str(utc.astimezone(timezone("UTC"))))
